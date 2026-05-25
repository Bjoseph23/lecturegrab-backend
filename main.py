from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from cleanup import cleanup_loop
from downloader import FILE_NAME_MAP, cleanup_job_directories, is_valid_bbb_url, run_job
from jobs import JOB_ROOT, OUTPUT_ROOT, READY_FILE_TYPES, JobStore


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bbb-backend")
store = JobStore()
cleanup_task: asyncio.Task[None] | None = None


class CreateJobRequest(BaseModel):
    url: str = Field(..., min_length=1)


class CreateJobResponse(BaseModel):
    job_id: str
    status: Literal["queued"]


class JobResponse(BaseModel):
    job_id: str
    status: Literal["queued", "downloading", "processing", "ready", "failed"]
    progress: int
    stage: str
    title: str | None
    date: str | None
    duration: str | None
    available_files: list[str]
    error: str | None


class DeleteJobResponse(BaseModel):
    deleted: bool


@asynccontextmanager
async def lifespan(_: FastAPI):
    global cleanup_task
    JOB_ROOT.mkdir(parents=True, exist_ok=True)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    cleanup_task = asyncio.create_task(cleanup_loop(store))
    try:
        yield
    finally:
        if cleanup_task is not None:
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="BBB Downloader Backend", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info("%s %s -> %s (%.2fms)", request.method, request.url.path, response.status_code, elapsed_ms)
    return response


def serialize_job(job_id: str, job) -> JobResponse:
    return JobResponse(
        job_id=job_id,
        status=job.status,
        progress=job.progress,
        stage=job.stage,
        title=job.title,
        date=job.date,
        duration=job.duration,
        available_files=job.available_files,
        error=job.error,
    )


async def run_job_task(job_id: str) -> None:
    job = await store.require_job(job_id)
    try:
        await run_job(job, store)
    finally:
        await store.pop_task(job_id)


async def delete_job_resources(job_id: str) -> None:
    job = await store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    task = await store.pop_task(job_id)
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await cleanup_job_directories(job)
    await store.remove_job(job_id)


@app.exception_handler(KeyError)
async def handle_key_error(_: Request, __: KeyError):
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": "Job not found"})


@app.post("/api/job", response_model=CreateJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_job(payload: CreateJobRequest) -> CreateJobResponse:
    if not is_valid_bbb_url(payload.url):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid BBB playback URL")

    job = await store.create_job(payload.url)
    task = asyncio.create_task(run_job_task(job.id), name=f"bbb-job-{job.id}")
    await store.set_task(job.id, task)
    return CreateJobResponse(job_id=job.id, status="queued")


@app.get("/api/job/{job_id}", response_model=JobResponse)
async def get_job(job_id: str) -> JobResponse:
    job = await store.require_job(job_id)
    return serialize_job(job_id, job)


@app.get("/api/job/{job_id}/download/{file_type}")
async def download_file(job_id: str, file_type: str):
    if file_type not in READY_FILE_TYPES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown file type")

    job = await store.require_job(job_id)
    if job.status != "ready":
        raise HTTPException(status_code=status.HTTP_425_TOO_EARLY, detail="Job still processing")

    file_path = Path(job.output_dir) / FILE_NAME_MAP[file_type]
    if not file_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    media_type = "application/octet-stream"
    if file_path.suffix == ".mp4":
        media_type = "video/mp4"
    elif file_path.suffix == ".webm":
        media_type = "video/webm"
    elif file_path.suffix == ".mp3":
        media_type = "audio/mpeg"
    elif file_path.suffix == ".zip":
        media_type = "application/zip"
    elif file_path.suffix == ".json":
        media_type = "application/json"
    elif file_path.suffix == ".html":
        media_type = "text/html"

    return FileResponse(path=file_path, media_type=media_type, filename=file_path.name)


@app.delete("/api/job/{job_id}", response_model=DeleteJobResponse)
async def delete_job(job_id: str) -> DeleteJobResponse:
    await delete_job_resources(job_id)
    return DeleteJobResponse(deleted=True)


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok", "version": "1.0.0"}
