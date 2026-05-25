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
from downloader import FILE_NAME_MAP, cleanup_job_directories, is_valid_bbb_url, run_file_task, run_job
from jobs import DERIVED_FILE_TYPES, JOB_ROOT, OUTPUT_ROOT, READY_FILE_TYPES, FileTaskStatus, JobStore


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bbb-backend")
store = JobStore()
cleanup_task: asyncio.Task[None] | None = None


class CreateJobRequest(BaseModel):
    url: str = Field(..., min_length=1)


class CreateJobResponse(BaseModel):
    job_id: str
    status: Literal["queued"]


class FileStatusResponse(BaseModel):
    status: str
    progress: int
    stage: str
    error: str | None


class JobResponse(BaseModel):
    job_id: str
    status: Literal["queued", "downloading", "processing", "ready", "failed"]
    progress: int
    stage: str
    title: str | None
    date: str | None
    duration: str | None
    available_files: list[str]
    file_statuses: dict[str, FileStatusResponse]
    error: str | None


class DeleteJobResponse(BaseModel):
    deleted: bool


class ProcessFileResponse(BaseModel):
    job_id: str
    file_type: str
    status: str
    progress: int
    stage: str
    error: str | None


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


app = FastAPI(title="BBB Downloader Backend", version="1.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("Unhandled request error for %s %s", request.method, request.url.path)
        raise
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info("%s %s -> %s (%.2fms)", request.method, request.url.path, response.status_code, elapsed_ms)
    return response


def serialize_file_status(file_status: FileTaskStatus) -> FileStatusResponse:
    return FileStatusResponse(
        status=file_status.status,
        progress=file_status.progress,
        stage=file_status.stage,
        error=file_status.error,
    )


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
        file_statuses={file_type: serialize_file_status(file_status) for file_type, file_status in job.file_statuses.items()},
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

    file_tasks = await store.pop_all_file_tasks(job_id)
    for file_task in file_tasks:
        if not file_task.done():
            file_task.cancel()
            try:
                await file_task
            except asyncio.CancelledError:
                pass

    await cleanup_job_directories(job)
    await store.remove_job(job_id)


def media_type_for_path(file_path: Path) -> str:
    if file_path.suffix == ".mp4":
        return "video/mp4"
    if file_path.suffix == ".webm":
        return "video/webm"
    if file_path.suffix == ".mp3":
        return "audio/mpeg"
    if file_path.suffix == ".zip":
        return "application/zip"
    if file_path.suffix == ".json":
        return "application/json"
    if file_path.suffix == ".html":
        return "text/html"
    return "application/octet-stream"


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
    logger.info("Queued job %s for %s", job.id, job.url)
    return CreateJobResponse(job_id=job.id, status="queued")


@app.get("/api/job/{job_id}", response_model=JobResponse)
async def get_job(job_id: str) -> JobResponse:
    job = await store.require_job(job_id)
    return serialize_job(job_id, job)


@app.post("/api/job/{job_id}/process/{file_type}", response_model=ProcessFileResponse, status_code=status.HTTP_202_ACCEPTED)
async def process_file(job_id: str, file_type: str) -> ProcessFileResponse:
    if file_type not in READY_FILE_TYPES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown file type")

    job = await store.require_job(job_id)
    if job.status != "ready":
        raise HTTPException(status_code=status.HTTP_425_TOO_EARLY, detail="Base recording is still processing")

    file_status = job.file_statuses[file_type]
    if file_status.status == "ready":
        return ProcessFileResponse(
            job_id=job_id,
            file_type=file_type,
            status=file_status.status,
            progress=file_status.progress,
            stage=file_status.stage,
            error=file_status.error,
        )

    running_task = await store.get_file_task(job_id, file_type)
    if running_task is not None and not running_task.done():
        return ProcessFileResponse(
            job_id=job_id,
            file_type=file_type,
            status=file_status.status,
            progress=file_status.progress,
            stage=file_status.stage,
            error=file_status.error,
        )

    await store.update_file_status(job_id, file_type, status="queued", progress=0, stage="Queued for processing", error=None)
    task = asyncio.create_task(run_file_task(job_id, file_type, store), name=f"bbb-file-{job_id}-{file_type}")
    await store.set_file_task(job_id, file_type, task)
    logger.info("Queued derived file task for job %s file %s", job_id, file_type)

    next_status = (await store.require_job(job_id)).file_statuses[file_type]
    return ProcessFileResponse(
        job_id=job_id,
        file_type=file_type,
        status=next_status.status,
        progress=next_status.progress,
        stage=next_status.stage,
        error=next_status.error,
    )


@app.get("/api/job/{job_id}/download/{file_type}")
async def download_file(job_id: str, file_type: str):
    if file_type not in READY_FILE_TYPES:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown file type")

    job = await store.require_job(job_id)
    file_status = job.file_statuses[file_type]
    if file_status.status != "ready":
        detail = "File is not ready yet. Start processing it first." if file_type in DERIVED_FILE_TYPES else "Job still processing"
        raise HTTPException(status_code=status.HTTP_425_TOO_EARLY, detail=detail)

    file_path = Path(job.output_dir) / FILE_NAME_MAP[file_type]
    if not file_path.exists():
        logger.error("Ready file missing on disk for job %s file %s", job_id, file_type)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

    return FileResponse(path=file_path, media_type=media_type_for_path(file_path), filename=file_path.name)


@app.delete("/api/job/{job_id}", response_model=DeleteJobResponse)
async def delete_job(job_id: str) -> DeleteJobResponse:
    await delete_job_resources(job_id)
    logger.info("Deleted job %s", job_id)
    return DeleteJobResponse(deleted=True)


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok", "version": "1.1.0"}
