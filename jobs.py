from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


JOB_ROOT = Path("/tmp/bbb-dl-jobs")
OUTPUT_ROOT = Path("/tmp/bbb-dl-output")
READY_FILE_TYPES = {
    "mp4",
    "webcam_webm",
    "deskshare_webm",
    "audio_mp3",
    "slides_zip",
    "transcript_json",
    "notes_html",
}


@dataclass(slots=True)
class Job:
    id: str
    url: str
    status: str = "queued"
    progress: int = 0
    stage: str = "Queued"
    title: str | None = None
    date: str | None = None
    duration: str | None = None
    temp_dir: Path = field(default_factory=Path)
    output_dir: Path = field(default_factory=Path)
    available_files: list[str] = field(default_factory=list)
    error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["temp_dir"] = str(self.temp_dir)
        payload["output_dir"] = str(self.output_dir)
        payload["created_at"] = self.created_at.isoformat()
        return payload


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._lock = asyncio.Lock()

    async def create_job(self, url: str) -> Job:
        job_id = str(uuid4())
        job = Job(
            id=job_id,
            url=url,
            temp_dir=JOB_ROOT / job_id,
            output_dir=OUTPUT_ROOT / job_id,
        )
        async with self._lock:
            self._jobs[job_id] = job
        return job

    async def list_jobs(self) -> list[Job]:
        async with self._lock:
            return list(self._jobs.values())

    async def get_job(self, job_id: str) -> Job | None:
        async with self._lock:
            return self._jobs.get(job_id)

    async def require_job(self, job_id: str) -> Job:
        job = await self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        return job

    async def update_job(self, job_id: str, **updates: Any) -> Job:
        async with self._lock:
            job = self._jobs[job_id]
            for key, value in updates.items():
                setattr(job, key, value)
            return job

    async def set_task(self, job_id: str, task: asyncio.Task[Any]) -> None:
        async with self._lock:
            self._tasks[job_id] = task

    async def pop_task(self, job_id: str) -> asyncio.Task[Any] | None:
        async with self._lock:
            return self._tasks.pop(job_id, None)

    async def get_task(self, job_id: str) -> asyncio.Task[Any] | None:
        async with self._lock:
            return self._tasks.get(job_id)

    async def remove_job(self, job_id: str) -> Job | None:
        async with self._lock:
            self._tasks.pop(job_id, None)
            return self._jobs.pop(job_id, None)

