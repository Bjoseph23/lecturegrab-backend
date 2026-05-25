from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from downloader import cleanup_job_directories
from jobs import JobStore


JOB_TTL = timedelta(hours=2)
CLEANUP_INTERVAL_SECONDS = 1800


async def cleanup_expired_jobs(store: JobStore) -> None:
    threshold = datetime.now(UTC) - JOB_TTL
    for job in await store.list_jobs():
        if job.created_at >= threshold:
            continue
        if job.status in {"queued", "downloading", "processing"}:
            continue
        if any(file_status.status in {"queued", "processing"} for file_status in job.file_statuses.values()):
            continue
        await cleanup_job_directories(job)
        await store.remove_job(job.id)


async def cleanup_loop(store: JobStore, interval_seconds: int = CLEANUP_INTERVAL_SECONDS) -> None:
    while True:
        try:
            await cleanup_expired_jobs(store)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(interval_seconds)
