from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cleanup import cleanup_expired_jobs
from jobs import FileTaskStatus, Job, JobStore


class CleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_cleanup_removes_expired_completed_job(self) -> None:
        store = JobStore()
        with tempfile.TemporaryDirectory() as temp_root:
            temp_path = Path(temp_root)
            job = Job(
                id="job-1",
                url="https://example.com/playback/presentation/2.3/test",
                status="ready",
                temp_dir=temp_path / "tmp",
                output_dir=temp_path / "out",
                created_at=datetime.now(UTC) - timedelta(hours=3),
            )
            job.temp_dir.mkdir()
            job.output_dir.mkdir()
            store._jobs[job.id] = job
            with patch("cleanup.cleanup_job_directories", new=AsyncMock()) as cleanup_mock:
                await cleanup_expired_jobs(store)
            self.assertIsNone(await store.get_job(job.id))
            cleanup_mock.assert_awaited_once()

    async def test_cleanup_keeps_active_job(self) -> None:
        store = JobStore()
        job = Job(
            id="job-2",
            url="https://example.com/playback/presentation/2.3/test",
            status="processing",
            temp_dir=Path("/tmp/job-2"),
            output_dir=Path("/tmp/out-2"),
            created_at=datetime.now(UTC) - timedelta(hours=3),
        )
        store._jobs[job.id] = job
        with patch("cleanup.cleanup_job_directories") as cleanup_mock:
            await cleanup_expired_jobs(store)
        self.assertIsNotNone(await store.get_job(job.id))
        cleanup_mock.assert_not_called()

    async def test_cleanup_keeps_job_with_active_file_processing(self) -> None:
        store = JobStore()
        job = Job(
            id="job-3",
            url="https://example.com/playback/presentation/2.3/test",
            status="ready",
            temp_dir=Path("/tmp/job-3"),
            output_dir=Path("/tmp/out-3"),
            created_at=datetime.now(UTC) - timedelta(hours=3),
            file_statuses={"audio_mp3": FileTaskStatus(status="processing", progress=50, stage="Extracting MP3 audio")},
        )
        store._jobs[job.id] = job
        with patch("cleanup.cleanup_job_directories") as cleanup_mock:
            await cleanup_expired_jobs(store)
        self.assertIsNotNone(await store.get_job(job.id))
        cleanup_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
