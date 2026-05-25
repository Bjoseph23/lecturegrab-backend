from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

import main
from jobs import Job


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)

    def tearDown(self) -> None:
        main.store._jobs.clear()
        main.store._tasks.clear()

    def test_create_job_rejects_invalid_url(self) -> None:
        response = self.client.post("/api/job", json={"url": "https://example.com/not-bbb"})
        self.assertEqual(response.status_code, 422)

    def test_health(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "version": "1.0.0"})

    def test_get_job_returns_state(self) -> None:
        async def prepare() -> str:
            job = await main.store.create_job("https://example.com/playback/presentation/2.3/demo")
            await main.store.update_job(job.id, status="ready", progress=100, stage="Done")
            return job.id

        job_id = asyncio.run(prepare())
        response = self.client.get(f"/api/job/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ready")

    def test_download_returns_425_when_not_ready(self) -> None:
        async def prepare() -> str:
            job = await main.store.create_job("https://example.com/playback/presentation/2.3/demo")
            await main.store.update_job(job.id, status="processing")
            return job.id

        job_id = asyncio.run(prepare())
        response = self.client.get(f"/api/job/{job_id}/download/mp4")
        self.assertEqual(response.status_code, 425)

    def test_download_streams_ready_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_root:
            output_dir = Path(temp_root)
            file_path = output_dir / "recording.mp4"
            file_path.write_bytes(b"video")

            async def prepare() -> str:
                job = Job(
                    id="job-download",
                    url="https://example.com/playback/presentation/2.3/demo",
                    status="ready",
                    progress=100,
                    stage="Done",
                    temp_dir=output_dir / "tmp",
                    output_dir=output_dir,
                    available_files=["mp4"],
                )
                main.store._jobs[job.id] = job
                return job.id

            job_id = asyncio.run(prepare())
            response = self.client.get(f"/api/job/{job_id}/download/mp4")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.content, b"video")

    def test_delete_job_cleans_resources(self) -> None:
        async def prepare() -> str:
            job = await main.store.create_job("https://example.com/playback/presentation/2.3/demo")
            task = asyncio.create_task(asyncio.sleep(60))
            await main.store.set_task(job.id, task)
            return job.id

        job_id = asyncio.run(prepare())
        with patch("main.cleanup_job_directories", new=AsyncMock()) as cleanup_mock:
            response = self.client.delete(f"/api/job/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"deleted": True})
        cleanup_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
