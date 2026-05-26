from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader import _find_first_existing, _finish_job_from_artifacts, is_valid_bbb_url, parse_progress_line
from jobs import JobStore


class DownloaderTests(unittest.TestCase):
    def test_valid_bbb_url(self) -> None:
        self.assertTrue(is_valid_bbb_url("https://example.com/playback/presentation/2.3/abcdef"))
        self.assertFalse(is_valid_bbb_url("https://example.com/playback/video/2.3/abcdef"))

    def test_progress_mapping(self) -> None:
        self.assertEqual(parse_progress_line("Downloading meta information"), ("downloading", 5, "Fetching metadata"))
        self.assertEqual(parse_progress_line("Downloading webcams"), ("downloading", 15, "Downloading webcam video"))
        self.assertEqual(parse_progress_line("Downloading deskshare"), ("downloading", 30, "Downloading screen recording"))
        self.assertEqual(parse_progress_line("Downloading slides"), ("downloading", 40, "Downloading slides"))
        self.assertEqual(
            parse_progress_line("Start capturing frames"),
            ("processing", 45, "Capturing slide frames (slowest step)..."),
        )
        self.assertEqual(
            parse_progress_line("3/7 Partition finished"),
            ("processing", 56, "Capturing slide frames (3/7 partitions)"),
        )
        self.assertEqual(parse_progress_line("Start creating slideshow"), ("processing", 72, "Assembling slideshow"))
        self.assertEqual(parse_progress_line("Start merging"), ("processing", 85, "Merging video streams"))
        self.assertEqual(parse_progress_line("Muxing"), ("processing", 92, "Finalising video"))


class DownloaderRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_find_first_existing_prefers_first_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a" / "webcams.mp4").parent.mkdir(parents=True)
            (root / "a" / "webcams.mp4").write_text("x")
            (root / "b" / "webcams.webm").parent.mkdir(parents=True)
            (root / "b" / "webcams.webm").write_text("x")

            store = JobStore()
            job = await store.create_job("https://example.com/playback/presentation/2.3/abcdef")
            job.temp_dir = root

            found = _find_first_existing(job, "webcams.webm", "webcams.mp4")
            self.assertEqual(found, root / "b" / "webcams.webm")

    async def test_finish_job_from_artifacts_recovers_ready_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            temp_dir = root / "temp"
            output_dir = root / "output"
            temp_dir.mkdir()
            output_dir.mkdir()
            (temp_dir / "webcams.webm").write_text("webm")
            (temp_dir / "presentation_text.json").write_text("{}")
            (temp_dir / "notes.html").write_text("<html></html>")

            store = JobStore()
            job = await store.create_job("https://example.com/playback/presentation/2.3/abcdef")
            job.temp_dir = temp_dir
            job.output_dir = output_dir

            async def fake_transcode(_: Path, destination: Path) -> bool:
                destination.write_text("mp4")
                return True

            with (
                patch("downloader._transcode_final_video", AsyncMock(side_effect=fake_transcode)) as transcode_mock,
                patch("downloader._auto_process_derived_files", AsyncMock(return_value=None)),
            ):
                recovered = await _finish_job_from_artifacts(
                    job,
                    store,
                    ["Time: 00:12:34"],
                    None,
                    stage="Done (recovered from BBB render failure)",
                    allow_fallback_mp4=True,
                )

            self.assertTrue(recovered)
            refreshed = await store.require_job(job.id)
            self.assertEqual(refreshed.status, "ready")
            self.assertIn("mp4", refreshed.available_files)
            self.assertEqual(refreshed.file_statuses["mp4"].status, "ready")
            self.assertEqual(refreshed.file_statuses["deskshare_webm"].status, "failed")
            transcode_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
