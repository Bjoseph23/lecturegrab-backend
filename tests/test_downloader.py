from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader import is_valid_bbb_url, parse_progress_line


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


if __name__ == "__main__":
    unittest.main()
