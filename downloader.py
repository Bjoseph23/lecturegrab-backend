from __future__ import annotations

import asyncio
import contextlib
import re
import shutil
import subprocess
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from jobs import Job, JobStore, READY_FILE_TYPES


BBB_URL_RE = re.compile(
    r"^https://[^/]+/playback/presentation/2\.[^/]+/[^/?#]+/?$",
    re.IGNORECASE,
)
PARTITION_RE = re.compile(r"(?P<done>\d+)/7 Partition finished")
TITLE_RE = re.compile(r"Title:\s*(?P<value>.+)")
DATE_RE = re.compile(r"Date:\s*(?P<value>.+)")
DURATION_RE = re.compile(r"Duration:\s*(?P<value>.+)")
FILE_NAME_MAP = {
    "mp4": "recording.mp4",
    "webcam_webm": "webcams.webm",
    "deskshare_webm": "deskshare.webm",
    "audio_mp3": "audio.mp3",
    "slides_zip": "slides.zip",
    "transcript_json": "presentation_text.json",
    "notes_html": "notes.html",
}
FILE_TYPE_ORDER = [
    "mp4",
    "webcam_webm",
    "deskshare_webm",
    "audio_mp3",
    "slides_zip",
    "transcript_json",
    "notes_html",
]


def is_valid_bbb_url(url: str) -> bool:
    return BBB_URL_RE.match(url) is not None


def parse_progress_line(line: str) -> tuple[str, int, str] | None:
    lowered = line.lower()
    if "downloading meta information" in lowered:
        return ("downloading", 5, "Fetching metadata")
    if "downloading webcams" in lowered:
        return ("downloading", 15, "Downloading webcam video")
    if "downloading deskshare" in lowered:
        return ("downloading", 30, "Downloading screen recording")
    if "downloading slides" in lowered:
        return ("downloading", 40, "Downloading slides")
    if "start capturing frames" in lowered:
        return ("processing", 45, "Capturing slide frames (slowest step)...")

    match = PARTITION_RE.search(line)
    if match:
        partition = min(max(int(match.group("done")), 0), 7)
        progress = 45 + round((partition / 7) * 25)
        return ("processing", progress, f"Capturing slide frames ({partition}/7 partitions)")

    if "start creating slideshow" in lowered:
        return ("processing", 72, "Assembling slideshow")
    if "start merging" in lowered:
        return ("processing", 85, "Merging video streams")
    if "muxing" in lowered:
        return ("processing", 92, "Finalising video")
    return None


async def _stream_process_output(process: subprocess.Popen[str], job: Job, store: JobStore) -> list[str]:
    lines: list[str] = []
    assert process.stdout is not None
    while True:
        line = await asyncio.to_thread(process.stdout.readline)
        if not line:
            break
        text = line.strip()
        if not text:
            continue
        lines.append(text)

        updates: dict[str, str | int] = {}
        if progress := parse_progress_line(text):
            status, progress_value, stage = progress
            updates.update(status=status, progress=progress_value, stage=stage)

        if match := TITLE_RE.search(text):
            updates["title"] = match.group("value").strip()
        if match := DATE_RE.search(text):
            updates["date"] = match.group("value").strip()
        if match := DURATION_RE.search(text):
            updates["duration"] = match.group("value").strip()

        if updates:
            await store.update_job(job.id, **updates)
    return lines


async def _copy_if_exists(source: Path, destination: Path) -> bool:
    if not source.exists():
        return False
    await asyncio.to_thread(shutil.copy2, source, destination)
    return True


async def _zip_slide_svgs(source_root: Path, zip_path: Path) -> bool:
    slide_files = sorted(source_root.rglob("*.svg"))
    if not slide_files:
        return False

    def _write_zip() -> None:
        with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
            for slide in slide_files:
                archive.write(slide, arcname=slide.relative_to(source_root))

    await asyncio.to_thread(_write_zip)
    return True


async def _extract_audio(webcams_path: Path, audio_path: Path) -> bool:
    if not webcams_path.exists():
        return False
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        str(webcams_path),
        "-vn",
        "-q:a",
        "0",
        str(audio_path),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return (await process.wait()) == 0 and audio_path.exists()


async def cleanup_job_directories(job: Job) -> None:
    for path in (job.temp_dir, job.output_dir):
        if path.exists():
            await asyncio.to_thread(shutil.rmtree, path, True)


async def _collect_output_files(job: Job) -> list[str]:
    job.output_dir.mkdir(parents=True, exist_ok=True)

    mp4_candidates = sorted(job.temp_dir.rglob("*.mp4"))
    if mp4_candidates:
        destination = job.output_dir / FILE_NAME_MAP["mp4"]
        await asyncio.to_thread(shutil.move, str(mp4_candidates[0]), str(destination))

    located = {
        "webcam_webm": next(iter(sorted(job.temp_dir.rglob("webcams.webm"))), None),
        "deskshare_webm": next(iter(sorted(job.temp_dir.rglob("deskshare.webm"))), None),
        "transcript_json": next(iter(sorted(job.temp_dir.rglob("presentation_text.json"))), None),
        "notes_html": next(iter(sorted(job.temp_dir.rglob("notes.html"))), None),
    }

    for file_type, source in located.items():
        if source is not None:
            await _copy_if_exists(source, job.output_dir / FILE_NAME_MAP[file_type])

    webcams_path = job.output_dir / FILE_NAME_MAP["webcam_webm"]
    audio_path = job.output_dir / FILE_NAME_MAP["audio_mp3"]
    with contextlib.suppress(Exception):
        await _extract_audio(webcams_path, audio_path)

    await _zip_slide_svgs(job.temp_dir, job.output_dir / FILE_NAME_MAP["slides_zip"])

    return [
        file_type
        for file_type in FILE_TYPE_ORDER
        if (job.output_dir / FILE_NAME_MAP[file_type]).exists()
    ]


async def run_job(job: Job, store: JobStore) -> None:
    process: subprocess.Popen[str] | None = None
    try:
        await store.update_job(job.id, status="downloading", progress=0, stage="Starting download")
        job.temp_dir.mkdir(parents=True, exist_ok=True)
        job.output_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            "bbb-dl",
            job.url,
            "--output-dir",
            str(job.temp_dir),
            "--max-parallel-chromes",
            "2",
        ]
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        lines = await _stream_process_output(process, job, store)
        return_code = await asyncio.to_thread(process.wait)
        if return_code != 0:
            error = lines[-1] if lines else "bbb-dl failed"
            await store.update_job(job.id, status="failed", stage="Failed", error=error)
            return

        available_files = await _collect_output_files(job)
        title = job.title
        if title is None and (job.output_dir / FILE_NAME_MAP["mp4"]).exists():
            title = (job.output_dir / FILE_NAME_MAP["mp4"]).stem.replace("_", " ")

        await store.update_job(
            job.id,
            status="ready",
            progress=100,
            stage="Done",
            title=title,
            available_files=available_files,
            error=None,
        )
    except asyncio.CancelledError:
        if process is not None and process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
            with contextlib.suppress(Exception):
                await asyncio.to_thread(process.wait, 10)
        await cleanup_job_directories(job)
        raise
    except Exception as exc:
        await store.update_job(job.id, status="failed", stage="Failed", error=str(exc))
