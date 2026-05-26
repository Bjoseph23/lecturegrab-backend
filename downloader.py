from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from jobs import AUTO_FILE_TYPES, DERIVED_FILE_TYPES, FileTaskStatus, Job, JobStore, READY_FILE_TYPES


logger = logging.getLogger("bbb-backend.downloader")

BBB_URL_RE = re.compile(
    r"^https://[^/]+/playback/presentation/2\.[^/]+/[^/?#]+/?$",
    re.IGNORECASE,
)
PARTITION_RE = re.compile(r"(?P<done>\d+)/7 Partition finished")
TITLE_RE = re.compile(r"Title:\s*(?P<value>.+)")
DATE_RE = re.compile(r"Date:\s*(?P<value>.+)")
DURATION_RE = re.compile(r"Duration:\s*(?P<value>.+)")
FINAL_VIDEO_RE = re.compile(r"All done!\s+Final video:\s*(?P<value>.+\.mp4)", re.IGNORECASE)
TIME_PROGRESS_RE = re.compile(r"Time:\s*(?P<value>\d+:\d{2}(?::\d{2})?)")
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


def artifact_path(job: Job, file_type: str) -> Path:
    return job.output_dir / FILE_NAME_MAP[file_type]


def _status_for_existing_file(job: Job, file_type: str) -> FileTaskStatus:
    path = artifact_path(job, file_type)
    if path.exists():
        return FileTaskStatus(
            status="ready",
            progress=100,
            stage="Ready",
            requested_at=datetime.now(UTC),
        )
    if file_type in AUTO_FILE_TYPES:
        return FileTaskStatus(status="pending", progress=0, stage="Waiting for base recording")
    return FileTaskStatus(status="pending", progress=0, stage="Ready to process on request")


async def initialize_file_statuses(job: Job, store: JobStore) -> None:
    for file_type in FILE_TYPE_ORDER:
        status = _status_for_existing_file(job, file_type)
        await store.update_file_status(
            job.id,
            file_type,
            status=status.status,
            progress=status.progress,
            stage=status.stage,
            error=status.error,
            requested_at=status.requested_at,
        )


async def refresh_available_files(job: Job, store: JobStore) -> list[str]:
    available = [
        file_type
        for file_type in FILE_TYPE_ORDER
        if artifact_path(job, file_type).exists()
    ]
    await store.update_job(job.id, available_files=available)
    return available


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
        logger.info("bbb-dl[%s] %s", job.id, text)

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


def _normalise_duration(value: str | None) -> str | None:
    if value is None:
        return None
    return value.split(".")[0].strip()


def _extract_final_video_path(lines: list[str]) -> Path | None:
    for line in reversed(lines):
        match = FINAL_VIDEO_RE.search(line)
        if match:
            return Path(match.group("value").strip())
    return None


def _extract_last_duration(lines: list[str]) -> str | None:
    for line in reversed(lines):
        match = TIME_PROGRESS_RE.search(line)
        if match:
            return _normalise_duration(match.group("value"))
    return None


def _extract_metadata_from_final_video_path(video_path: Path) -> tuple[str | None, str | None]:
    stem = video_path.stem
    if "_" not in stem:
        return None, None

    prefix, raw_title = stem.split("_", 1)
    date_value = prefix[:10] if len(prefix) >= 10 else None
    cleaned_title = raw_title.replace("_", " ").strip()
    cleaned_title = re.sub(r"\s*\(All participants\)\s*$", "", cleaned_title, flags=re.IGNORECASE).strip()
    return cleaned_title or None, date_value


def _find_first_existing(job: Job, *patterns: str) -> Path | None:
    for pattern in patterns:
        matches = sorted(job.temp_dir.rglob(pattern))
        if matches:
            return matches[0]
    return None


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


async def _transcode_final_video(source: Path, destination: Path) -> bool:
    if not source.exists():
        return False

    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-i",
        str(source),
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        str(destination),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return (await process.wait()) == 0 and destination.exists()


async def _publish_mp4(job: Job, store: JobStore, source: Path, stage: str) -> bool:
    destination = artifact_path(job, "mp4")
    await store.update_file_status(job.id, "mp4", status="processing", progress=75, stage=stage, error=None)
    transcode_ok = await _transcode_final_video(source, destination)
    if not transcode_ok and source.suffix.lower() == ".mp4":
        await asyncio.to_thread(shutil.copy2, source, destination)
    if destination.exists():
        await store.update_file_status(job.id, "mp4", status="ready", progress=100, stage="Ready", requested_at=datetime.now(UTC))
        return True
    await store.update_file_status(
        job.id,
        "mp4",
        status="failed",
        progress=0,
        stage="Video export failed",
        error="Unable to generate a browser-compatible MP4",
    )
    return False


async def _mark_unavailable_auto_files(job: Job, store: JobStore, message: str) -> None:
    for file_type in AUTO_FILE_TYPES:
        if artifact_path(job, file_type).exists():
            continue
        await store.update_file_status(
            job.id,
            file_type,
            status="failed",
            progress=0,
            stage="Unavailable",
            error=message,
        )


async def cleanup_job_directories(job: Job) -> None:
    for path in (job.temp_dir, job.output_dir):
        if path.exists():
            await asyncio.to_thread(shutil.rmtree, path, True)


async def collect_base_files(job: Job, store: JobStore, final_video_source: Path | None, *, allow_fallback_mp4: bool = False) -> list[str]:
    job.output_dir.mkdir(parents=True, exist_ok=True)

    webcam_source = _find_first_existing(job, "webcams.webm", "webcams.mp4")
    deskshare_source = _find_first_existing(job, "deskshare.webm", "deskshare.mp4")

    mp4_source = final_video_source if final_video_source and final_video_source.exists() else None
    if mp4_source is not None:
        await _publish_mp4(job, store, mp4_source, "Publishing lecture video")
    elif allow_fallback_mp4:
        fallback_source = webcam_source or deskshare_source
        if fallback_source is not None:
            await _publish_mp4(job, store, fallback_source, "Creating fallback lecture video")

    located = {
        "webcam_webm": webcam_source if webcam_source and webcam_source.suffix.lower() == ".webm" else None,
        "deskshare_webm": deskshare_source if deskshare_source and deskshare_source.suffix.lower() == ".webm" else None,
    }

    for file_type, source in located.items():
        if source is None:
            continue
        await store.update_file_status(job.id, file_type, status="processing", progress=80, stage="Publishing base file")
        copied = await _copy_if_exists(source, artifact_path(job, file_type))
        if copied:
            await store.update_file_status(job.id, file_type, status="ready", progress=100, stage="Ready", requested_at=datetime.now(UTC))
        else:
            await store.update_file_status(job.id, file_type, status="failed", progress=0, stage="Missing source file", error="Source file missing")

    await initialize_file_statuses(job, store)
    return await refresh_available_files(job, store)


async def _process_audio(job: Job, store: JobStore) -> bool:
    webcams_path = artifact_path(job, "webcam_webm")
    audio_path = artifact_path(job, "audio_mp3")
    await store.update_file_status(job.id, "audio_mp3", status="processing", progress=15, stage="Checking webcam audio", error=None)
    if not webcams_path.exists():
        await store.update_file_status(job.id, "audio_mp3", status="failed", progress=0, stage="Webcam file missing", error="webcams.webm is required")
        return False
    await store.update_file_status(job.id, "audio_mp3", progress=55, stage="Extracting MP3 audio")
    created = await _extract_audio(webcams_path, audio_path)
    if created:
        await store.update_file_status(job.id, "audio_mp3", status="ready", progress=100, stage="Ready", requested_at=datetime.now(UTC))
        await refresh_available_files(job, store)
        return True
    await store.update_file_status(job.id, "audio_mp3", status="failed", progress=0, stage="Audio extraction failed", error="ffmpeg could not create audio.mp3")
    return False


async def _process_slides(job: Job, store: JobStore) -> bool:
    slides_zip_path = artifact_path(job, "slides_zip")
    await store.update_file_status(job.id, "slides_zip", status="processing", progress=20, stage="Scanning slide assets", error=None)
    await store.update_file_status(job.id, "slides_zip", progress=70, stage="Packaging slides")
    created = await _zip_slide_svgs(job.temp_dir, slides_zip_path)
    if created:
        await store.update_file_status(job.id, "slides_zip", status="ready", progress=100, stage="Ready", requested_at=datetime.now(UTC))
        await refresh_available_files(job, store)
        return True
    await store.update_file_status(job.id, "slides_zip", status="failed", progress=0, stage="No slides found", error="No SVG slide files were generated")
    return False


async def _process_simple_copy(job: Job, store: JobStore, file_type: str, source_name: str, stage: str) -> bool:
    destination = artifact_path(job, file_type)
    await store.update_file_status(job.id, file_type, status="processing", progress=35, stage=stage, error=None)
    source = next(iter(sorted(job.temp_dir.rglob(source_name))), None)
    if source is None:
        await store.update_file_status(job.id, file_type, status="failed", progress=0, stage="Source missing", error=f"{source_name} was not generated")
        return False
    copied = await _copy_if_exists(source, destination)
    if copied:
        await store.update_file_status(job.id, file_type, status="ready", progress=100, stage="Ready", requested_at=datetime.now(UTC))
        await refresh_available_files(job, store)
        return True
    await store.update_file_status(job.id, file_type, status="failed", progress=0, stage="Copy failed", error=f"Could not copy {source_name}")
    return False


async def process_file(job: Job, store: JobStore, file_type: str) -> bool:
    if file_type not in READY_FILE_TYPES:
        raise ValueError(f"Unsupported file type: {file_type}")

    if file_type in AUTO_FILE_TYPES:
        path = artifact_path(job, file_type)
        if path.exists():
            await store.update_file_status(job.id, file_type, status="ready", progress=100, stage="Ready", requested_at=datetime.now(UTC))
            await refresh_available_files(job, store)
            return True
        await store.update_file_status(job.id, file_type, status="failed", progress=0, stage="Base file missing", error="Base recording file is not available")
        return False

    if file_type == "audio_mp3":
        return await _process_audio(job, store)
    if file_type == "slides_zip":
        return await _process_slides(job, store)
    if file_type == "transcript_json":
        return await _process_simple_copy(job, store, file_type, "presentation_text.json", "Collecting transcript")
    if file_type == "notes_html":
        return await _process_simple_copy(job, store, file_type, "notes.html", "Collecting notes")
    raise ValueError(f"Unhandled file type: {file_type}")


async def run_file_task(job_id: str, file_type: str, store: JobStore) -> None:
    job = await store.require_job(job_id)
    try:
        await process_file(job, store, file_type)
    except asyncio.CancelledError:
        await store.update_file_status(job_id, file_type, status="pending", progress=0, stage="Cancelled")
        raise
    except Exception as exc:
        logger.exception("File processing failed for job %s file %s", job_id, file_type)
        await store.update_file_status(job_id, file_type, status="failed", progress=0, stage="Processing failed", error=str(exc))
    finally:
        await store.pop_file_task(job_id, file_type)


async def _auto_process_derived_files(job: Job, store: JobStore) -> None:
    for file_type in ("audio_mp3", "slides_zip", "transcript_json", "notes_html"):
        try:
            await process_file(job, store, file_type)
        except Exception:
            logger.exception("Automatic processing failed for job %s file %s", job.id, file_type)


async def _finish_job_from_artifacts(
    job: Job,
    store: JobStore,
    lines: list[str],
    final_video_source: Path | None,
    *,
    stage: str,
    allow_fallback_mp4: bool,
) -> bool:
    await collect_base_files(job, store, final_video_source, allow_fallback_mp4=allow_fallback_mp4)
    await _auto_process_derived_files(job, store)
    await _mark_unavailable_auto_files(job, store, "BBB export did not generate this file")
    available_files = await refresh_available_files(job, store)
    if not available_files:
        return False

    title = job.title
    date_value = job.date
    duration_value = _normalise_duration(job.duration) or _extract_last_duration(lines)
    if final_video_source is not None:
        inferred_title, inferred_date = _extract_metadata_from_final_video_path(final_video_source)
        title = title or inferred_title
        date_value = date_value or inferred_date
    if title is None and artifact_path(job, "mp4").exists():
        title = artifact_path(job, "mp4").stem.replace("_", " ")

    await store.update_job(
        job.id,
        status="ready",
        progress=100,
        stage=stage,
        title=title,
        date=date_value,
        duration=duration_value,
        available_files=available_files,
        error=None,
    )
    return True


async def run_job(job: Job, store: JobStore) -> None:
    process: subprocess.Popen[str] | None = None
    try:
        await store.update_job(job.id, status="downloading", progress=0, stage="Starting download")
        for file_type in FILE_TYPE_ORDER:
            initial_stage = "Waiting for base recording" if file_type in AUTO_FILE_TYPES else "Ready to process on request"
            await store.update_file_status(job.id, file_type, status="pending", progress=0, stage=initial_stage, error=None)

        job.temp_dir.mkdir(parents=True, exist_ok=True)
        job.output_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            "bbb-dl",
            job.url,
            "--output-dir",
            str(job.temp_dir),
            "--working-dir",
            str(job.temp_dir),
            "--keep-tmp-files",
            "--audiocodec",
            "aac",
            "--max-parallel-chromes",
            "2",
        ]
        logger.info("Starting bbb-dl job %s for %s", job.id, job.url)
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        lines = await _stream_process_output(process, job, store)
        return_code = await asyncio.to_thread(process.wait)
        final_video_source = _extract_final_video_path(lines)
        if return_code != 0:
            error = lines[-1] if lines else "bbb-dl failed"
            logger.error("bbb-dl job %s failed: %s", job.id, error)
            await store.update_job(job.id, status="processing", progress=max(job.progress, 80), stage="Recovering available outputs")
            recovered = await _finish_job_from_artifacts(
                job,
                store,
                lines,
                final_video_source,
                stage="Done (recovered from BBB render failure)",
                allow_fallback_mp4=True,
            )
            if recovered:
                logger.warning("bbb-dl job %s recovered after renderer failure", job.id)
                return
            await store.update_job(job.id, status="failed", stage="Failed", error=error)
            return

        completed = await _finish_job_from_artifacts(
            job,
            store,
            lines,
            final_video_source,
            stage="Done",
            allow_fallback_mp4=False,
        )
        if not completed:
            await store.update_job(job.id, status="failed", stage="Failed", error="bbb-dl completed without producing any downloadable files")
            return
        logger.info("bbb-dl job %s completed", job.id)
    except asyncio.CancelledError:
        logger.info("bbb-dl job %s cancelled", job.id)
        if process is not None and process.poll() is None:
            with contextlib.suppress(Exception):
                process.terminate()
            with contextlib.suppress(Exception):
                await asyncio.to_thread(process.wait, 10)
        await cleanup_job_directories(job)
        raise
    except Exception as exc:
        logger.exception("Unexpected error while running job %s", job.id)
        await store.update_job(job.id, status="failed", stage="Failed", error=str(exc))
