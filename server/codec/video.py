from __future__ import annotations

import math
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .constants import FPS

ProgressCallback = Callable[[int, str], None]
FRAME_PROGRESS_PATTERN = re.compile(r"frame=\s*(\d+)")
YOUTUBE_MIN_DURATION_SECONDS = 5


@dataclass(slots=True)
class ExtractionResult:
    extracted_frames: int
    warning: str | None = None


def ffmpeg_executable() -> str:
    override = os.environ.get("IMAGEIO_FFMPEG_EXE")
    if override:
        return override

    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg:
        return system_ffmpeg

    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception as exc:
        raise RuntimeError(
            "No ffmpeg executable could be resolved. Install ffmpeg with `brew install ffmpeg`, "
            "or set the IMAGEIO_FFMPEG_EXE environment variable."
        ) from exc


def encode_frames_to_webm(frames_dir: Path, output_path: Path, fps: int = FPS) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame_pattern = str(frames_dir / "frame_%06d.png")
    command = [
        ffmpeg_executable(),
        "-y",
        "-framerate",
        str(fps),
        "-i",
        frame_pattern,
        "-c:v",
        "libvpx-vp9",
        "-lossless",
        "1",
        "-pix_fmt",
        "yuv420p",
        str(output_path),
    ]
    _run_ffmpeg(command, "Failed to encode the PNG frame sequence into WebM.")


def encode_frames_to_youtube_mp4(
    frames_dir: Path,
    output_path: Path,
    *,
    fps: int = FPS,
    frame_count: int | None = None,
    min_duration_seconds: int = YOUTUBE_MIN_DURATION_SECONDS,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame_pattern = str(frames_dir / "frame_%06d.png")
    effective_frame_count = frame_count if frame_count is not None else len(list(frames_dir.glob("frame_*.png")))
    current_duration = (effective_frame_count / fps) if effective_frame_count and fps > 0 else 0
    pad_duration = max(0.0, float(min_duration_seconds) - current_duration)
    filter_parts = []
    if pad_duration > 0:
        filter_parts.append(f"tpad=stop_mode=clone:stop_duration={pad_duration:.3f}")
    filter_parts.append("format=yuv420p")

    command = [
        ffmpeg_executable(),
        "-y",
        "-framerate",
        str(fps),
        "-i",
        frame_pattern,
        "-f",
        "lavfi",
        "-i",
        "anullsrc=channel_layout=stereo:sample_rate=48000",
        "-vf",
        ",".join(filter_parts),
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "18",
        "-profile:v",
        "high",
        "-level",
        "4.0",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-shortest",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    _run_ffmpeg(command, "Failed to encode the PNG frame sequence into a YouTube-compatible MP4.")


def extract_video_frames(
    video_path: Path,
    output_dir: Path,
    progress: ProgressCallback | None = None,
) -> ExtractionResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame_pattern = str(output_dir / "frame_%06d.png")

    command = [
        ffmpeg_executable(),
        "-y",
        "-loglevel",
        "error",
        "-nostats",
        "-progress",
        "pipe:1",
        "-err_detect",
        "ignore_err",
        "-fflags",
        "+discardcorrupt",
        "-i",
        str(video_path),
        "-fps_mode",
        "passthrough",
        "-vcodec",
        "png",
        frame_pattern,
    ]
    completed = _run_ffmpeg(
        command,
        "Failed to extract frames from the uploaded WebM.",
        progress=_frame_progress_callback(
            progress=progress,
            progress_start=5,
            progress_span=45,
            message_prefix="Extracting frame",
        ),
        allow_partial_output=True,
        partial_output_dir=output_dir,
    )

    extracted_frames = len(list(output_dir.glob("frame_*.png")))
    if extracted_frames == 0:
        stderr = completed.stderr.strip()
        raise RuntimeError(f"Failed to extract frames from the uploaded WebM. {stderr}".strip())

    warning = None
    if completed.returncode != 0:
        warning = completed.stderr.strip() or "ffmpeg reported a non-clean exit after extracting frames."

    return ExtractionResult(
        extracted_frames=extracted_frames,
        warning=warning,
    )


def _run_ffmpeg(
    command: list[str],
    error_message: str,
    *,
    progress: Callable[[int], None] | None = None,
    allow_partial_output: bool = False,
    partial_output_dir: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    if progress is None:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        if completed.returncode != 0:
            if allow_partial_output and partial_output_dir is not None and any(partial_output_dir.glob("frame_*.png")):
                return completed

            stderr = completed.stderr.strip()
            raise RuntimeError(f"{error_message} {stderr}".strip())
        return completed

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    assert process.stdout is not None
    assert process.stderr is not None

    for raw_line in process.stdout:
        line = raw_line.rstrip()
        stdout_lines.append(line)
        if progress is None:
            continue

        match = FRAME_PROGRESS_PATTERN.search(line)
        if match:
            progress(int(match.group(1)))

    stderr_output = process.stderr.read()
    if stderr_output:
        stderr_lines.extend(stderr_output.splitlines())

    return_code = process.wait()
    completed = subprocess.CompletedProcess(command, return_code, "\n".join(stdout_lines), "\n".join(stderr_lines))

    if completed.returncode != 0:
        if allow_partial_output and partial_output_dir is not None and any(partial_output_dir.glob("frame_*.png")):
            return completed

        stderr = completed.stderr.strip()
        raise RuntimeError(f"{error_message} {stderr}".strip())

    return completed


def _frame_progress_callback(
    *,
    progress: ProgressCallback | None,
    progress_start: int,
    progress_span: int,
    message_prefix: str,
) -> Callable[[int], None]:
    if progress is None:
        return lambda _frame_index: None

    def update(frame_index: int) -> None:
        progress_value = min(progress_start + progress_span - 1, progress_start + min(frame_index, progress_span))
        progress(progress_value, f"{message_prefix} {frame_index}.")

    return update
