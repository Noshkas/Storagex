from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, Iterable

from .constants import FPS, FRAME_HEIGHT, FRAME_WIDTH

ProgressCallback = Callable[[int, str], None]
FRAME_PROGRESS_PATTERN = re.compile(r"frame=\s*(\d+)")
YOUTUBE_MIN_DURATION_SECONDS = 5
RAW_FRAME_SIZE = FRAME_WIDTH * FRAME_HEIGHT
YOUTUBE_ENCODER_ENV = "STORAGEX_YOUTUBE_ENCODER"


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
    pad_duration = _pad_duration(frame_count=effective_frame_count, fps=fps, min_duration_seconds=min_duration_seconds)
    filter_parts = []
    if pad_duration > 0:
        filter_parts.append(f"tpad=stop_mode=clone:stop_duration={pad_duration:.3f}")
    filter_parts.append("format=yuv420p")

    _, video_encoder_args = _resolve_youtube_video_encoder()
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
        *video_encoder_args,
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


def encode_raw_frames_to_mkv(frame_source: Iterable[bytes], output_path: Path, *, fps: int = FPS) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        ffmpeg_executable(),
        "-y",
        "-loglevel",
        "error",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "gray",
        "-video_size",
        f"{FRAME_WIDTH}x{FRAME_HEIGHT}",
        "-framerate",
        str(fps),
        "-i",
        "pipe:0",
        "-c:v",
        "ffv1",
        "-level",
        "3",
        "-pix_fmt",
        "gray",
        str(output_path),
    ]
    _run_raw_frame_encoder(
        frame_source,
        command=command,
        error_message="Failed to encode the raw frame sequence into Matroska.",
    )


def encode_raw_frames_to_youtube_mp4(
    frame_source: Iterable[bytes],
    output_path: Path,
    *,
    fps: int = FPS,
    frame_count: int,
    min_duration_seconds: int = YOUTUBE_MIN_DURATION_SECONDS,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pad_duration = _pad_duration(frame_count=frame_count, fps=fps, min_duration_seconds=min_duration_seconds)
    filter_parts = []
    if pad_duration > 0:
        filter_parts.append(f"tpad=stop_mode=clone:stop_duration={pad_duration:.3f}")
    filter_parts.append("format=yuv420p")

    _, video_encoder_args = _resolve_youtube_video_encoder()
    command = [
        ffmpeg_executable(),
        "-y",
        "-loglevel",
        "error",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "gray",
        "-video_size",
        f"{FRAME_WIDTH}x{FRAME_HEIGHT}",
        "-framerate",
        str(fps),
        "-i",
        "pipe:0",
        "-f",
        "lavfi",
        "-i",
        "anullsrc=channel_layout=stereo:sample_rate=48000",
        "-vf",
        ",".join(filter_parts),
        *video_encoder_args,
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-shortest",
        "-movflags",
        "+faststart",
        str(output_path),
    ]
    _run_raw_frame_encoder(
        frame_source,
        command=command,
        error_message="Failed to encode the raw frame sequence into a YouTube-compatible MP4.",
    )


def stream_video_frames(
    video_path: Path,
    *,
    frame_handler: Callable[[bytes, int], None],
    progress: ProgressCallback | None = None,
) -> ExtractionResult:
    command = [
        ffmpeg_executable(),
        "-y",
        "-loglevel",
        "error",
        "-err_detect",
        "ignore_err",
        "-fflags",
        "+discardcorrupt",
        "-i",
        str(video_path),
        "-fps_mode",
        "passthrough",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "gray",
        "pipe:1",
    ]
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    assert process.stderr is not None

    extracted_frames = 0
    partial_warning = False
    try:
        while True:
            frame_bytes = _read_exact(process.stdout, RAW_FRAME_SIZE)
            if frame_bytes is None:
                break
            if len(frame_bytes) != RAW_FRAME_SIZE:
                partial_warning = True
                break

            extracted_frames += 1
            frame_handler(frame_bytes, extracted_frames)
            if progress is not None:
                progress_value = min(54, 5 + min(extracted_frames, 49))
                progress(progress_value, f"Extracted frame {extracted_frames}.")
    except Exception:
        process.kill()
        process.wait()
        raise

    stderr_output = process.stderr.read().decode("utf-8", errors="replace")
    return_code = process.wait()
    warning = stderr_output.strip() or None
    if partial_warning and warning is None:
        warning = "ffmpeg returned a partial trailing frame while decoding the uploaded video."

    if extracted_frames == 0:
        raise RuntimeError(f"Failed to extract frames from the uploaded video. {stderr_output.strip()}".strip())
    if return_code != 0 and warning is None:
        warning = "ffmpeg reported a non-clean exit after decoding the uploaded video."

    return ExtractionResult(extracted_frames=extracted_frames, warning=warning)


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
        "Failed to extract frames from the uploaded video.",
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
        raise RuntimeError(f"Failed to extract frames from the uploaded video. {stderr}".strip())

    warning = None
    if completed.returncode != 0:
        warning = completed.stderr.strip() or "ffmpeg reported a non-clean exit after extracting frames."

    return ExtractionResult(
        extracted_frames=extracted_frames,
        warning=warning,
    )


def _run_raw_frame_encoder(
    frame_source: Iterable[bytes],
    *,
    command: list[str],
    error_message: str,
) -> None:
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    assert process.stdin is not None
    assert process.stderr is not None

    try:
        for frame_bytes in frame_source:
            process.stdin.write(frame_bytes)
        process.stdin.close()
        stderr_output = process.stderr.read().decode("utf-8", errors="replace")
        return_code = process.wait()
    except Exception:
        process.kill()
        process.wait()
        raise

    if return_code != 0:
        raise RuntimeError(f"{error_message} {stderr_output.strip()}".strip())


def _read_exact(handle, size: int) -> bytes | None:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = handle.read(size - len(chunks))
        if not chunk:
            if not chunks:
                return None
            return bytes(chunks)
        chunks.extend(chunk)
    return bytes(chunks)


def _pad_duration(*, frame_count: int | None, fps: int, min_duration_seconds: int) -> float:
    effective_frame_count = frame_count or 0
    current_duration = (effective_frame_count / fps) if effective_frame_count and fps > 0 else 0
    return max(0.0, float(min_duration_seconds) - current_duration)


def _resolve_youtube_video_encoder() -> tuple[str, list[str]]:
    override = os.environ.get(YOUTUBE_ENCODER_ENV, "").strip().lower()
    if override in {"videotoolbox", "h264_videotoolbox"}:
        return "h264_videotoolbox", _videotoolbox_encoder_args()
    if override in {"libx264", "x264"}:
        return "libx264", _libx264_encoder_args()

    if sys.platform == "darwin" and "h264_videotoolbox" in _available_ffmpeg_encoders():
        return "h264_videotoolbox", _videotoolbox_encoder_args()
    return "libx264", _libx264_encoder_args()


@lru_cache(maxsize=1)
def _available_ffmpeg_encoders() -> frozenset[str]:
    completed = subprocess.run(
        [ffmpeg_executable(), "-hide_banner", "-encoders"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return frozenset()

    encoders: set[str] = set()
    for raw_line in completed.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2 or not parts[0].startswith("V"):
            continue
        encoders.add(parts[1])
    return frozenset(encoders)


def _libx264_encoder_args() -> list[str]:
    return [
        "-c:v",
        "libx264",
        "-preset",
        "superfast",
        "-crf",
        "18",
        "-profile:v",
        "high",
        "-level",
        "4.0",
        "-pix_fmt",
        "yuv420p",
    ]


def _videotoolbox_encoder_args() -> list[str]:
    return [
        "-c:v",
        "h264_videotoolbox",
        "-b:v",
        "40M",
        "-maxrate",
        "80M",
        "-bufsize",
        "120M",
        "-pix_fmt",
        "yuv420p",
        "-allow_sw",
        "1",
    ]


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
