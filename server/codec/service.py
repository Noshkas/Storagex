from __future__ import annotations

import hashlib
import json
import math
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
from PIL import Image, UnidentifiedImageError

from .constants import (
    CELL_SIZE,
    FPS,
    FRAME_HEIGHT,
    FRAME_PATTERN,
    FRAME_WIDTH,
    FRAMES_ARCHIVE_NAME,
    GRID_COLS,
    GRID_ROWS,
    MANIFEST_NAME,
    QUIET_MARGIN,
    RECOVERED_MANIFEST_NAME,
    VIDEO_NAME,
)
from .format import (
    FRAME_HEADER_STRUCT,
    FRAME_MAGIC,
    LAYOUT,
    bits_to_bytes,
    build_stream_with_manifest,
    bytes_to_bits,
    parse_stream,
    sanitize_filename,
    validate_manifest,
)
from .keyed import scramble_payload, unscramble_payload, validate_numeric_key
from .video import encode_frames_to_webm, extract_video_frames

ProgressCallback = Callable[[int, str], None]


class CodecError(RuntimeError):
    pass


@dataclass(slots=True)
class EncodeResult:
    manifest: dict[str, object]
    video_path: Path
    frames_zip_path: Path
    manifest_path: Path
    frame_paths: list[Path]


@dataclass(slots=True)
class DecodeResult:
    manifest: dict[str, object]
    restored_path: Path
    manifest_path: Path
    extracted_frame_paths: list[Path]
    integrity_ok: bool


def encode_file(
    *,
    source_path: Path,
    original_filename: str,
    media_type: str,
    key: str,
    job_dir: Path,
    progress: ProgressCallback | None = None,
) -> EncodeResult:
    update = progress or _noop_progress
    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise CodecError(str(exc)) from exc
    source_bytes = source_path.read_bytes()
    protected_bytes = scramble_payload(source_bytes, normalized_key)
    manifest, _stream_bytes, chunks = build_stream_with_manifest(
        original_filename=original_filename,
        media_type=media_type,
        original_bytes=source_bytes,
        stored_bytes=protected_bytes,
    )

    frames_dir = job_dir / "frames"
    output_dir = job_dir / "output"
    frames_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    update(10, "Prepared manifest and bit stream.")

    frame_paths: list[Path] = []
    for frame_index, chunk in enumerate(chunks):
        frame_path = frames_dir / (FRAME_PATTERN % (frame_index + 1))
        grid = _render_frame_grid(frame_index=frame_index, chunk=chunk)
        _write_frame_png(grid, frame_path)
        frame_paths.append(frame_path)
        frame_progress = 10 + math.floor(((frame_index + 1) / len(chunks)) * 60)
        update(frame_progress, f"Rendered frame {frame_index + 1} of {len(chunks)}.")

    manifest_path = job_dir / MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    update(75, "Saved manifest.")

    frames_zip_path = job_dir / FRAMES_ARCHIVE_NAME
    _archive_frames(frame_paths, job_dir, frames_zip_path)
    update(82, "Archived PNG frames.")

    video_path = job_dir / VIDEO_NAME
    try:
        encode_frames_to_webm(frames_dir, video_path, fps=FPS)
    except RuntimeError as exc:
        raise CodecError(str(exc)) from exc
    update(100, "Encoded lossless WebM.")

    return EncodeResult(
        manifest=manifest,
        video_path=video_path,
        frames_zip_path=frames_zip_path,
        manifest_path=manifest_path,
        frame_paths=frame_paths,
    )


def decode_video(
    *,
    video_path: Path,
    job_dir: Path,
    key: str,
    allow_duplicate_frame_chunks: bool = False,
    progress: ProgressCallback | None = None,
) -> DecodeResult:
    update = progress or _noop_progress
    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise CodecError(str(exc)) from exc
    frames_dir = job_dir / "decoded_frames"
    update(2, "Inspecting uploaded WebM.")
    try:
        extraction = extract_video_frames(
            video_path,
            frames_dir,
            progress=lambda progress_value, message: update(progress_value, message),
        )
    except RuntimeError as exc:
        raise CodecError(str(exc)) from exc

    if extraction.warning:
        update(55, f"Extracted {extraction.extracted_frames} frames with a recoverable ffmpeg warning.")
    else:
        update(55, f"Extracted {extraction.extracted_frames} frames.")

    return decode_video_from_frames(
        frames_dir=frames_dir,
        job_dir=job_dir,
        progress=update,
        key=normalized_key,
        allow_duplicate_frame_chunks=allow_duplicate_frame_chunks,
    )


def decode_video_from_frames(
    *,
    frames_dir: Path,
    job_dir: Path,
    key: str,
    allow_duplicate_frame_chunks: bool = False,
    progress: ProgressCallback | None = None,
) -> DecodeResult:
    update = progress or _noop_progress
    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise CodecError(str(exc)) from exc
    frame_paths = _collect_decodable_frame_paths(frames_dir)
    if not frame_paths:
        raise CodecError("No frames were extracted from the uploaded video.")

    update(60, f"Reading {len(frame_paths)} extracted frames.")
    chunk_map: dict[int, bytes] = {}
    for index, frame_path in enumerate(frame_paths, start=1):
        grid = _read_grid_from_png(frame_path)
        _validate_reserved_cells(grid)
        frame_index, chunk = _parse_frame_grid(grid)
        if frame_index in chunk_map:
            if allow_duplicate_frame_chunks and chunk_map[frame_index] == chunk:
                continue
            raise CodecError(f"Duplicate frame index {frame_index} detected.")
        chunk_map[frame_index] = chunk
        frame_progress = 60 + math.floor((index / len(frame_paths)) * 35)
        update(frame_progress, f"Decoded frame {index} of {len(frame_paths)}.")

    if not chunk_map:
        raise CodecError("No payload frames were decoded from the video.")

    ordered_indexes = sorted(chunk_map)
    if ordered_indexes[0] != 0 or ordered_indexes != list(range(len(ordered_indexes))):
        raise CodecError("Frame sequence is incomplete or out of range.")

    stream_bytes = b"".join(chunk_map[index] for index in ordered_indexes)

    try:
        manifest, protected_bytes = parse_stream(stream_bytes)
        validate_manifest(manifest)
    except ValueError as exc:
        raise CodecError(str(exc)) from exc

    total_frames = int(manifest["total_frames"])
    if total_frames != len(ordered_indexes):
        raise CodecError("Frame count does not match the encoded manifest.")

    update(96, "Validating decoded payload.")

    if len(protected_bytes) != int(manifest["stored_size"]):
        raise CodecError("Stored payload length does not match the manifest.")

    restored_bytes = unscramble_payload(protected_bytes, normalized_key)

    if len(restored_bytes) != int(manifest["original_size"]):
        raise CodecError("Recovered payload length does not match the manifest.")

    crc32 = f"{zlib.crc32(restored_bytes) & 0xFFFFFFFF:08x}"
    sha256 = hashlib.sha256(restored_bytes).hexdigest()
    integrity_ok = crc32 == manifest["crc32"] and sha256 == manifest["sha256"]

    output_dir = job_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = job_dir / RECOVERED_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    restored_name = sanitize_filename(str(manifest["original_filename"]), fallback="restored.bin")
    restored_path = output_dir / restored_name
    restored_path.write_bytes(restored_bytes)
    if integrity_ok:
        update(100, "Recovered original file bytes.")
    else:
        update(100, "Recovered file bytes, but integrity validation failed.")

    return DecodeResult(
        manifest=manifest,
        restored_path=restored_path,
        manifest_path=manifest_path,
        extracted_frame_paths=frame_paths,
        integrity_ok=integrity_ok,
    )


def _render_frame_grid(*, frame_index: int, chunk: bytes) -> np.ndarray:
    if len(chunk) > LAYOUT.chunk_byte_capacity:
        raise CodecError("Chunk exceeds frame payload capacity.")

    header = FRAME_HEADER_STRUCT.pack(FRAME_MAGIC, frame_index, len(chunk), zlib.crc32(chunk) & 0xFFFFFFFF)
    payload_bits = bytes_to_bits(header + chunk)
    if payload_bits.size > LAYOUT.payload_bit_capacity:
        raise CodecError("Packed payload exceeds frame bit capacity.")

    grid = LAYOUT.base_grid.copy()
    rows = LAYOUT.payload_rows[: payload_bits.size]
    cols = LAYOUT.payload_cols[: payload_bits.size]
    grid[rows, cols] = payload_bits
    return grid


def _write_frame_png(grid: np.ndarray, output_path: Path) -> None:
    canvas = np.full((FRAME_HEIGHT, FRAME_WIDTH), 255, dtype=np.uint8)
    grid_pixels = np.where(grid > 0, 0, 255).astype(np.uint8)
    scaled = np.repeat(np.repeat(grid_pixels, CELL_SIZE, axis=0), CELL_SIZE, axis=1)
    canvas[QUIET_MARGIN : QUIET_MARGIN + scaled.shape[0], QUIET_MARGIN : QUIET_MARGIN + scaled.shape[1]] = scaled
    output_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(canvas, mode="L").save(output_path)


def _archive_frames(frame_paths: list[Path], job_dir: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as zip_file:
        for frame_path in frame_paths:
            zip_file.write(frame_path, arcname=frame_path.relative_to(job_dir))


def _read_grid_from_png(frame_path: Path) -> np.ndarray:
    frame = _load_frame_pixels(frame_path)

    if frame.shape != (FRAME_HEIGHT, FRAME_WIDTH):
        raise CodecError("Decoded frame dimensions do not match the expected geometry.")

    cropped = frame[QUIET_MARGIN:-QUIET_MARGIN, QUIET_MARGIN:-QUIET_MARGIN]
    cell_means = cropped.reshape(GRID_ROWS, CELL_SIZE, GRID_COLS, CELL_SIZE).mean(axis=(1, 3))
    return (cell_means < 128).astype(np.uint8)


def _validate_reserved_cells(grid: np.ndarray) -> None:
    expected = LAYOUT.base_grid[LAYOUT.reserved_mask]
    actual = grid[LAYOUT.reserved_mask]
    mismatches = int(np.count_nonzero(expected != actual))
    tolerance = max(8, int(expected.size * 0.01))
    if mismatches > tolerance:
        raise CodecError("Finder or timing pattern validation failed.")


def _parse_frame_grid(grid: np.ndarray) -> tuple[int, bytes]:
    payload_bits = grid[LAYOUT.payload_rows, LAYOUT.payload_cols]
    header_size = FRAME_HEADER_STRUCT.size
    header_bytes = bits_to_bytes(payload_bits[: header_size * 8], header_size)
    magic, frame_index, chunk_length, chunk_crc = FRAME_HEADER_STRUCT.unpack(header_bytes)

    if magic != FRAME_MAGIC:
        raise CodecError("Frame header magic did not match the app format.")
    if chunk_length > LAYOUT.chunk_byte_capacity:
        raise CodecError("Frame chunk length exceeds the payload capacity.")

    data_end = (header_size + chunk_length) * 8
    payload_bytes = bits_to_bytes(payload_bits[:data_end], header_size + chunk_length)
    chunk = payload_bytes[header_size:]
    if zlib.crc32(chunk) & 0xFFFFFFFF != chunk_crc:
        raise CodecError(f"Frame {frame_index} failed CRC validation.")
    return frame_index, chunk


def _noop_progress(_progress: int, _message: str) -> None:
    return None


def _collect_decodable_frame_paths(frames_dir: Path) -> list[Path]:
    frame_paths = sorted(frames_dir.glob("frame_*.png"))
    while frame_paths:
        try:
            _load_frame_pixels(frame_paths[-1])
            break
        except CodecError:
            frame_paths.pop()

    return frame_paths


def _load_frame_pixels(frame_path: Path) -> np.ndarray:
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            with Image.open(frame_path) as image:
                grayscale = image.convert("L")
                return np.asarray(grayscale, dtype=np.uint8)
        except (UnidentifiedImageError, OSError, ValueError) as exc:
            last_error = exc
            if attempt < 4:
                time.sleep(0.1)
                continue
            raise CodecError(f"Decoded frame {frame_path.name} is not a readable PNG image.") from exc

    raise CodecError(f"Decoded frame {frame_path.name} is not a readable PNG image.") from last_error
