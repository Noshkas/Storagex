from __future__ import annotations

import hashlib
import json
import math
import time
import zlib
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Callable
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
from PIL import Image, UnidentifiedImageError

from .constants import (
    CELL_SIZE,
    FORMAT_VERSION,
    FPS,
    FRAME_HEIGHT,
    FRAME_PATTERN,
    FRAME_WIDTH,
    FRAMES_ARCHIVE_NAME,
    GRID_COLS,
    GRID_ROWS,
    KEY_CHUNK_BYTES,
    MANIFEST_NAME,
    QUIET_MARGIN,
    RECOVERED_MANIFEST_NAME,
    VIDEO_NAME,
    YOUTUBE_VIDEO_NAME,
)
from .format import (
    FRAME_HEADER_STRUCT,
    FRAME_MAGIC,
    LAYOUT,
    STREAM_HEADER_STRUCT,
    assemble_stream_prefix,
    bits_to_bytes,
    build_manifest_for_payload,
    build_stream_with_manifest,
    bytes_to_bits,
    parse_stream,
    sanitize_filename,
    validate_manifest,
)
from .keyed import (
    LEGACY_KEY_MODE,
    STREAM_KEY_MODE,
    scramble_payload,
    stream_payload_transform,
    unscramble_payload,
    validate_numeric_key,
)
from .video import (
    encode_raw_frames_to_mkv,
    encode_raw_frames_to_youtube_mp4,
    stream_video_frames,
)

ProgressCallback = Callable[[int, str], None]


class CodecError(RuntimeError):
    pass


@dataclass(slots=True)
class EncodeResult:
    manifest: dict[str, object]
    video_path: Path
    manifest_path: Path
    frames_zip_path: Path | None = None
    frame_paths: list[Path] = field(default_factory=list)


@dataclass(slots=True)
class DecodeResult:
    manifest: dict[str, object]
    restored_path: Path
    manifest_path: Path
    extracted_frame_paths: list[Path] = field(default_factory=list)
    integrity_ok: bool = False


_BASE_FRAME_PIXELS = np.full((FRAME_HEIGHT, FRAME_WIDTH), 255, dtype=np.uint8)
_BASE_GRID_PIXELS = np.where(LAYOUT.base_grid > 0, 0, 255).astype(np.uint8)
_SCALED_BASE_GRID = np.repeat(np.repeat(_BASE_GRID_PIXELS, CELL_SIZE, axis=0), CELL_SIZE, axis=1)
_BASE_FRAME_PIXELS[
    QUIET_MARGIN : QUIET_MARGIN + _SCALED_BASE_GRID.shape[0],
    QUIET_MARGIN : QUIET_MARGIN + _SCALED_BASE_GRID.shape[1],
] = _SCALED_BASE_GRID
_PAYLOAD_PIXEL_INDEXES = (
    (
        QUIET_MARGIN
        + (LAYOUT.payload_rows[:, None, None] * CELL_SIZE)
        + np.arange(CELL_SIZE, dtype=np.int32)[None, :, None]
    )
    * FRAME_WIDTH
    + (
        QUIET_MARGIN
        + (LAYOUT.payload_cols[:, None, None] * CELL_SIZE)
        + np.arange(CELL_SIZE, dtype=np.int32)[None, None, :]
    )
).reshape(-1)


def encode_file(
    *,
    source_path: Path,
    original_filename: str,
    media_type: str,
    key: str,
    job_dir: Path,
    progress: ProgressCallback | None = None,
    debug_artifacts: bool = False,
) -> EncodeResult:
    return _encode_source(
        source_path=source_path,
        original_filename=original_filename,
        media_type=media_type,
        key=key,
        job_dir=job_dir,
        output_path=job_dir / VIDEO_NAME,
        target="local",
        progress=progress,
        debug_artifacts=debug_artifacts,
    )


def encode_file_for_youtube_upload(
    *,
    source_path: Path,
    original_filename: str,
    media_type: str,
    key: str,
    job_dir: Path,
    progress: ProgressCallback | None = None,
) -> EncodeResult:
    return _encode_source(
        source_path=source_path,
        original_filename=original_filename,
        media_type=media_type,
        key=key,
        job_dir=job_dir,
        output_path=job_dir / YOUTUBE_VIDEO_NAME,
        target="youtube",
        progress=progress,
        debug_artifacts=False,
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

    output_dir = job_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    protected_path = job_dir / "protected_payload.bin"
    prefix_buffer = bytearray()
    manifest: dict[str, object] | None = None
    protected_handle = None
    expected_frame_index = 0
    previous_frame_index: int | None = None
    previous_chunk: bytes | None = None

    update(2, "Inspecting uploaded archive.")

    def handle_frame(frame_bytes: bytes, extracted_frames: int) -> None:
        del extracted_frames
        nonlocal expected_frame_index, previous_frame_index, previous_chunk, manifest, protected_handle

        grid = _grid_from_frame_pixels(_frame_array_from_bytes(frame_bytes))
        _validate_reserved_cells(grid)
        frame_index, chunk = _parse_frame_grid(grid)

        if frame_index == expected_frame_index:
            expected_frame_index += 1
            previous_frame_index = frame_index
            previous_chunk = chunk
        elif (
            allow_duplicate_frame_chunks
            and previous_frame_index is not None
            and frame_index == previous_frame_index
            and previous_chunk == chunk
        ):
            return
        elif frame_index < expected_frame_index:
            raise CodecError(f"Duplicate frame index {frame_index} detected.")
        else:
            raise CodecError("Frame sequence is incomplete or out of range.")

        if manifest is None:
            prefix_buffer.extend(chunk)
            parsed_manifest, remainder = _parse_stream_prefix(prefix_buffer)
            if parsed_manifest is None:
                return
            try:
                validate_manifest(parsed_manifest)
            except ValueError as exc:
                raise CodecError(str(exc)) from exc

            manifest = parsed_manifest
            protected_handle = protected_path.open("wb")
            if remainder:
                protected_handle.write(remainder)
            prefix_buffer.clear()
            return

        assert protected_handle is not None
        protected_handle.write(chunk)

    try:
        extraction = stream_video_frames(
            video_path,
            frame_handler=handle_frame,
            progress=lambda progress_value, message: update(progress_value, message),
        )
    except RuntimeError as exc:
        raise CodecError(str(exc)) from exc
    finally:
        if protected_handle is not None:
            protected_handle.close()

    if extraction.warning:
        update(55, f"Extracted {extraction.extracted_frames} frames with a recoverable ffmpeg warning.")
    else:
        update(55, f"Extracted {extraction.extracted_frames} frames.")

    if manifest is None:
        raise CodecError("Stream is too small to contain a manifest header.")

    total_frames = int(manifest["total_frames"])
    if total_frames != expected_frame_index:
        raise CodecError("Frame count does not match the encoded manifest.")

    stored_size = int(manifest["stored_size"])
    actual_stored_size = protected_path.stat().st_size if protected_path.exists() else 0
    if actual_stored_size != stored_size:
        raise CodecError("Stored payload length does not match the manifest.")

    update(96, "Validating decoded payload.")
    manifest_path = job_dir / RECOVERED_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    restored_name = sanitize_filename(str(manifest["original_filename"]), fallback="restored.bin")
    restored_path = output_dir / restored_name
    integrity_ok = _restore_payload_file(
        manifest=manifest,
        protected_path=protected_path,
        restored_path=restored_path,
        key=normalized_key,
    )
    if integrity_ok:
        update(100, "Recovered original file bytes.")
    else:
        update(100, "Recovered file bytes, but integrity validation failed.")

    return DecodeResult(
        manifest=manifest,
        restored_path=restored_path,
        manifest_path=manifest_path,
        extracted_frame_paths=[],
        integrity_ok=integrity_ok,
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

    output_dir = job_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = job_dir / RECOVERED_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    restored_name = sanitize_filename(str(manifest["original_filename"]), fallback="restored.bin")
    restored_path = output_dir / restored_name
    integrity_ok = _restore_payload_bytes(
        manifest=manifest,
        protected_bytes=protected_bytes,
        restored_path=restored_path,
        key=normalized_key,
    )

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


def _encode_source(
    *,
    source_path: Path,
    original_filename: str,
    media_type: str,
    key: str,
    job_dir: Path,
    output_path: Path,
    target: str,
    progress: ProgressCallback | None,
    debug_artifacts: bool,
) -> EncodeResult:
    update = progress or _noop_progress
    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise CodecError(str(exc)) from exc

    stats = _scan_source_file(source_path)
    manifest = build_manifest_for_payload(
        original_filename=original_filename,
        media_type=media_type,
        original_size=stats["size"],
        stored_size=stats["size"],
        sha256=stats["sha256"],
        crc32=stats["crc32"],
        version=FORMAT_VERSION,
    )

    output_dir = job_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = job_dir / MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    update(10, "Prepared manifest and streaming encode.")

    frame_paths: list[Path] = []
    frames_dir = job_dir / "frames" if debug_artifacts else None
    if frames_dir is not None:
        frames_dir.mkdir(parents=True, exist_ok=True)

    total_frames = int(manifest["total_frames"])

    def frame_source():
        generated_frames = 0
        for frame_index, chunk in enumerate(_iter_frame_chunks(source_path=source_path, key=normalized_key, manifest=manifest)):
            frame_pixels = _render_frame_pixels(frame_index=frame_index, chunk=chunk)
            if frames_dir is not None:
                frame_path = frames_dir / (FRAME_PATTERN % (frame_index + 1))
                _save_frame_png(frame_pixels, frame_path)
                frame_paths.append(frame_path)
            generated_frames += 1
            frame_progress = 12 + math.floor((generated_frames / total_frames) * 80)
            update(frame_progress, f"Encoded frame {generated_frames} of {total_frames}.")
            yield frame_pixels.tobytes()

        if generated_frames != total_frames:
            raise CodecError("Frame count does not match the encoded manifest.")

    try:
        if target == "local":
            encode_raw_frames_to_mkv(frame_source(), output_path, fps=int(manifest["fps"]))
        elif target == "youtube":
            encode_raw_frames_to_youtube_mp4(
                frame_source(),
                output_path,
                fps=int(manifest["fps"]),
                frame_count=total_frames,
            )
        else:
            raise CodecError(f"Unsupported encode target: {target}")
    except RuntimeError as exc:
        raise CodecError(str(exc)) from exc

    frames_zip_path: Path | None = None
    if frames_dir is not None:
        frames_zip_path = job_dir / FRAMES_ARCHIVE_NAME
        _archive_frames(frame_paths, job_dir, frames_zip_path)
        update(98, "Archived PNG debug frames.")

    update(100, "Encoded archive video.")
    return EncodeResult(
        manifest=manifest,
        video_path=output_path,
        frames_zip_path=frames_zip_path,
        manifest_path=manifest_path,
        frame_paths=frame_paths,
    )


def _scan_source_file(source_path: Path) -> dict[str, str | int]:
    sha256 = hashlib.sha256()
    crc32 = 0
    total_bytes = 0
    with source_path.open("rb") as source:
        while True:
            chunk = source.read(KEY_CHUNK_BYTES)
            if not chunk:
                break
            total_bytes += len(chunk)
            sha256.update(chunk)
            crc32 = zlib.crc32(chunk, crc32)
    return {
        "size": total_bytes,
        "sha256": sha256.hexdigest(),
        "crc32": f"{crc32 & 0xFFFFFFFF:08x}",
    }


def _iter_frame_chunks(*, source_path: Path, key: str, manifest: dict[str, object]):
    buffer = bytearray(assemble_stream_prefix(manifest))
    chunk_capacity = LAYOUT.chunk_byte_capacity
    key_chunk_bytes = int(manifest.get("key_chunk_bytes", KEY_CHUNK_BYTES))

    with source_path.open("rb") as source:
        for protected_chunk in stream_payload_transform(source, key=key, chunk_size=key_chunk_bytes):
            buffer.extend(protected_chunk)
            while len(buffer) >= chunk_capacity:
                yield bytes(buffer[:chunk_capacity])
                del buffer[:chunk_capacity]

    if buffer:
        yield bytes(buffer)


def _restore_payload_file(
    *,
    manifest: dict[str, object],
    protected_path: Path,
    restored_path: Path,
    key: str,
) -> bool:
    restored_path.parent.mkdir(parents=True, exist_ok=True)
    key_mode = str(manifest["key_mode"])
    original_size = int(manifest["original_size"])

    if key_mode == LEGACY_KEY_MODE:
        protected_bytes = protected_path.read_bytes()
        restored_bytes = unscramble_payload(protected_bytes, key)
        if len(restored_bytes) != original_size:
            raise CodecError("Recovered payload length does not match the manifest.")
        restored_path.write_bytes(restored_bytes)
        return _integrity_matches(manifest=manifest, restored_bytes=restored_bytes)

    restored_size = 0
    sha256 = hashlib.sha256()
    crc32 = 0
    with protected_path.open("rb") as protected_file, restored_path.open("wb") as restored_file:
        for restored_chunk in stream_payload_transform(
            protected_file,
            key=key,
            chunk_size=int(manifest.get("key_chunk_bytes", KEY_CHUNK_BYTES)),
        ):
            restored_size += len(restored_chunk)
            sha256.update(restored_chunk)
            crc32 = zlib.crc32(restored_chunk, crc32)
            restored_file.write(restored_chunk)

    if restored_size != original_size:
        raise CodecError("Recovered payload length does not match the manifest.")
    return (
        f"{crc32 & 0xFFFFFFFF:08x}" == str(manifest["crc32"])
        and sha256.hexdigest() == str(manifest["sha256"])
    )


def _restore_payload_bytes(
    *,
    manifest: dict[str, object],
    protected_bytes: bytes,
    restored_path: Path,
    key: str,
) -> bool:
    restored_path.parent.mkdir(parents=True, exist_ok=True)
    key_mode = str(manifest["key_mode"])
    original_size = int(manifest["original_size"])
    if key_mode == LEGACY_KEY_MODE:
        restored_bytes = unscramble_payload(protected_bytes, key)
    else:
        restored_chunks = []
        with BytesIO(protected_bytes) as source:
            for chunk in stream_payload_transform(
                source,
                key=key,
                chunk_size=int(manifest.get("key_chunk_bytes", KEY_CHUNK_BYTES)),
            ):
                restored_chunks.append(chunk)
        restored_bytes = b"".join(restored_chunks)

    if len(restored_bytes) != original_size:
        raise CodecError("Recovered payload length does not match the manifest.")
    restored_path.write_bytes(restored_bytes)
    return _integrity_matches(manifest=manifest, restored_bytes=restored_bytes)


def _integrity_matches(*, manifest: dict[str, object], restored_bytes: bytes) -> bool:
    crc32 = f"{zlib.crc32(restored_bytes) & 0xFFFFFFFF:08x}"
    sha256 = hashlib.sha256(restored_bytes).hexdigest()
    return crc32 == manifest["crc32"] and sha256 == manifest["sha256"]


def _parse_stream_prefix(prefix_buffer: bytearray) -> tuple[dict[str, object] | None, bytes]:
    if len(prefix_buffer) < STREAM_HEADER_STRUCT.size:
        return None, b""

    (manifest_size,) = STREAM_HEADER_STRUCT.unpack(prefix_buffer[: STREAM_HEADER_STRUCT.size])
    manifest_end = STREAM_HEADER_STRUCT.size + manifest_size
    if len(prefix_buffer) < manifest_end:
        return None, b""

    try:
        manifest = json.loads(prefix_buffer[STREAM_HEADER_STRUCT.size:manifest_end].decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise CodecError("Manifest JSON could not be decoded from the archive stream.") from exc
    return manifest, bytes(prefix_buffer[manifest_end:])


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


def _render_frame_pixels(*, frame_index: int, chunk: bytes) -> np.ndarray:
    if len(chunk) > LAYOUT.chunk_byte_capacity:
        raise CodecError("Chunk exceeds frame payload capacity.")

    header = FRAME_HEADER_STRUCT.pack(FRAME_MAGIC, frame_index, len(chunk), zlib.crc32(chunk) & 0xFFFFFFFF)
    payload_bits = bytes_to_bits(header + chunk)
    if payload_bits.size > LAYOUT.payload_bit_capacity:
        raise CodecError("Packed payload exceeds frame bit capacity.")

    canvas = _BASE_FRAME_PIXELS.copy()
    payload_values = np.repeat((1 - payload_bits) * 255, CELL_SIZE * CELL_SIZE)
    canvas.ravel()[_PAYLOAD_PIXEL_INDEXES[: payload_values.size]] = payload_values
    return canvas


def _save_frame_png(frame_pixels: np.ndarray, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(frame_pixels, mode="L").save(output_path)


def _write_frame_png(grid: np.ndarray, output_path: Path) -> None:
    canvas = np.full((FRAME_HEIGHT, FRAME_WIDTH), 255, dtype=np.uint8)
    grid_pixels = np.where(grid > 0, 0, 255).astype(np.uint8)
    scaled = np.repeat(np.repeat(grid_pixels, CELL_SIZE, axis=0), CELL_SIZE, axis=1)
    canvas[QUIET_MARGIN : QUIET_MARGIN + scaled.shape[0], QUIET_MARGIN : QUIET_MARGIN + scaled.shape[1]] = scaled
    _save_frame_png(canvas, output_path)


def _archive_frames(frame_paths: list[Path], job_dir: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as zip_file:
        for frame_path in frame_paths:
            zip_file.write(frame_path, arcname=frame_path.relative_to(job_dir))


def _read_grid_from_png(frame_path: Path) -> np.ndarray:
    frame = _load_frame_pixels(frame_path)
    return _grid_from_frame_pixels(frame)


def _grid_from_frame_pixels(frame: np.ndarray) -> np.ndarray:
    if frame.shape != (FRAME_HEIGHT, FRAME_WIDTH):
        raise CodecError("Decoded frame dimensions do not match the expected geometry.")

    cropped = frame[QUIET_MARGIN:-QUIET_MARGIN, QUIET_MARGIN:-QUIET_MARGIN]
    cell_means = cropped.reshape(GRID_ROWS, CELL_SIZE, GRID_COLS, CELL_SIZE).mean(axis=(1, 3))
    return (cell_means < 128).astype(np.uint8)


def _frame_array_from_bytes(frame_bytes: bytes) -> np.ndarray:
    if len(frame_bytes) != FRAME_HEIGHT * FRAME_WIDTH:
        raise CodecError("Decoded frame dimensions do not match the expected geometry.")
    return np.frombuffer(frame_bytes, dtype=np.uint8).reshape((FRAME_HEIGHT, FRAME_WIDTH))


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
