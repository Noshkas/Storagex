from __future__ import annotations

import hashlib
import json
import math
import mimetypes
import re
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .constants import (
    APP_MAGIC,
    CELL_SIZE,
    FINDER_SIZE,
    FORMAT_VERSION,
    FPS,
    FRAME_HEIGHT,
    FRAME_MAGIC,
    FRAME_WIDTH,
    GRID_COLS,
    GRID_ROWS,
    KEY_CHUNK_BYTES,
    LEGACY_FORMAT_VERSION,
    QUIET_MARGIN,
    TIMING_INDEX,
)
from .keyed import KEY_LENGTH, LEGACY_KEY_MODE, STREAM_KEY_MODE

FRAME_HEADER_STRUCT = struct.Struct("<4sIII")
STREAM_HEADER_STRUCT = struct.Struct("<I")
SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True, slots=True)
class FrameLayout:
    base_grid: np.ndarray
    reserved_mask: np.ndarray
    payload_rows: np.ndarray
    payload_cols: np.ndarray
    payload_bit_capacity: int
    payload_byte_capacity: int
    chunk_byte_capacity: int


def _finder_pattern() -> np.ndarray:
    return np.array(
        [
            [1, 1, 1, 1, 1, 1, 1],
            [1, 0, 0, 0, 0, 0, 1],
            [1, 0, 1, 1, 1, 0, 1],
            [1, 0, 1, 1, 1, 0, 1],
            [1, 0, 1, 1, 1, 0, 1],
            [1, 0, 0, 0, 0, 0, 1],
            [1, 1, 1, 1, 1, 1, 1],
        ],
        dtype=np.uint8,
    )


def build_layout() -> FrameLayout:
    base_grid = np.zeros((GRID_ROWS, GRID_COLS), dtype=np.uint8)
    reserved_mask = np.zeros_like(base_grid, dtype=bool)
    finder = _finder_pattern()

    corners = (
        (0, 0),
        (0, GRID_COLS - FINDER_SIZE),
        (GRID_ROWS - FINDER_SIZE, 0),
        (GRID_ROWS - FINDER_SIZE, GRID_COLS - FINDER_SIZE),
    )

    for row_start, col_start in corners:
        row_end = row_start + FINDER_SIZE
        col_end = col_start + FINDER_SIZE
        base_grid[row_start:row_end, col_start:col_end] = finder
        reserved_mask[row_start:row_end, col_start:col_end] = True

    for col_index in range(GRID_COLS):
        base_grid[TIMING_INDEX, col_index] = col_index % 2 == 0
        reserved_mask[TIMING_INDEX, col_index] = True

    for row_index in range(GRID_ROWS):
        base_grid[row_index, TIMING_INDEX] = row_index % 2 == 0
        reserved_mask[row_index, TIMING_INDEX] = True

    payload_rows, payload_cols = np.nonzero(~reserved_mask)
    payload_bit_capacity = int(payload_rows.size)
    payload_byte_capacity = payload_bit_capacity // 8
    chunk_byte_capacity = payload_byte_capacity - FRAME_HEADER_STRUCT.size

    return FrameLayout(
        base_grid=base_grid,
        reserved_mask=reserved_mask,
        payload_rows=payload_rows.astype(np.int32),
        payload_cols=payload_cols.astype(np.int32),
        payload_bit_capacity=payload_bit_capacity,
        payload_byte_capacity=payload_byte_capacity,
        chunk_byte_capacity=chunk_byte_capacity,
    )


LAYOUT = build_layout()


def bytes_to_bits(data: bytes) -> np.ndarray:
    if not data:
        return np.zeros(0, dtype=np.uint8)
    return np.unpackbits(np.frombuffer(data, dtype=np.uint8), bitorder="big")


def bits_to_bytes(bits: np.ndarray, byte_length: int | None = None) -> bytes:
    bit_array = np.asarray(bits, dtype=np.uint8)
    if byte_length is None:
        byte_length = (bit_array.size + 7) // 8

    padded_bits = byte_length * 8
    if bit_array.size < padded_bits:
        bit_array = np.pad(bit_array, (0, padded_bits - bit_array.size), constant_values=0)

    packed = np.packbits(bit_array[:padded_bits], bitorder="big")
    return packed.tobytes()[:byte_length]


def sanitize_filename(filename: str | None, fallback: str = "upload.bin") -> str:
    if not filename:
        return fallback

    cleaned = SAFE_FILENAME_PATTERN.sub("_", Path(filename).name.strip())
    cleaned = cleaned.strip("._")
    if not cleaned:
        return fallback
    return cleaned[:255]


def guess_media_type(filename: str, fallback: str = "application/octet-stream") -> str:
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or fallback


def build_manifest_from_stats(
    *,
    original_filename: str,
    media_type: str,
    original_size: int,
    stored_size: int,
    total_frames: int,
    sha256: str,
    crc32: str,
    version: int = FORMAT_VERSION,
) -> dict[str, int | str | bool]:
    manifest: dict[str, int | str | bool] = {
        "magic": APP_MAGIC,
        "version": version,
        "original_filename": sanitize_filename(original_filename),
        "media_type": media_type or guess_media_type(original_filename),
        "original_size": original_size,
        "stored_size": stored_size,
        "compressed": False,
        "keyed": True,
        "key_length": KEY_LENGTH,
        "protected_scope": "payload",
        "sha256": sha256,
        "crc32": crc32,
        "frame_width": FRAME_WIDTH,
        "frame_height": FRAME_HEIGHT,
        "cell_size": CELL_SIZE,
        "quiet_margin": QUIET_MARGIN,
        "grid_cols": GRID_COLS,
        "grid_rows": GRID_ROWS,
        "fps": FPS,
        "finder_size": FINDER_SIZE,
        "timing_index": TIMING_INDEX,
        "chunk_payload_bytes": LAYOUT.chunk_byte_capacity,
        "total_frames": total_frames,
    }
    if version == LEGACY_FORMAT_VERSION:
        manifest["key_mode"] = LEGACY_KEY_MODE
    elif version == FORMAT_VERSION:
        manifest["key_mode"] = STREAM_KEY_MODE
        manifest["key_chunk_bytes"] = KEY_CHUNK_BYTES
    else:
        manifest["key_mode"] = STREAM_KEY_MODE
    return manifest


def build_manifest(
    *,
    original_filename: str,
    media_type: str,
    original_bytes: bytes,
    stored_bytes: bytes,
    total_frames: int,
    version: int = FORMAT_VERSION,
) -> dict[str, int | str | bool]:
    return build_manifest_from_stats(
        original_filename=original_filename,
        media_type=media_type,
        original_size=len(original_bytes),
        stored_size=len(stored_bytes),
        total_frames=total_frames,
        sha256=hashlib.sha256(original_bytes).hexdigest(),
        crc32=f"{zlib.crc32(original_bytes) & 0xFFFFFFFF:08x}",
        version=version,
    )


def serialize_manifest(manifest: dict[str, object]) -> bytes:
    return json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")


def assemble_stream_prefix(manifest: dict[str, object]) -> bytes:
    manifest_bytes = serialize_manifest(manifest)
    return STREAM_HEADER_STRUCT.pack(len(manifest_bytes)) + manifest_bytes


def assemble_stream(manifest: dict[str, object], stored_bytes: bytes) -> bytes:
    return assemble_stream_prefix(manifest) + stored_bytes


def parse_stream(stream_bytes: bytes) -> tuple[dict[str, object], bytes]:
    if len(stream_bytes) < STREAM_HEADER_STRUCT.size:
        raise ValueError("Stream is too small to contain a manifest header.")

    (manifest_size,) = STREAM_HEADER_STRUCT.unpack(stream_bytes[: STREAM_HEADER_STRUCT.size])
    manifest_start = STREAM_HEADER_STRUCT.size
    manifest_end = manifest_start + manifest_size
    if manifest_end > len(stream_bytes):
        raise ValueError("Manifest length exceeds stream size.")

    manifest = json.loads(stream_bytes[manifest_start:manifest_end].decode("utf-8"))
    payload_bytes = stream_bytes[manifest_end:]
    return manifest, payload_bytes


def stream_prefix_length(manifest: dict[str, object]) -> int:
    return STREAM_HEADER_STRUCT.size + len(serialize_manifest(manifest))


def total_stream_size(*, manifest: dict[str, object], stored_size: int) -> int:
    return stream_prefix_length(manifest) + stored_size


def chunk_stream(stream_bytes: bytes) -> list[bytes]:
    if not stream_bytes:
        return [b""]

    return [
        stream_bytes[offset : offset + LAYOUT.chunk_byte_capacity]
        for offset in range(0, len(stream_bytes), LAYOUT.chunk_byte_capacity)
    ]


def build_manifest_for_payload(
    *,
    original_filename: str,
    media_type: str,
    original_size: int,
    stored_size: int,
    sha256: str,
    crc32: str,
    version: int = FORMAT_VERSION,
) -> dict[str, object]:
    total_frames = 0

    while True:
        manifest = build_manifest_from_stats(
            original_filename=original_filename,
            media_type=media_type,
            original_size=original_size,
            stored_size=stored_size,
            total_frames=total_frames,
            sha256=sha256,
            crc32=crc32,
            version=version,
        )
        required_frames = max(1, math.ceil(total_stream_size(manifest=manifest, stored_size=stored_size) / LAYOUT.chunk_byte_capacity))
        if required_frames == total_frames:
            return manifest
        total_frames = required_frames


def build_stream_with_manifest(
    *,
    original_filename: str,
    media_type: str,
    original_bytes: bytes,
    stored_bytes: bytes,
    version: int = FORMAT_VERSION,
) -> tuple[dict[str, object], bytes, list[bytes]]:
    manifest = build_manifest_for_payload(
        original_filename=original_filename,
        media_type=media_type,
        original_size=len(original_bytes),
        stored_size=len(stored_bytes),
        sha256=hashlib.sha256(original_bytes).hexdigest(),
        crc32=f"{zlib.crc32(original_bytes) & 0xFFFFFFFF:08x}",
        version=version,
    )
    stream_bytes = assemble_stream(manifest, stored_bytes)
    return manifest, stream_bytes, chunk_stream(stream_bytes)


def validate_manifest(manifest: dict[str, object]) -> None:
    if manifest.get("magic") != APP_MAGIC:
        raise ValueError("Video is not an app-generated bit video.")

    version = manifest.get("version")
    if version not in {LEGACY_FORMAT_VERSION, FORMAT_VERSION}:
        raise ValueError("Unsupported bit video version.")
    if manifest.get("keyed") is not True:
        raise ValueError("Video is not using the keyed format.")
    if manifest.get("key_length") != KEY_LENGTH:
        raise ValueError("Unexpected key length in manifest.")
    if manifest.get("protected_scope") != "payload":
        raise ValueError("Unexpected protection scope in manifest.")
    if manifest.get("compressed") is not False:
        raise ValueError("Unexpected compression mode in manifest.")
    if manifest.get("frame_width") != FRAME_WIDTH or manifest.get("frame_height") != FRAME_HEIGHT:
        raise ValueError("Unexpected frame dimensions in manifest.")
    if manifest.get("cell_size") != CELL_SIZE or manifest.get("quiet_margin") != QUIET_MARGIN:
        raise ValueError("Unexpected frame geometry in manifest.")
    if manifest.get("grid_cols") != GRID_COLS or manifest.get("grid_rows") != GRID_ROWS:
        raise ValueError("Unexpected grid geometry in manifest.")
    if manifest.get("finder_size") != FINDER_SIZE or manifest.get("timing_index") != TIMING_INDEX:
        raise ValueError("Unexpected finder pattern geometry in manifest.")
    if manifest.get("chunk_payload_bytes") != LAYOUT.chunk_byte_capacity:
        raise ValueError("Unexpected payload chunk capacity in manifest.")
    if manifest.get("fps") != FPS:
        raise ValueError("Unexpected frame rate in manifest.")

    if version == LEGACY_FORMAT_VERSION:
        if manifest.get("key_mode") != LEGACY_KEY_MODE:
            raise ValueError("Unexpected key mode in manifest.")
        if "key_chunk_bytes" in manifest:
            raise ValueError("Unexpected chunked key metadata in legacy manifest.")
        return

    if manifest.get("key_mode") != STREAM_KEY_MODE:
        raise ValueError("Unexpected key mode in manifest.")
    if manifest.get("key_chunk_bytes") != KEY_CHUNK_BYTES:
        raise ValueError("Unexpected keyed chunk size in manifest.")
