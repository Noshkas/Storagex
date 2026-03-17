from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image

from server.codec.constants import CELL_SIZE, FORMAT_VERSION, KEY_CHUNK_BYTES, LEGACY_FORMAT_VERSION, QUIET_MARGIN
from server.codec.format import (
    DENSE_CHUNK_BYTE_CAPACITY,
    LAYOUT,
    assemble_stream,
    bits_to_bytes,
    build_manifest_for_payload,
    build_stream_with_manifest,
    bytes_to_bits,
    chunk_stream,
    validate_manifest,
)
from server.codec.keyed import LEGACY_KEY_MODE, STREAM_KEY_MODE, scramble_payload, stream_payload_transform, unscramble_payload, validate_numeric_key
from server.codec.service import CodecError, _render_frame_grid, _write_frame_png, decode_video, decode_video_from_frames, encode_file
from server.codec.video import encode_frames_to_webm, encode_frames_to_youtube_mp4

VALID_KEY = "012345678901234567890123"
WRONG_KEY = "999999999999999999999999"


def _sample_pdf_bytes() -> bytes:
    return (
        b"%PDF-1.4\n"
        b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n"
        b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n"
        b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] /Contents 4 0 R >>endobj\n"
        b"4 0 obj<< /Length 44 >>stream\nBT /F1 12 Tf 40 120 Td (StorageX) Tj ET\nendstream endobj\n"
        b"xref\n0 5\n0000000000 65535 f \ntrailer<< /Root 1 0 R /Size 5 >>\nstartxref\n256\n%%EOF\n"
    )


def _sample_png_bytes() -> bytes:
    image = Image.new("RGB", (24, 24), (18, 92, 63))
    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def _sample_jpeg_bytes() -> bytes:
    image = Image.new("RGB", (24, 24), (140, 110, 64))
    output = BytesIO()
    image.save(output, format="JPEG")
    return output.getvalue()


def _sample_mp4_bytes() -> bytes:
    return (
        b"\x00\x00\x00\x18ftypmp42"
        b"\x00\x00\x00\x00mp42isom"
        b"\x00\x00\x00\x08free"
        b"\x00\x00\x00\x20mdat"
        + (b"\x01\x23\x45\x67" * 64)
    )


def test_bits_round_trip() -> None:
    payload = b"\x00\xFFhello\x10"
    bits = bytes_to_bits(payload)
    assert bits_to_bytes(bits, len(payload)) == payload


def test_numeric_key_validation_accepts_leading_zeros() -> None:
    assert validate_numeric_key(VALID_KEY) == VALID_KEY
    with pytest.raises(ValueError, match="exactly 24 digits"):
        validate_numeric_key("1234")


def test_scramble_round_trip() -> None:
    payload = b"storagex-keyed-video" * 50
    scrambled = scramble_payload(payload, VALID_KEY)
    assert scrambled != payload
    assert unscramble_payload(scrambled, VALID_KEY) == payload


def test_stream_payload_transform_round_trip() -> None:
    payload = _multi_frame_payload() + b"stream-end"
    transformed = b"".join(stream_payload_transform(BytesIO(payload), key=VALID_KEY))
    assert transformed != payload
    assert b"".join(stream_payload_transform(BytesIO(transformed), key=VALID_KEY)) == payload


def test_different_keys_produce_different_scrambled_payloads() -> None:
    payload = b"storagex-keyed-video" * 50
    assert scramble_payload(payload, VALID_KEY) != scramble_payload(payload, WRONG_KEY)


def test_manifest_total_frames_is_stable() -> None:
    payload = b"storagex" * 500
    manifest, _, chunks = build_stream_with_manifest(
        original_filename="example.txt",
        media_type="text/plain",
        original_bytes=payload,
        stored_bytes=b"".join(stream_payload_transform(BytesIO(payload), key=VALID_KEY)),
        version=FORMAT_VERSION,
    )
    assert manifest["version"] == FORMAT_VERSION
    assert manifest["key_mode"] == STREAM_KEY_MODE
    assert manifest["key_chunk_bytes"] == KEY_CHUNK_BYTES
    assert manifest["chunk_payload_bytes"] == DENSE_CHUNK_BYTE_CAPACITY
    assert manifest["chunk_payload_bytes"] > LAYOUT.chunk_byte_capacity
    assert manifest["total_frames"] == len(chunks)
    assert manifest["compressed"] is False
    assert manifest["keyed"] is True
    assert manifest["key_length"] == 24
    assert VALID_KEY not in json.dumps(manifest, sort_keys=True)


def test_validate_manifest_rejects_wrong_v3_chunk_size() -> None:
    manifest = build_manifest_for_payload(
        original_filename="example.txt",
        media_type="text/plain",
        original_size=128,
        stored_size=128,
        sha256="0" * 64,
        crc32="0" * 8,
    )
    manifest["key_chunk_bytes"] = 2048
    with pytest.raises(ValueError, match="Unexpected keyed chunk size"):
        validate_manifest(manifest)


@pytest.mark.parametrize(
    ("filename", "payload"),
    [
        ("note.txt", b"storagex bit video\n" * 90),
        ("document.pdf", _sample_pdf_bytes()),
        ("preview.png", _sample_png_bytes()),
        ("photo.jpeg", _sample_jpeg_bytes()),
        ("clip.mp4", _sample_mp4_bytes()),
    ],
)
def test_round_trip_supported_types(tmp_path: Path, filename: str, payload: bytes) -> None:
    encode_dir = tmp_path / f"encode-{filename}"
    decode_dir = tmp_path / f"decode-{filename}"
    source_path = tmp_path / filename
    source_path.write_bytes(payload)

    encoded = encode_file(
        source_path=source_path,
        original_filename=filename,
        media_type="application/octet-stream",
        key=VALID_KEY,
        job_dir=encode_dir,
    )
    decoded = decode_video(video_path=encoded.video_path, job_dir=decode_dir, key=VALID_KEY)

    recovered = decoded.restored_path.read_bytes()
    assert encoded.video_path.suffix == ".mkv"
    assert encoded.manifest["version"] == FORMAT_VERSION
    assert encoded.manifest["key_mode"] == STREAM_KEY_MODE
    assert recovered == payload
    assert hashlib.sha256(recovered).hexdigest() == decoded.manifest["sha256"]
    assert decoded.integrity_ok is True


def test_wrong_key_download_stays_same_length_but_fails_integrity(tmp_path: Path) -> None:
    payload = _sample_png_bytes()
    source_path = tmp_path / "sample.png"
    source_path.write_bytes(payload)

    encoded = encode_file(
        source_path=source_path,
        original_filename="sample.png",
        media_type="image/png",
        key=VALID_KEY,
        job_dir=tmp_path / "encode",
    )
    decoded = decode_video(video_path=encoded.video_path, job_dir=tmp_path / "decode", key=WRONG_KEY)

    recovered = decoded.restored_path.read_bytes()
    assert decoded.integrity_ok is False
    assert len(recovered) == len(payload)
    assert recovered != payload


def test_decode_accepts_legacy_v2_webm(tmp_path: Path) -> None:
    payload = _multi_frame_payload() + b"legacy-data"
    source_path = tmp_path / "legacy.txt"
    source_path.write_bytes(payload)

    protected = scramble_payload(payload, VALID_KEY)
    manifest, _, chunks = build_stream_with_manifest(
        original_filename="legacy.txt",
        media_type="text/plain",
        original_bytes=payload,
        stored_bytes=protected,
        version=LEGACY_FORMAT_VERSION,
    )
    assert manifest["key_mode"] == LEGACY_KEY_MODE

    frames_dir = tmp_path / "legacy-frames"
    frames_dir.mkdir()
    for index, chunk in enumerate(chunks):
        frame_path = frames_dir / f"frame_{index + 1:06d}.png"
        _write_frame_png(_render_frame_grid(frame_index=index, chunk=chunk), frame_path)

    video_path = tmp_path / "legacy.webm"
    encode_frames_to_webm(frames_dir, video_path)

    decoded = decode_video(video_path=video_path, job_dir=tmp_path / "decode", key=VALID_KEY)
    assert decoded.integrity_ok is True
    assert decoded.restored_path.read_bytes() == payload


def test_decode_rejects_modified_finder_pattern(tmp_path: Path) -> None:
    frames_dir = _encode_to_frames(tmp_path, b"finder-test" * 400)
    first_frame = sorted(frames_dir.glob("frame_*.png"))[0]

    with Image.open(first_frame) as image:
        canvas = image.convert("L")
        finder_span = CELL_SIZE * 7
        for x in range(QUIET_MARGIN, QUIET_MARGIN + finder_span):
            for y in range(QUIET_MARGIN, QUIET_MARGIN + finder_span):
                canvas.putpixel((x, y), 255)
        canvas.save(first_frame)

    with pytest.raises(CodecError, match="Finder or timing pattern validation failed"):
        decode_video_from_frames(frames_dir=frames_dir, job_dir=tmp_path / "decode", key=VALID_KEY)


def test_decode_rejects_duplicate_frame_index(tmp_path: Path) -> None:
    payload = _multi_frame_payload()
    frames_dir = _encode_to_frames(tmp_path, payload)
    frame_paths = sorted(frames_dir.glob("frame_*.png"))
    assert len(frame_paths) >= 2

    shutil.copyfile(frame_paths[0], frame_paths[1])

    with pytest.raises(CodecError, match="Duplicate frame index"):
        decode_video_from_frames(frames_dir=frames_dir, job_dir=tmp_path / "decode", key=VALID_KEY)


def test_decode_rejects_truncated_frames(tmp_path: Path) -> None:
    payload = _multi_frame_payload()
    frames_dir = _encode_to_frames(tmp_path, payload)
    last_frame = sorted(frames_dir.glob("frame_*.png"))[-1]
    last_frame.unlink()

    with pytest.raises(CodecError, match="Frame count does not match"):
        decode_video_from_frames(frames_dir=frames_dir, job_dir=tmp_path / "decode", key=VALID_KEY)


def test_decode_rejects_bad_frame_crc(tmp_path: Path) -> None:
    frames_dir = _encode_to_frames(tmp_path, b"checksum-test" * 500)
    first_frame = sorted(frames_dir.glob("frame_*.png"))[0]
    payload_row = int(LAYOUT.payload_rows[200])
    payload_col = int(LAYOUT.payload_cols[200])
    x_start = QUIET_MARGIN + (payload_col * CELL_SIZE)
    y_start = QUIET_MARGIN + (payload_row * CELL_SIZE)

    with Image.open(first_frame) as image:
        canvas = image.convert("L")
        current = canvas.getpixel((x_start, y_start))
        replacement = 0 if current > 127 else 255
        for x in range(x_start, x_start + CELL_SIZE):
            for y in range(y_start, y_start + CELL_SIZE):
                canvas.putpixel((x, y), replacement)
        canvas.save(first_frame)

    with pytest.raises(CodecError, match="failed CRC validation"):
        decode_video_from_frames(frames_dir=frames_dir, job_dir=tmp_path / "decode", key=VALID_KEY)


def test_decode_rejects_non_app_webm(tmp_path: Path) -> None:
    frames_dir = tmp_path / "plain-frames"
    frames_dir.mkdir()
    frame_path = frames_dir / "frame_000001.png"
    Image.new("L", (1280, 720), 255).save(frame_path)

    video_path = tmp_path / "plain.webm"
    encode_frames_to_webm(frames_dir, video_path)

    with pytest.raises(CodecError):
        decode_video(video_path=video_path, job_dir=tmp_path / "decode", key=VALID_KEY)


def test_encode_frames_to_youtube_mp4_creates_h264_video_with_min_duration(tmp_path: Path) -> None:
    frames_dir = tmp_path / "frames"
    frames_dir.mkdir()
    Image.new("L", (1280, 720), 255).save(frames_dir / "frame_000001.png")

    output_path = tmp_path / "youtube-upload.mp4"
    encode_frames_to_youtube_mp4(frames_dir, output_path, fps=24, frame_count=1, min_duration_seconds=5)

    probe = subprocess.check_output(
        [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            str(output_path),
        ],
        text=True,
    )
    payload = json.loads(probe)
    codecs = {stream["codec_name"] for stream in payload["streams"]}

    assert output_path.exists()
    assert "h264" in codecs
    assert "aac" in codecs
    assert float(payload["format"]["duration"]) >= 5.0


def test_decode_accepts_identical_duplicate_frames_for_youtube_recovery(tmp_path: Path) -> None:
    payload = b"hello from storagex\n" * 32
    source_path = tmp_path / "sample.txt"
    source_path.write_bytes(payload)

    encoded = encode_file(
        source_path=source_path,
        original_filename="sample.txt",
        media_type="text/plain",
        key=VALID_KEY,
        job_dir=tmp_path / "encode",
        debug_artifacts=True,
    )

    youtube_video_path = tmp_path / "encode" / "output" / "youtube-upload.mp4"
    encode_frames_to_youtube_mp4(
        tmp_path / "encode" / "frames",
        youtube_video_path,
        fps=24,
        frame_count=len(encoded.frame_paths),
        min_duration_seconds=5,
    )

    decoded = decode_video(
        video_path=youtube_video_path,
        job_dir=tmp_path / "decode",
        key=VALID_KEY,
        allow_duplicate_frame_chunks=True,
    )

    assert decoded.integrity_ok is True
    assert decoded.restored_path.read_bytes() == payload


def test_decode_rejects_version_one_video(tmp_path: Path) -> None:
    payload = b"legacy-video" * 400
    protected = b"".join(stream_payload_transform(BytesIO(payload), key=VALID_KEY))
    manifest, _, _ = build_stream_with_manifest(
        original_filename="legacy.txt",
        media_type="text/plain",
        original_bytes=payload,
        stored_bytes=protected,
    )
    manifest["version"] = 1
    legacy_stream = assemble_stream(manifest, protected)
    chunks = chunk_stream(legacy_stream)

    frames_dir = tmp_path / "legacy-frames"
    frames_dir.mkdir()
    for index, chunk in enumerate(chunks):
        frame_path = frames_dir / f"frame_{index + 1:06d}.png"
        _write_frame_png(_render_frame_grid(frame_index=index, chunk=chunk), frame_path)

    video_path = tmp_path / "legacy.webm"
    encode_frames_to_webm(frames_dir, video_path)

    with pytest.raises(CodecError, match="Unsupported bit video version"):
        decode_video(video_path=video_path, job_dir=tmp_path / "decode", key=VALID_KEY)


def test_decode_rejects_unreadable_extracted_frame(tmp_path: Path) -> None:
    frames_dir = _encode_to_frames(tmp_path, _multi_frame_payload())
    frame_paths = sorted(frames_dir.glob("frame_*.png"))
    assert frame_paths

    frame_paths[0].write_bytes(b"not-a-real-png")

    with pytest.raises(CodecError, match="not a readable PNG image"):
        decode_video_from_frames(frames_dir=frames_dir, job_dir=tmp_path / "decode", key=VALID_KEY)


def test_decode_ignores_unreadable_trailing_frame(tmp_path: Path) -> None:
    payload = _multi_frame_payload()
    frames_dir = _encode_to_frames(tmp_path, payload)
    frame_paths = sorted(frames_dir.glob("frame_*.png"))
    assert len(frame_paths) >= 2

    frame_paths[-1].write_bytes(b"not-a-real-png")

    with pytest.raises(CodecError, match="Frame count does not match"):
        decode_video_from_frames(frames_dir=frames_dir, job_dir=tmp_path / "decode", key=VALID_KEY)


def _encode_to_frames(tmp_path: Path, payload: bytes) -> Path:
    source_path = tmp_path / "sample.txt"
    source_path.write_bytes(payload)
    job_dir = tmp_path / f"job-{hashlib.sha256(payload).hexdigest()[:8]}"
    encode_file(
        source_path=source_path,
        original_filename="sample.txt",
        media_type="text/plain",
        key=VALID_KEY,
        job_dir=job_dir,
        debug_artifacts=True,
    )
    return job_dir / "frames"


def _multi_frame_payload() -> bytes:
    size = LAYOUT.chunk_byte_capacity * 2
    chunks: list[bytes] = []
    seed = b"storagex-seed"
    while sum(len(chunk) for chunk in chunks) < size:
        seed = hashlib.sha256(seed).digest()
        chunks.append(seed)
    return b"".join(chunks)[:size]
