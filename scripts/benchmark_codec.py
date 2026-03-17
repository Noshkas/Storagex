from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.codec.constants import LEGACY_FORMAT_VERSION
from server.codec.format import build_stream_with_manifest
from server.codec.keyed import scramble_payload
from server.codec.service import _render_frame_grid, _write_frame_png, decode_video, encode_file
from server.codec.video import encode_frames_to_webm

VALID_KEY = "012345678901234567890123"


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark StorageX local encode/decode performance.")
    parser.add_argument(
        "--sizes",
        default="1,10,100",
        help="Comma-separated payload sizes in MiB. Default: 1,10,100",
    )
    parser.add_argument(
        "--modes",
        default="current,legacy",
        help="Comma-separated benchmark modes: current, legacy. Default: current,legacy",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of a markdown table.",
    )
    args = parser.parse_args()

    sizes = [int(item.strip()) for item in args.sizes.split(",") if item.strip()]
    requested_modes = [item.strip() for item in args.modes.split(",") if item.strip()]
    rows: list[dict[str, object]] = []

    for mode in requested_modes:
        for size_mb in sizes:
            rows.append(_benchmark_mode(mode=mode, size_mb=size_mb))

    if args.json:
        print(json.dumps(rows, indent=2))
        return

    print("| Mode | Size (MiB) | Encode (s) | Decode (s) | Archive | Archive Size (MiB) | Integrity |")
    print("| --- | ---: | ---: | ---: | --- | ---: | --- |")
    for row in rows:
        print(
            f"| {row['mode']} | {row['size_mb']} | {row['encode_seconds']:.3f} | {row['decode_seconds']:.3f} | "
            f"{row['archive_suffix']} | {row['archive_size_mb']:.2f} | {row['integrity_ok']} |"
        )


def _benchmark_mode(*, mode: str, size_mb: int) -> dict[str, object]:
    payload = _build_payload(size_mb)
    with tempfile.TemporaryDirectory(prefix=f"storagex-bench-{mode}-{size_mb}-") as tmp_dir:
        root = Path(tmp_dir)
        source_path = root / f"sample-{size_mb}mb.bin"
        source_path.write_bytes(payload)

        if mode == "current":
            start = time.perf_counter()
            encoded = encode_file(
                source_path=source_path,
                original_filename=source_path.name,
                media_type="application/octet-stream",
                key=VALID_KEY,
                job_dir=root / "encode",
            )
            encode_seconds = time.perf_counter() - start

            start = time.perf_counter()
            decoded = decode_video(video_path=encoded.video_path, job_dir=root / "decode", key=VALID_KEY)
            decode_seconds = time.perf_counter() - start
        elif mode == "legacy":
            start = time.perf_counter()
            encoded_path = _encode_legacy_archive(source_path=source_path, job_dir=root / "encode")
            encode_seconds = time.perf_counter() - start

            start = time.perf_counter()
            decoded = decode_video(video_path=encoded_path, job_dir=root / "decode", key=VALID_KEY)
            decode_seconds = time.perf_counter() - start
            encoded = type("LegacyEncoded", (), {"video_path": encoded_path})
        else:
            raise ValueError(f"Unsupported benchmark mode: {mode}")

        return {
            "mode": mode,
            "size_mb": size_mb,
            "encode_seconds": encode_seconds,
            "decode_seconds": decode_seconds,
            "archive_suffix": encoded.video_path.suffix,
            "archive_size_mb": encoded.video_path.stat().st_size / (1024 * 1024),
            "integrity_ok": decoded.integrity_ok,
        }


def _build_payload(size_mb: int) -> bytes:
    target_size = size_mb * 1024 * 1024
    chunks: list[bytes] = []
    seed = b"storagex-benchmark-seed"
    while sum(len(chunk) for chunk in chunks) < target_size:
        seed = hashlib.sha256(seed).digest()
        chunks.append(seed * 1024)
    return b"".join(chunks)[:target_size]


def _encode_legacy_archive(*, source_path: Path, job_dir: Path) -> Path:
    payload = source_path.read_bytes()
    protected = scramble_payload(payload, VALID_KEY)
    manifest, _, chunks = build_stream_with_manifest(
        original_filename=source_path.name,
        media_type="application/octet-stream",
        original_bytes=payload,
        stored_bytes=protected,
        version=LEGACY_FORMAT_VERSION,
    )

    frames_dir = job_dir / "frames"
    output_dir = job_dir / "output"
    frames_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    for index, chunk in enumerate(chunks):
        frame_path = frames_dir / f"frame_{index + 1:06d}.png"
        _write_frame_png(_render_frame_grid(frame_index=index, chunk=chunk), frame_path)

    video_path = output_dir / "video.webm"
    encode_frames_to_webm(frames_dir, video_path)
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    shutil.rmtree(frames_dir, ignore_errors=True)
    return video_path


if __name__ == "__main__":
    main()
