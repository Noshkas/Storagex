from __future__ import annotations

import mimetypes
import re
from pathlib import Path

SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


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
