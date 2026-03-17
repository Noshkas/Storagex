from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

from .codec.constants import DATA_DIR

APP_SETTINGS_PATH = DATA_DIR / "app-settings.json"
SETTINGS_VERSION = 1


class AppSettingsError(ValueError):
    pass


class AppSettingsStore:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or APP_SETTINGS_PATH
        self._lock = threading.Lock()

    def snapshot(self) -> dict[str, str | bool]:
        with self._lock:
            state = self._load_state()
            public_app_url = str(state.get("public_app_url") or "")
            return {
                "configured": bool(public_app_url),
                "public_app_url": public_app_url,
            }

    def update(self, *, public_app_url: str | None) -> dict[str, str | bool]:
        normalized_public_app_url = self._normalize_public_app_url(public_app_url)
        with self._lock:
            state = self._load_state()
            state["public_app_url"] = normalized_public_app_url
            self._persist_state(state)
        return self.snapshot()

    def _load_state(self) -> dict[str, str | int]:
        if not self._path.exists():
            return self._default_state()

        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            state = self._default_state()
            self._persist_state(state)
            return state

        if not isinstance(payload, dict) or payload.get("version") != SETTINGS_VERSION:
            state = self._default_state()
            self._persist_state(state)
            return state

        public_app_url = payload.get("public_app_url")
        if public_app_url is None:
            normalized_public_app_url = ""
        else:
            normalized_public_app_url = self._normalize_public_app_url(public_app_url)

        return {
            "version": SETTINGS_VERSION,
            "public_app_url": normalized_public_app_url,
        }

    def _persist_state(self, state: dict[str, str | int]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self._path)
        try:
            os.chmod(self._path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _default_state() -> dict[str, str | int]:
        return {
            "version": SETTINGS_VERSION,
            "public_app_url": "",
        }

    @staticmethod
    def _normalize_public_app_url(value: str | None) -> str:
        candidate = str(value or "").strip()
        if not candidate:
            return ""

        parsed = urlsplit(candidate)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise AppSettingsError("Public App URL must be a valid http or https URL.")
        if parsed.path not in {"", "/"}:
            raise AppSettingsError("Public App URL must be a root URL without a path.")

        normalized = SplitResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path="",
            query="",
            fragment="",
        )
        return urlunsplit(normalized)
