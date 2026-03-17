from __future__ import annotations

from pathlib import Path

import pytest

import server.app as server_app
from server.app import JobStore
from server.app_settings import AppSettingsStore
from server.library_index import LibraryIndexStore
from server.share_store import ShareStore


@pytest.fixture
def isolated_jobs_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    data_dir = tmp_path / "data"
    jobs_dir = tmp_path / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(server_app, "JOBS_DIR", jobs_dir)
    monkeypatch.setattr(server_app, "DATA_DIR", data_dir)
    monkeypatch.setattr(server_app, "SHARE_ARTIFACTS_DIR", data_dir / "share-artifacts")
    monkeypatch.setattr(server_app, "jobs", JobStore())
    monkeypatch.setattr(server_app, "library_index", LibraryIndexStore(tmp_path / "library-index.json"))
    monkeypatch.setattr(server_app, "app_settings", AppSettingsStore(data_dir / "app-settings.json"))
    monkeypatch.setattr(server_app, "share_store", ShareStore(data_dir / "shares.json"))
    return jobs_dir
