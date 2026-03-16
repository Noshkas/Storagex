from __future__ import annotations

from pathlib import Path

import pytest

import server.app as server_app
from server.app import JobStore


@pytest.fixture
def isolated_jobs_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    jobs_dir = tmp_path / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(server_app, "JOBS_DIR", jobs_dir)
    monkeypatch.setattr(server_app, "jobs", JobStore())
    return jobs_dir
