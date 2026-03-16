from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

import server.app as server_app
from server.youtube import (
    YouTubeAuthError,
    YouTubeConfigurationError,
    YouTubeFileRecord,
    YouTubeSessionStatus,
)

VALID_KEY = "012345678901234567890123"
WRONG_KEY = "999999999999999999999999"


class FakeYouTubeService:
    def __init__(self, *, configured: bool = True, connected: bool = True) -> None:
        self.configured = configured
        self.connected = connected
        self.files: list[YouTubeFileRecord] = []
        self.client_id = "client-id" if configured else ""
        self.has_client_secret = configured
        self._remote_video_payloads: dict[str, tuple[bytes, str]] = {}

    def session_status(self) -> YouTubeSessionStatus:
        return YouTubeSessionStatus(
            configured=self.configured,
            connected=self.connected,
            channel_title="Test Channel" if self.connected else None,
            privacy_status="private",
        )

    def settings_snapshot(self) -> dict:
        return {
            "configured": self.configured,
            "client_id": self.client_id,
            "has_client_secret": self.has_client_secret,
            "source": "runtime" if self.configured else "none",
        }

    def set_runtime_client_config(self, *, client_id: str, client_secret: str) -> dict:
        self.client_id = client_id
        self.has_client_secret = bool(client_secret)
        self.configured = bool(client_id and client_secret)
        self.connected = False
        return self.settings_snapshot()

    def disconnect(self) -> None:
        self.connected = False

    def reset_local_state(self) -> dict:
        self.connected = False
        self.configured = False
        self.client_id = ""
        self.has_client_secret = False
        return self.settings_snapshot()

    def authorization_url(self, redirect_uri: str) -> str:
        del redirect_uri
        if not self.configured or not self.has_client_secret:
            raise YouTubeConfigurationError("Save a YouTube client ID and client secret first.")
        return "https://accounts.google.com/o/oauth2/auth?state=test-state"

    def list_files(self) -> list[YouTubeFileRecord]:
        return list(self.files)

    def get_file(self, video_id: str) -> YouTubeFileRecord:
        for item in self.files:
            if item.video_id == video_id:
                return item
        raise AssertionError(f"missing fake youtube file {video_id}")

    def upload_video(self, *, video_path: Path, manifest: dict, progress=None) -> YouTubeFileRecord:
        assert video_path.exists()
        if progress is not None:
            progress(85, "Uploading to YouTube (28%).")
            progress(99, "Uploading to YouTube (100%).")

        video_id = f"video-{len(self.files) + 1}"
        record = YouTubeFileRecord(
            video_id=video_id,
            original_filename=str(manifest["original_filename"]),
            media_type=str(manifest["media_type"]),
            original_size=int(manifest["original_size"]),
            stored_size=int(manifest["stored_size"]),
            sha256=str(manifest["sha256"]),
            crc32=str(manifest["crc32"]),
            frame_count=int(manifest["total_frames"]),
            fps=int(manifest["fps"]),
            uploaded_at="2026-03-16T10:00:00Z",
            privacy_status="private",
            watch_url=f"https://www.youtube.com/watch?v={video_id}",
            studio_url=f"https://studio.youtube.com/video/{video_id}/edit",
            thumbnail_url=None,
            youtube_title=f"StorageX · {manifest['original_filename']}",
        )
        self.files.insert(0, record)
        self._remote_video_payloads[video_id] = (video_path.read_bytes(), video_path.suffix or ".bin")
        return record

    def download_video(self, *, video_id: str, output_dir: Path) -> Path:
        payload, suffix = self._remote_video_payloads[video_id]
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{video_id}{suffix}"
        output_path.write_bytes(payload)
        return output_path


class FakeFailingAuthYouTubeService:
    def complete_authorization(self, *, state: str, code: str, redirect_uri: str) -> None:
        del state, code, redirect_uri
        raise YouTubeAuthError("Google rejected the client secret.")


class FakeCredentials:
    def to_json(self) -> str:
        return json.dumps(
            {
                "token": "token",
                "refresh_token": "refresh",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "scopes": ["https://www.googleapis.com/auth/youtube.upload"],
            }
        )


class FakeFlow:
    instances: list["FakeFlow"] = []

    def __init__(self, *, state=None, redirect_uri=None, code_verifier=None, **kwargs) -> None:
        del kwargs
        self.state = state
        self.redirect_uri = redirect_uri
        self.code_verifier = code_verifier
        self.credentials = FakeCredentials()
        self.fetch_kwargs: dict | None = None
        FakeFlow.instances.append(self)

    @classmethod
    def from_client_config(cls, client_config, scopes, **kwargs):
        del client_config, scopes
        return cls(**kwargs)

    def authorization_url(self, **kwargs):
        del kwargs
        self.code_verifier = "pkce-verifier"
        return "https://accounts.google.com/o/oauth2/auth?state=test-state", "test-state"

    def fetch_token(self, **kwargs):
        self.fetch_kwargs = kwargs
        return {"access_token": "token"}


def test_api_encode_then_decode_flow(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        response = client.post(
            "/api/encode",
            files={"file": ("hello.txt", b"hello from storagex\n" * 100, "text/plain")},
            data={"key": VALID_KEY},
        )
        assert response.status_code == 200
        encode_job = _wait_for_completion(client, response.json()["job_id"])
        assert encode_job["status"] == "completed"
        assert encode_job["artifacts"]["video"].endswith(".webm")

        video_response = client.get(encode_job["artifacts"]["video"])
        assert video_response.status_code == 200
        assert video_response.content

        decode_response = client.post(
            "/api/decode",
            files={"file": ("encoded.webm", video_response.content, "video/webm")},
            data={"key": VALID_KEY},
        )
        assert decode_response.status_code == 200
        decode_job = _wait_for_completion(client, decode_response.json()["job_id"])
        assert decode_job["status"] == "completed"
        assert decode_job["metadata"]["integrity_ok"] is True

        recovered_response = client.get(decode_job["artifacts"]["recovered_file"])
        assert recovered_response.status_code == 200
        assert recovered_response.content == b"hello from storagex\n" * 100


def test_api_decode_with_wrong_key_downloads_corrupted_file(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        source_bytes = b"hello from storagex\n" * 100
        encode_response = client.post(
            "/api/encode",
            files={"file": ("hello.txt", source_bytes, "text/plain")},
            data={"key": VALID_KEY},
        )
        assert encode_response.status_code == 200
        encode_job = _wait_for_completion(client, encode_response.json()["job_id"])

        video_response = client.get(encode_job["artifacts"]["video"])
        assert video_response.status_code == 200

        decode_response = client.post(
            "/api/decode",
            files={"file": ("encoded.webm", video_response.content, "video/webm")},
            data={"key": WRONG_KEY},
        )
        assert decode_response.status_code == 200
        decode_job = _wait_for_completion(client, decode_response.json()["job_id"])
        assert decode_job["status"] == "completed"
        assert decode_job["metadata"]["integrity_ok"] is False

        recovered_response = client.get(decode_job["artifacts"]["recovered_file"])
        assert recovered_response.status_code == 200
        assert len(recovered_response.content) == len(source_bytes)
        assert recovered_response.content != source_bytes


def test_api_rejects_unsupported_upload_type(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        response = client.post(
            "/api/encode",
            files={"file": ("notes.docx", b"not supported", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")},
            data={"key": VALID_KEY},
        )
        assert response.status_code == 400
        assert response.json()["detail"] == "Unsupported file type."


def test_api_rejects_oversize_upload(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        response = client.post(
            "/api/encode",
            files={"file": ("big.txt", b"x" * ((10 * 1024 * 1024) + 1), "text/plain")},
            data={"key": VALID_KEY},
        )
        assert response.status_code == 413
        assert response.json()["detail"] == "File exceeds the 10 MB limit."


def test_api_rejects_invalid_key(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        response = client.post(
            "/api/encode",
            files={"file": ("hello.txt", b"hello", "text/plain")},
            data={"key": "1234"},
        )
        assert response.status_code == 400
        assert response.json()["detail"] == "Key must be exactly 24 digits."


def test_library_lists_youtube_backed_files(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    fake_youtube.files.append(
        YouTubeFileRecord(
            video_id="video-1",
            original_filename="hello.txt",
            media_type="text/plain",
            original_size=1024,
            stored_size=1024,
            sha256="abc123",
            crc32="deadbeef",
            frame_count=2,
            fps=24,
            uploaded_at="2026-03-16T10:00:00Z",
            privacy_status="private",
            watch_url="https://www.youtube.com/watch?v=video-1",
            studio_url="https://studio.youtube.com/video/video-1/edit",
            thumbnail_url=None,
            youtube_title="StorageX · hello.txt",
        )
    )
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        response = client.get("/api/library")
        assert response.status_code == 200
        payload = response.json()
        assert payload["connected"] is True
        assert payload["channel_title"] == "Test Channel"
        assert len(payload["files"]) == 1
        assert payload["files"][0]["original_filename"] == "hello.txt"


def test_settings_can_be_saved_at_runtime(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService(configured=False, connected=False)
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        response = client.get("/api/settings/youtube")
        assert response.status_code == 200
        assert response.json()["configured"] is False

        save_response = client.post(
            "/api/settings/youtube",
            json={"client_id": "new-client", "client_secret": "new-secret"},
        )
        assert save_response.status_code == 200
        payload = save_response.json()
        assert payload["configured"] is True
        assert payload["client_id"] == "new-client"

        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 8, "text/plain")},
            data={"key": VALID_KEY},
        )
        assert upload_response.status_code == 409
        assert upload_response.json()["detail"] == "Connect YouTube before uploading files."


def test_local_youtube_reset_clears_saved_runtime_state(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService(configured=True, connected=True)
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        response = client.post("/api/auth/reset")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "reset"
    assert payload["settings"]["configured"] is False
    assert fake_youtube.configured is False
    assert fake_youtube.connected is False


def test_youtube_callback_redirects_with_reason(isolated_jobs_dir, monkeypatch) -> None:
    monkeypatch.setattr(server_app, "youtube_service", FakeFailingAuthYouTubeService())

    with TestClient(server_app.app) as client:
        response = client.get(
            "/auth/youtube/callback",
            params={"state": "abc", "code": "123"},
            follow_redirects=False,
        )

    assert response.status_code == 302
    location = response.headers["location"]
    parsed = urlparse(location)
    query = parse_qs(parsed.query)
    assert parsed.path == "/"
    assert query["youtube"] == ["error"]
    assert query["reason"] == ["Google rejected the client secret."]


def test_youtube_connect_redirects_back_to_app_when_not_configured(isolated_jobs_dir, monkeypatch) -> None:
    monkeypatch.setattr(server_app, "youtube_service", FakeYouTubeService(configured=False, connected=False))

    with TestClient(server_app.app) as client:
        response = client.get("/auth/youtube/start", follow_redirects=False)

    assert response.status_code == 302
    location = response.headers["location"]
    parsed = urlparse(location)
    query = parse_qs(parsed.query)
    assert parsed.path == "/"
    assert query["youtube"] == ["error"]
    assert query["reason"] == ["Save a YouTube client ID and client secret first."]


def test_youtube_authorization_reuses_pkce_verifier(isolated_jobs_dir, monkeypatch) -> None:
    from server.youtube import YouTubeService

    FakeFlow.instances.clear()
    service = YouTubeService()
    service.set_runtime_client_config(client_id="client-id", client_secret="client-secret")
    monkeypatch.setattr(service, "_import_flow", lambda: FakeFlow)

    redirect_uri = "http://127.0.0.1:8000/auth/youtube/callback"
    authorization_url = service.authorization_url(redirect_uri)
    assert authorization_url.startswith("https://accounts.google.com/o/oauth2/auth")

    service.complete_authorization(state="test-state", code="code-123", redirect_uri=redirect_uri)

    assert len(FakeFlow.instances) == 2
    assert FakeFlow.instances[1].code_verifier == "pkce-verifier"
    assert FakeFlow.instances[1].fetch_kwargs == {"code": "code-123"}


def test_api_uploads_to_youtube_and_cleans_temp_files(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 32, "text/plain")},
            data={"key": VALID_KEY},
        )
        assert response.status_code == 200

        upload_job = _wait_for_completion(client, response.json()["job_id"])
        assert upload_job["status"] == "completed"
        assert upload_job["artifacts"]["youtube_watch"].startswith("https://www.youtube.com/watch?v=")
        assert upload_job["metadata"]["remote_file"]["original_filename"] == "hello.txt"

        library_response = client.get("/api/library")
        assert library_response.status_code == 200
        library_payload = library_response.json()
        assert len(library_payload["files"]) == 1
        assert library_payload["files"][0]["video_id"] == "video-1"
        assert list(isolated_jobs_dir.iterdir()) == []


def test_api_upload_requires_connected_youtube(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService(connected=False)
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello", "text/plain")},
            data={"key": VALID_KEY},
        )
        assert response.status_code == 409
        assert response.json()["detail"] == "Connect YouTube before uploading files."


def test_api_downloads_and_recovers_youtube_file(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)
    source_bytes = b"hello from storagex\n" * 32

    with TestClient(server_app.app) as client:
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", source_bytes, "text/plain")},
            data={"key": VALID_KEY},
        )
        assert upload_response.status_code == 200
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]

        download_response = client.post(
            f"/api/files/{video_id}/download",
            data={"key": VALID_KEY},
        )
        assert download_response.status_code == 200

        download_job = _wait_for_completion(client, download_response.json()["job_id"])
        assert download_job["status"] == "completed"
        assert download_job["metadata"]["integrity_ok"] is True

        recovered_response = client.get(download_job["artifacts"]["recovered_file"])
        assert recovered_response.status_code == 200
        assert recovered_response.content == source_bytes


def _wait_for_completion(client: TestClient, job_id: str, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = client.get(f"/api/jobs/{job_id}")
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in {"completed", "failed"}:
            return payload
        time.sleep(0.1)
    raise AssertionError(f"Job {job_id} did not finish in time.")
