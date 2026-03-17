from __future__ import annotations

import asyncio
import io
import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from fastapi import UploadFile
from fastapi.testclient import TestClient

import server.app as server_app
from server.codec.constants import LEGACY_FORMAT_VERSION
from server.codec.format import build_stream_with_manifest
from server.codec.keyed import scramble_payload
from server.codec.service import _render_frame_grid, _write_frame_png
from server.share_store import ShareStore
from server.codec.video import encode_frames_to_webm
from server.youtube import (
    InMemoryYouTubeStore,
    YouTubeAuthError,
    YouTubeConfigurationError,
    YouTubeError,
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

    def delete_video(self, video_id: str) -> None:
        self.files = [item for item in self.files if item.video_id != video_id]
        self._remote_video_payloads.pop(video_id, None)


class FakeFailingAuthYouTubeService:
    def complete_authorization(self, *, state: str, code: str, redirect_uri: str) -> None:
        del state, code, redirect_uri
        raise YouTubeAuthError("Google rejected the client secret.")


class FakeUnavailableYouTubeService(FakeYouTubeService):
    def list_files(self) -> list[YouTubeFileRecord]:
        raise YouTubeError("Could not read the connected YouTube channel.")


class FakeBrokenDownloadYouTubeService(FakeYouTubeService):
    def download_video(self, *, video_id: str, output_dir: Path) -> Path:
        del video_id, output_dir
        raise YouTubeError("downstream failure")


class FakeQuickTunnelManager:
    def __init__(self, public_url: str = "https://fake-public.trycloudflare.com") -> None:
        self.public_url = public_url
        self.calls: list[str] = []

    def ensure_started(self, *, local_url: str) -> str:
        self.calls.append(local_url)
        return self.public_url

    def stop(self) -> None:
        return


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
        assert encode_job["artifacts"]["video"].endswith(".mkv")
        assert "frames_zip" not in encode_job["artifacts"]
        assert "frame_files" not in encode_job["metadata"]

        video_response = client.get(encode_job["artifacts"]["video"])
        assert video_response.status_code == 200
        assert video_response.content
        assert [path for path in isolated_jobs_dir.iterdir() if path.is_dir()] == []

        decode_response = client.post(
            "/api/decode",
            files={"file": ("encoded.mkv", video_response.content, "video/x-matroska")},
            data={"key": VALID_KEY},
        )
        assert decode_response.status_code == 200
        decode_job = _wait_for_completion(client, decode_response.json()["job_id"])
        assert decode_job["status"] == "completed"
        assert decode_job["metadata"]["integrity_ok"] is True

        recovered_response = client.get(decode_job["artifacts"]["recovered_file"])
        assert recovered_response.status_code == 200
        assert recovered_response.content == b"hello from storagex\n" * 100
        assert [path for path in isolated_jobs_dir.iterdir() if path.is_dir()] == []


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
        assert [path for path in isolated_jobs_dir.iterdir() if path.is_dir()] == []

        decode_response = client.post(
            "/api/decode",
            files={"file": ("encoded.mkv", video_response.content, "video/x-matroska")},
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
        assert [path for path in isolated_jobs_dir.iterdir() if path.is_dir()] == []


def test_api_decode_accepts_legacy_webm_upload(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        legacy_video = _build_legacy_webm_archive(isolated_jobs_dir, filename="legacy.txt", payload=b"legacy payload\n" * 64)
        decode_response = client.post(
            "/api/decode",
            files={"file": ("encoded.webm", legacy_video.read_bytes(), "video/webm")},
            data={"key": VALID_KEY},
        )
        assert decode_response.status_code == 200
        decode_job = _wait_for_completion(client, decode_response.json()["job_id"])
        assert decode_job["status"] == "completed"
        assert decode_job["metadata"]["integrity_ok"] is True

        recovered_response = client.get(decode_job["artifacts"]["recovered_file"])
        assert recovered_response.status_code == 200
        assert recovered_response.content == b"legacy payload\n" * 64


def test_api_accepts_office_document_upload_type(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        response = client.post(
            "/api/encode",
            files={"file": ("slides.pptx", b"office payload", "application/vnd.openxmlformats-officedocument.presentationml.presentation")},
            data={"key": VALID_KEY},
        )
        assert response.status_code == 200
        encode_job = _wait_for_completion(client, response.json()["job_id"])
        assert encode_job["status"] == "completed"
        assert encode_job["metadata"]["original_filename"] == "slides.pptx"


def test_api_encode_debug_artifacts_are_opt_in(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        default_response = client.post(
            "/api/encode",
            files={"file": ("hello.txt", b"hello from storagex\n" * 64, "text/plain")},
            data={"key": VALID_KEY},
        )
        assert default_response.status_code == 200
        default_job = _wait_for_completion(client, default_response.json()["job_id"])
        assert default_job["status"] == "completed"
        assert "frames_zip" not in default_job["artifacts"]
        assert "frame_files" not in default_job["metadata"]

        debug_response = client.post(
            "/api/encode",
            files={"file": ("hello.txt", b"hello from storagex\n" * 64, "text/plain")},
            data={"key": VALID_KEY, "debug_artifacts": "true"},
        )
        assert debug_response.status_code == 200
        debug_job = _wait_for_completion(client, debug_response.json()["job_id"])
        assert debug_job["status"] == "completed"
        assert debug_job["artifacts"]["frames_zip"].endswith(".zip")
        assert debug_job["metadata"]["frame_files"]


def test_write_incoming_upload_allows_payloads_above_previous_limit(isolated_jobs_dir) -> None:
    upload = UploadFile(
        filename="deck.pptx",
        file=io.BytesIO(b"x" * ((10 * 1024 * 1024) + 1)),
        headers={"content-type": "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
    )

    source_path = asyncio.run(server_app._write_incoming_upload("job-large", upload, "deck.pptx"))

    assert source_path.name == "deck.pptx"
    assert source_path.stat().st_size == (10 * 1024 * 1024) + 1


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
    fake_youtube.files.append(_youtube_file(video_id="video-1", original_filename="hello.txt"))
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        response = client.get("/api/library")
        assert response.status_code == 200
        payload = response.json()
        assert payload["connected"] is True
        assert payload["channel_title"] == "Test Channel"
        assert len(payload["files"]) == 1
        assert payload["files"][0]["original_filename"] == "hello.txt"
        assert payload["files"][0]["folder_id"] == "root"
        assert payload["folders"] == [{"id": "root", "name": "All files", "parent_id": None}]


def test_library_recovers_from_corrupt_local_index(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    fake_youtube.files.append(_youtube_file(video_id="video-1", original_filename="hello.txt"))
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)
    server_app.library_index._path.write_text("{broken", encoding="utf-8")

    with TestClient(server_app.app) as client:
        response = client.get("/api/library")

    assert response.status_code == 200
    payload = response.json()
    assert payload["index_recovered"] is True
    assert payload["files"][0]["folder_id"] == "root"
    assert payload["folders"] == [{"id": "root", "name": "All files", "parent_id": None}]


def test_folder_endpoints_create_and_reject_cycles(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        parent_response = client.post("/api/library/folders", json={"name": "Work"})
        assert parent_response.status_code == 200
        parent_id = parent_response.json()["folder"]["id"]

        child_response = client.post("/api/library/folders", json={"name": "2026", "parent_id": parent_id})
        assert child_response.status_code == 200
        child_id = child_response.json()["folder"]["id"]

        cycle_response = client.patch(f"/api/library/folders/{parent_id}", json={"parent_id": child_id})
        assert cycle_response.status_code == 400
        assert cycle_response.json()["detail"] == "Folders cannot be moved into themselves."


def test_upload_uses_folder_assignment_and_local_rename_for_downloads(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        folder_response = client.post("/api/library/folders", json={"name": "Receipts"})
        assert folder_response.status_code == 200
        folder_id = folder_response.json()["folder"]["id"]

        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 32, "text/plain")},
            data={"key": VALID_KEY, "folder_id": folder_id},
        )
        assert upload_response.status_code == 200
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        assert upload_job["metadata"]["remote_file"]["folder_id"] == folder_id

        rename_response = client.patch(
            f"/api/library/files/{video_id}",
            json={"display_name": "Receipt_March_2026.txt"},
        )
        assert rename_response.status_code == 200
        assert rename_response.json()["file"]["display_name"] == "Receipt_March_2026.txt"

        library_response = client.get("/api/library")
        assert library_response.status_code == 200
        library_file = library_response.json()["files"][0]
        assert library_file["folder_id"] == folder_id
        assert library_file["display_name_override"] == "Receipt_March_2026.txt"
        assert library_file["display_name"] == "Receipt_March_2026.txt"

        download_response = client.post(
            f"/api/files/{video_id}/download",
            data={"key": VALID_KEY},
        )
        assert download_response.status_code == 200
        download_job = _wait_for_completion(client, download_response.json()["job_id"])
        assert download_job["metadata"]["original_filename"] == "hello.txt"
        assert download_job["metadata"]["display_filename"] == "Receipt_March_2026.txt"


def test_delete_folder_moves_files_back_to_root(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        parent_response = client.post("/api/library/folders", json={"name": "Work"})
        assert parent_response.status_code == 200
        parent_id = parent_response.json()["folder"]["id"]

        child_response = client.post("/api/library/folders", json={"name": "Reports", "parent_id": parent_id})
        assert child_response.status_code == 200
        child_id = child_response.json()["folder"]["id"]

        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 32, "text/plain")},
            data={"key": VALID_KEY, "folder_id": child_id},
        )
        assert upload_response.status_code == 200
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]

        delete_response = client.delete(f"/api/library/folders/{parent_id}")
        assert delete_response.status_code == 200
        payload = delete_response.json()
        assert payload["status"] == "deleted"
        assert set(payload["result"]["deleted_folder_ids"]) == {parent_id, child_id}
        assert payload["result"]["moved_file_ids"] == [video_id]

        library_response = client.get("/api/library")
        assert library_response.status_code == 200
        library_payload = library_response.json()
        assert library_payload["folders"] == [{"id": "root", "name": "All files", "parent_id": None}]
        assert library_payload["files"][0]["folder_id"] == "root"


def test_delete_root_folder_is_rejected(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        response = client.delete("/api/library/folders/root")

    assert response.status_code == 400
    assert response.json()["detail"] == "The root folder cannot be deleted."


def test_delete_file_removes_it_from_library_and_youtube(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 16, "text/plain")},
            data={"key": VALID_KEY},
        )
        assert upload_response.status_code == 200
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]

        delete_response = client.delete(f"/api/library/files/{video_id}")
        assert delete_response.status_code == 200
        assert delete_response.json()["status"] == "deleted"
        assert fake_youtube.files == []
        assert video_id not in fake_youtube._remote_video_payloads

        library_response = client.get("/api/library")
        assert library_response.status_code == 200
        assert library_response.json()["files"] == []


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


def test_app_settings_can_be_saved_at_runtime(isolated_jobs_dir) -> None:
    with TestClient(server_app.app) as client:
        response = client.get("/api/settings/app")
        assert response.status_code == 200
        assert response.json() == {"configured": False, "public_app_url": ""}

        save_response = client.post(
            "/api/settings/app",
            json={"public_app_url": "https://files.example.com"},
        )
        assert save_response.status_code == 200
        assert save_response.json() == {
            "configured": True,
            "public_app_url": "https://files.example.com",
        }


def test_quick_tunnel_endpoint_creates_and_saves_public_url(isolated_jobs_dir, monkeypatch) -> None:
    fake_tunnel = FakeQuickTunnelManager()
    monkeypatch.setattr(server_app, "quick_tunnel_manager", fake_tunnel)

    with TestClient(server_app.app) as client:
        response = client.post("/api/settings/app/public-url/quick-tunnel")

    assert response.status_code == 200
    assert response.json() == {
        "configured": True,
        "public_app_url": "https://fake-public.trycloudflare.com",
    }
    assert fake_tunnel.calls == ["http://127.0.0.1:8000"]


def test_create_share_requires_public_app_url(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 16, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]

        response = client.post(f"/api/library/files/{video_id}/share", json={"key": VALID_KEY})

    assert response.status_code == 400
    assert response.json()["detail"] == "Create or save a Public App URL in Settings before sharing files."


def test_create_share_prepares_share_and_returns_job(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 16, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]

        response = client.post(f"/api/library/files/{video_id}/share", json={"key": VALID_KEY})
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "queued"
        share_job = _wait_for_completion(client, payload["job_id"])

    assert share_job["status"] == "completed"
    assert share_job["metadata"]["share_url"].startswith("https://files.example.com/s/")


def test_share_persists_across_restart_and_rejects_expired_token(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 8, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        share_response = client.post(f"/api/library/files/{video_id}/share", json={"key": VALID_KEY})
        share_job = _wait_for_completion(client, share_response.json()["job_id"])
        token = share_job["metadata"]["share_url"].rsplit("/", 1)[-1]

    shares_path = server_app.share_store._path
    payload = json.loads(shares_path.read_text(encoding="utf-8"))
    payload["shares"][token]["expires_at"] = "2026-03-01T00:00:00Z"
    shares_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    monkeypatch.setattr(server_app, "share_store", ShareStore(shares_path))

    with TestClient(server_app.app) as client:
        page_response = client.get(f"/s/{token}")
        download_response = client.get(f"/api/shares/{token}/download")

    assert page_response.status_code == 410
    assert "expired" in page_response.text.lower()
    assert download_response.status_code == 410
    assert download_response.json()["detail"] == "This share link has expired."


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


def test_library_keeps_saved_connection_when_youtube_read_temporarily_fails(isolated_jobs_dir, monkeypatch) -> None:
    monkeypatch.setattr(server_app, "youtube_service", FakeUnavailableYouTubeService())

    with TestClient(server_app.app) as client:
        response = client.get("/api/library")

    assert response.status_code == 200
    payload = response.json()
    assert payload["connected"] is True
    assert payload["error"] == "Could not read the connected YouTube channel."


def test_youtube_authorization_reuses_pkce_verifier(isolated_jobs_dir, monkeypatch) -> None:
    from server.youtube import YouTubeService

    FakeFlow.instances.clear()
    service = YouTubeService(store=InMemoryYouTubeStore())
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
        assert [path for path in isolated_jobs_dir.iterdir() if path.is_dir()] == []


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
        assert list(isolated_jobs_dir.iterdir()) == []


def test_share_prepare_with_wrong_owner_key_fails_without_exposing_download(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 12, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        bad_job, bad_token = _create_share_and_wait(client, video_id, key=WRONG_KEY)
        good_job, good_token = _create_share_and_wait(client, video_id, key=VALID_KEY)

    assert bad_job["status"] == "failed"
    assert bad_job["error"] == "That key does not unlock this file."
    assert bad_job["metadata"]["share_url"].endswith(f"/s/{bad_token}")
    assert good_job["status"] == "completed"
    assert good_job["metadata"]["share_url"].endswith(f"/s/{good_token}")
    assert bad_token != good_token


def test_share_download_logs_ip_and_marks_link_used(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)
    source_bytes = b"hello from storagex\n" * 10

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", source_bytes, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        share_job, token = _create_share_and_wait(client, video_id)
        artifact_dir = Path(server_app.DATA_DIR) / "share-artifacts" / token
        first_response = client.get(
            f"/api/shares/{token}/download",
            headers={"cf-connecting-ip": "203.0.113.10"},
        )
        second_response = client.get(
            f"/api/shares/{token}/download",
            headers={"x-forwarded-for": "198.51.100.24"},
        )
        owner_shares = client.get("/api/library/shares")

    assert share_job["status"] == "completed"
    assert first_response.status_code == 200
    assert first_response.content == source_bytes
    assert second_response.status_code == 410
    assert second_response.json()["detail"] == "This share link has already been used."
    assert owner_shares.status_code == 200
    share_payload = _share_from_owner_list(owner_shares.json()["shares"], token)
    assert share_payload["status"] == "used"
    assert share_payload["download_count"] == 1
    assert share_payload["downloads"][0]["ip_address"] == "203.0.113.10"
    assert artifact_dir.exists() is False


def test_owner_can_extend_used_share_for_another_download(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)
    source_bytes = b"hello from storagex\n" * 8

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", source_bytes, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        share_job, token = _create_share_and_wait(client, video_id)

        first_response = client.get(
            f"/api/shares/{token}/download",
            headers={"cf-connecting-ip": "203.0.113.10"},
        )
        extend_response = client.post(f"/api/library/shares/{token}/extend", json={"key": VALID_KEY})
        extend_job = _wait_for_completion(client, extend_response.json()["job_id"])
        second_response = client.get(
            f"/api/shares/{token}/download",
            headers={"x-forwarded-for": "198.51.100.24"},
        )
        owner_shares = client.get("/api/library/shares")

    assert share_job["status"] == "completed"
    assert first_response.status_code == 200
    assert first_response.content == source_bytes
    assert extend_response.status_code == 200
    assert extend_response.json()["share"]["status"] == "pending"
    assert extend_job["status"] == "completed"
    assert extend_job["metadata"]["share_url"].endswith(f"/s/{token}")
    assert second_response.status_code == 200
    assert second_response.content == source_bytes
    assert owner_shares.status_code == 200
    share_payload = _share_from_owner_list(owner_shares.json()["shares"], token)
    assert share_payload["status"] == "used"
    assert share_payload["download_count"] == 2
    assert share_payload["downloads"][0]["ip_address"] == "198.51.100.24"
    assert share_payload["downloads"][1]["ip_address"] == "203.0.113.10"


def test_failed_share_extension_restores_used_link(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 8, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        _, token = _create_share_and_wait(client, video_id)

        first_response = client.get(
            f"/api/shares/{token}/download",
            headers={"cf-connecting-ip": "203.0.113.10"},
        )
        extend_response = client.post(f"/api/library/shares/{token}/extend", json={"key": WRONG_KEY})
        extend_job = _wait_for_completion(client, extend_response.json()["job_id"])
        second_response = client.get(f"/api/shares/{token}/download")
        owner_shares = client.get("/api/library/shares")

    assert first_response.status_code == 200
    assert extend_response.status_code == 200
    assert extend_response.json()["share"]["status"] == "pending"
    assert extend_job["status"] == "failed"
    assert extend_job["error"] == "That key does not unlock this file."
    assert second_response.status_code == 410
    assert second_response.json()["detail"] == "This share link has already been used."
    share_payload = _share_from_owner_list(owner_shares.json()["shares"], token)
    assert share_payload["status"] == "used"
    assert share_payload["download_count"] == 1
    assert share_payload["downloads"][0]["ip_address"] == "203.0.113.10"


def test_owner_can_list_and_revoke_shares(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 6, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        _, token = _create_share_and_wait(client, video_id)
        artifact_dir = Path(server_app.DATA_DIR) / "share-artifacts" / token

        list_response = client.get("/api/library/shares")
        revoke_response = client.post(f"/api/library/shares/{token}/revoke")
        download_response = client.get(f"/api/shares/{token}/download")

    assert list_response.status_code == 200
    listed_share = _share_from_owner_list(list_response.json()["shares"], token)
    assert listed_share["status"] == "active"
    assert listed_share["share_url"].endswith(f"/s/{token}")
    assert revoke_response.status_code == 200
    assert revoke_response.json()["share"]["status"] == "revoked"
    assert download_response.status_code == 410
    assert download_response.json()["detail"] == "This share link is no longer available."
    assert artifact_dir.exists() is False


def test_replacing_share_invalidates_previous_token(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 10, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]

        first_job, first_share = _create_share_and_wait(client, video_id)
        first_artifact_dir = Path(server_app.DATA_DIR) / "share-artifacts" / first_share
        second_job, second_share = _create_share_and_wait(client, video_id)

        old_response = client.get(f"/api/shares/{first_share}/download")
        new_response = client.get(f"/api/shares/{second_share}/download")

    assert first_share != second_share
    assert first_job["status"] == "completed"
    assert second_job["status"] == "completed"
    assert old_response.status_code == 410
    assert old_response.json()["detail"] == "This share link is no longer available."
    assert new_response.status_code == 200
    assert first_artifact_dir.exists() is False


def test_share_failure_hides_private_youtube_details(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeBrokenDownloadYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 12, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        share_job, token = _create_share_and_wait(client, video_id)
        page_response = client.get(f"/s/{token}")

    assert share_job["status"] == "failed"
    assert share_job["error"] == "Could not prepare this share."
    assert "downstream failure" not in share_job["error"]
    assert page_response.status_code == 410


def test_share_page_renders_file_details(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 4, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        share_job, token = _create_share_and_wait(client, video_id)

        response = client.get(f"/s/{token}")

    assert share_job["status"] == "completed"
    assert response.status_code == 200
    assert "hello.txt" in response.text
    assert "window.__SHARE_PAGE__" in response.text
    assert f"/api/shares/{token}/download" in response.text


def test_public_host_only_serves_share_routes(isolated_jobs_dir, monkeypatch) -> None:
    fake_youtube = FakeYouTubeService()
    monkeypatch.setattr(server_app, "youtube_service", fake_youtube)

    with TestClient(server_app.app) as client:
        client.post("/api/settings/app", json={"public_app_url": "https://files.example.com"})
        upload_response = client.post(
            "/api/files",
            files={"file": ("hello.txt", b"hello from storagex\n" * 4, "text/plain")},
            data={"key": VALID_KEY},
        )
        upload_job = _wait_for_completion(client, upload_response.json()["job_id"])
        video_id = upload_job["metadata"]["remote_file"]["video_id"]
        _, token = _create_share_and_wait(client, video_id)

        blocked_response = client.get("/", headers={"host": "files.example.com"})
        share_response = client.get(f"/s/{token}", headers={"host": "files.example.com"})

    assert blocked_response.status_code == 404
    assert blocked_response.text == "Not Found"
    assert share_response.status_code == 200
    assert "window.__SHARE_PAGE__" in share_response.text


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


def _create_share_and_wait(client: TestClient, video_id: str, *, key: str = VALID_KEY) -> tuple[dict, str]:
    response = client.post(f"/api/library/files/{video_id}/share", json={"key": key})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "queued"
    job = _wait_for_completion(client, payload["job_id"])
    token = str(job["metadata"]["share_url"]).rsplit("/", 1)[-1]
    return job, token


def _share_from_owner_list(shares: list[dict], token: str) -> dict:
    for share in shares:
        if share.get("token") == token:
            return share
    raise AssertionError(f"Missing share {token}")


def _build_legacy_webm_archive(root: Path, *, filename: str, payload: bytes) -> Path:
    protected = scramble_payload(payload, VALID_KEY)
    manifest, _, chunks = build_stream_with_manifest(
        original_filename=filename,
        media_type="text/plain",
        original_bytes=payload,
        stored_bytes=protected,
        version=LEGACY_FORMAT_VERSION,
    )

    frames_dir = root / f"legacy-frames-{filename}"
    frames_dir.mkdir(parents=True, exist_ok=True)
    for index, chunk in enumerate(chunks):
        frame_path = frames_dir / f"frame_{index + 1:06d}.png"
        _write_frame_png(_render_frame_grid(frame_index=index, chunk=chunk), frame_path)

    video_path = root / f"{Path(filename).stem}.webm"
    encode_frames_to_webm(frames_dir, video_path)
    assert manifest["version"] == LEGACY_FORMAT_VERSION
    return video_path


def _youtube_file(*, video_id: str, original_filename: str) -> YouTubeFileRecord:
    return YouTubeFileRecord(
        video_id=video_id,
        original_filename=original_filename,
        media_type="text/plain",
        original_size=1024,
        stored_size=1024,
        sha256=f"sha-{video_id}",
        crc32=f"crc-{video_id}",
        frame_count=2,
        fps=24,
        uploaded_at="2026-03-16T10:00:00Z",
        privacy_status="private",
        watch_url=f"https://www.youtube.com/watch?v={video_id}",
        studio_url=f"https://studio.youtube.com/video/{video_id}/edit",
        thumbnail_url=None,
        youtube_title=f"StorageX · {original_filename}",
    )
