from __future__ import annotations

import mimetypes
import json
import math
import os
import re
import shutil
import subprocess
import sys
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from .codec.constants import DATA_DIR, FORMAT_VERSION

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]
METADATA_BLOCK_PATTERN = re.compile(r"\[storagex\](?P<body>.*?)\[/storagex\]", re.DOTALL)
TITLE_PREFIX = "StorageX · "
METADATA_TAG = "storagex"
UPLOAD_PROGRESS_START = 80
UPLOAD_PROGRESS_SPAN = 19
YOUTUBE_STORE_PATH = DATA_DIR / "youtube-auth.json"


class YouTubeError(RuntimeError):
    pass


class YouTubeConfigurationError(YouTubeError):
    pass


class YouTubeAuthError(YouTubeError):
    pass


class YouTubeUploadError(YouTubeError):
    pass


class YouTubeDownloadError(YouTubeError):
    pass


ProgressCallback = Callable[[int, str], None]


@dataclass(slots=True)
class YouTubeSessionStatus:
    configured: bool
    connected: bool
    channel_title: str | None
    privacy_status: str


@dataclass(slots=True)
class YouTubeFileRecord:
    video_id: str
    original_filename: str
    media_type: str
    original_size: int
    stored_size: int
    sha256: str
    crc32: str
    frame_count: int
    fps: int
    uploaded_at: str
    privacy_status: str
    watch_url: str
    studio_url: str
    thumbnail_url: str | None
    youtube_title: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class InMemoryYouTubeStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._credentials: dict[str, Any] | None = None
        self._oauth_states: dict[str, tuple[str, str | None]] = {}
        self._client_id: str | None = None
        self._client_secret: str | None = None

    def set_credentials(self, credentials: Any) -> None:
        payload = json.loads(credentials.to_json())
        with self._lock:
            self._credentials = payload

    def get_credentials(self) -> dict[str, Any] | None:
        with self._lock:
            if self._credentials is None:
                return None
            return dict(self._credentials)

    def clear_credentials(self) -> None:
        with self._lock:
            self._credentials = None
            self._oauth_states.clear()

    def set_client_config(self, client_id: str | None, client_secret: str | None) -> None:
        with self._lock:
            self._client_id = client_id or None
            self._client_secret = client_secret or None
            self._credentials = None
            self._oauth_states.clear()

    def get_client_config(self) -> tuple[str | None, str | None]:
        with self._lock:
            return self._client_id, self._client_secret

    def add_state(self, state: str, redirect_uri: str, code_verifier: str | None) -> None:
        with self._lock:
            self._oauth_states[state] = (redirect_uri, code_verifier)

    def pop_state(self, state: str) -> tuple[str, str | None] | None:
        with self._lock:
            return self._oauth_states.pop(state, None)

    def reset(self) -> None:
        with self._lock:
            self._credentials = None
            self._oauth_states.clear()
            self._client_id = None
            self._client_secret = None


class PersistentYouTubeStore(InMemoryYouTubeStore):
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or YOUTUBE_STORE_PATH
        super().__init__()
        self._load()

    def set_credentials(self, credentials: Any) -> None:
        super().set_credentials(credentials)
        self._persist()

    def clear_credentials(self) -> None:
        super().clear_credentials()
        self._persist()

    def set_client_config(self, client_id: str | None, client_secret: str | None) -> None:
        super().set_client_config(client_id, client_secret)
        self._persist()

    def add_state(self, state: str, redirect_uri: str, code_verifier: str | None) -> None:
        super().add_state(state, redirect_uri, code_verifier)
        self._persist()

    def pop_state(self, state: str) -> tuple[str, str | None] | None:
        value = super().pop_state(state)
        if value is not None:
            self._persist()
        return value

    def reset(self) -> None:
        super().reset()
        self._persist()

    def _load(self) -> None:
        if not self._path.exists():
            return

        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            return

        oauth_states = payload.get("oauth_states") if isinstance(payload, dict) else {}
        normalized_states: dict[str, tuple[str, str | None]] = {}
        if isinstance(oauth_states, dict):
            for state, value in oauth_states.items():
                if not isinstance(state, str) or not isinstance(value, dict):
                    continue
                redirect_uri = value.get("redirect_uri")
                code_verifier = value.get("code_verifier")
                if isinstance(redirect_uri, str):
                    normalized_states[state] = (
                        redirect_uri,
                        code_verifier if isinstance(code_verifier, str) else None,
                    )

        with self._lock:
            credentials = payload.get("credentials") if isinstance(payload, dict) else None
            self._credentials = credentials if isinstance(credentials, dict) else None
            client_id = payload.get("client_id") if isinstance(payload, dict) else None
            client_secret = payload.get("client_secret") if isinstance(payload, dict) else None
            self._client_id = client_id if isinstance(client_id, str) and client_id else None
            self._client_secret = client_secret if isinstance(client_secret, str) and client_secret else None
            self._oauth_states = normalized_states

    def _persist(self) -> None:
        with self._lock:
            payload = {
                "credentials": self._credentials,
                "oauth_states": {
                    state: {
                        "redirect_uri": redirect_uri,
                        "code_verifier": code_verifier,
                    }
                    for state, (redirect_uri, code_verifier) in self._oauth_states.items()
                },
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }

        if not payload["credentials"] and not payload["oauth_states"] and not payload["client_id"] and not payload["client_secret"]:
            self._path.unlink(missing_ok=True)
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self._path)
        try:
            os.chmod(self._path, 0o600)
        except OSError:
            pass


class YouTubeService:
    def __init__(self, store: InMemoryYouTubeStore | PersistentYouTubeStore | None = None) -> None:
        self._store = store or PersistentYouTubeStore()

    def session_status(self) -> YouTubeSessionStatus:
        if not self.is_configured():
            return YouTubeSessionStatus(
                configured=False,
                connected=False,
                channel_title=None,
                privacy_status=self.privacy_status,
            )

        credentials_payload = self._store.get_credentials()
        if credentials_payload is None:
            return YouTubeSessionStatus(
                configured=True,
                connected=False,
                channel_title=None,
                privacy_status=self.privacy_status,
            )

        try:
            channel_title, _uploads_playlist_id = self._channel_info()
        except YouTubeAuthError:
            self.disconnect()
            return YouTubeSessionStatus(
                configured=True,
                connected=False,
                channel_title=None,
                privacy_status=self.privacy_status,
            )

        return YouTubeSessionStatus(
            configured=True,
            connected=True,
            channel_title=channel_title,
            privacy_status=self.privacy_status,
        )

    def settings_snapshot(self) -> dict[str, Any]:
        runtime_client_id, runtime_client_secret = self._store.get_client_config()
        env_client_id = os.environ.get("YOUTUBE_CLIENT_ID") or os.environ.get("GOOGLE_CLIENT_ID")
        env_client_secret = os.environ.get("YOUTUBE_CLIENT_SECRET") or os.environ.get("GOOGLE_CLIENT_SECRET")

        client_id = runtime_client_id or env_client_id or ""
        has_client_secret = bool(runtime_client_secret or env_client_secret)
        source = "runtime" if runtime_client_id or runtime_client_secret else ("environment" if client_id or has_client_secret else "none")

        return {
            "configured": bool(client_id and has_client_secret),
            "client_id": client_id,
            "has_client_secret": has_client_secret,
            "source": source,
        }

    def set_runtime_client_config(self, *, client_id: str, client_secret: str) -> dict[str, Any]:
        normalized_client_id = client_id.strip()
        normalized_client_secret = client_secret.strip()
        if not normalized_client_id or not normalized_client_secret:
            raise YouTubeConfigurationError("Client ID and client secret are required.")
        self._store.set_client_config(normalized_client_id, normalized_client_secret)
        return self.settings_snapshot()

    def clear_runtime_client_config(self) -> dict[str, Any]:
        self._store.set_client_config(None, None)
        return self.settings_snapshot()

    def reset_local_state(self) -> dict[str, Any]:
        self._store.reset()
        return self.settings_snapshot()

    def authorization_url(self, redirect_uri: str) -> str:
        self._ensure_configured()
        Flow = self._import_flow()
        flow = Flow.from_client_config(
            self._client_config(redirect_uri),
            scopes=YOUTUBE_SCOPES,
            redirect_uri=redirect_uri,
        )
        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            prompt="consent",
        )
        self._store.add_state(state, redirect_uri, getattr(flow, "code_verifier", None))
        return authorization_url

    def complete_authorization(self, *, state: str, code: str, redirect_uri: str) -> None:
        self._ensure_configured()
        stored_state = self._store.pop_state(state)
        if stored_state is None:
            raise YouTubeAuthError("The YouTube authorization flow expired. Start the connection again.")
        expected_redirect_uri, code_verifier = stored_state
        if expected_redirect_uri != redirect_uri:
            raise YouTubeAuthError("The YouTube authorization flow expired. Start the connection again.")

        Flow = self._import_flow()
        flow = Flow.from_client_config(
            self._client_config(redirect_uri),
            scopes=YOUTUBE_SCOPES,
            state=state,
            redirect_uri=redirect_uri,
            code_verifier=code_verifier,
        )
        try:
            flow.fetch_token(code=code)
        except Exception as exc:  # pragma: no cover - network/auth failure
            raise YouTubeAuthError(self._token_exchange_error_message(exc)) from exc

        self._store.set_credentials(flow.credentials)

    def disconnect(self) -> None:
        self._store.clear_credentials()

    def list_files(self) -> list[YouTubeFileRecord]:
        channel_title, uploads_playlist_id = self._channel_info()
        del channel_title
        service = self._build_service()
        playlist_published_at: dict[str, str] = {}
        video_ids: list[str] = []
        page_token: str | None = None

        while True:
            request = service.playlistItems().list(
                part="snippet,contentDetails,status",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=page_token,
            )
            response = request.execute()
            for item in response.get("items", []):
                video_id = (
                    item.get("contentDetails", {}).get("videoId")
                    or item.get("snippet", {}).get("resourceId", {}).get("videoId")
                    or ""
                )
                if not video_id:
                    continue
                video_ids.append(str(video_id))
                playlist_published_at[str(video_id)] = str(item.get("snippet", {}).get("publishedAt") or "")

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        files: list[YouTubeFileRecord] = []
        for offset in range(0, len(video_ids), 50):
            batch_ids = video_ids[offset : offset + 50]
            response = service.videos().list(part="snippet,status", id=",".join(batch_ids)).execute()
            for item in response.get("items", []):
                record = self._record_from_video_item(item, playlist_published_at)
                if record is not None:
                    files.append(record)

        files.sort(key=lambda item: item.uploaded_at, reverse=True)
        return files

    def upload_video(
        self,
        *,
        video_path: Path,
        manifest: dict[str, Any],
        progress: ProgressCallback | None = None,
    ) -> YouTubeFileRecord:
        service = self._build_service()
        MediaFileUpload = self._import_media_file_upload()

        record_metadata = {
            "app": METADATA_TAG,
            "format_version": FORMAT_VERSION,
            "original_filename": str(manifest["original_filename"]),
            "media_type": str(manifest["media_type"]),
            "original_size": int(manifest["original_size"]),
            "stored_size": int(manifest["stored_size"]),
            "sha256": str(manifest["sha256"]),
            "crc32": str(manifest["crc32"]),
            "frame_count": int(manifest["total_frames"]),
            "fps": int(manifest["fps"]),
            "privacy_status": self.privacy_status,
            "uploaded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }

        upload_request = service.videos().insert(
            part="snippet,status",
            body={
                "snippet": {
                    "title": build_youtube_title(record_metadata["original_filename"]),
                    "description": build_youtube_description(record_metadata),
                    "tags": [METADATA_TAG, "bit-video", "archive"],
                    "categoryId": "22",
                },
                "status": {
                    "privacyStatus": self.privacy_status,
                    "selfDeclaredMadeForKids": False,
                },
            },
            media_body=MediaFileUpload(
                str(video_path),
                mimetype=mimetypes.guess_type(video_path.name)[0] or "application/octet-stream",
                chunksize=8 * 1024 * 1024,
                resumable=True,
            ),
        )

        response: dict[str, Any] | None = None
        try:
            while response is None:
                upload_status, response = upload_request.next_chunk()
                if upload_status is None or progress is None:
                    continue

                percentage = math.floor(upload_status.progress() * 100)
                progress_value = min(
                    UPLOAD_PROGRESS_START + UPLOAD_PROGRESS_SPAN,
                    UPLOAD_PROGRESS_START + math.floor(upload_status.progress() * UPLOAD_PROGRESS_SPAN),
                )
                progress(progress_value, f"Uploading to YouTube ({percentage}%).")
        except Exception as exc:  # pragma: no cover - network/upload failure
            raise YouTubeUploadError("YouTube rejected the upload request.") from exc

        if response is None or "id" not in response:
            raise YouTubeUploadError("YouTube did not return a video ID for the uploaded archive.")

        video_id = str(response["id"])
        return YouTubeFileRecord(
            video_id=video_id,
            original_filename=record_metadata["original_filename"],
            media_type=record_metadata["media_type"],
            original_size=record_metadata["original_size"],
            stored_size=record_metadata["stored_size"],
            sha256=record_metadata["sha256"],
            crc32=record_metadata["crc32"],
            frame_count=record_metadata["frame_count"],
            fps=record_metadata["fps"],
            uploaded_at=record_metadata["uploaded_at"],
            privacy_status=record_metadata["privacy_status"],
            watch_url=watch_url(video_id),
            studio_url=studio_url(video_id),
            thumbnail_url=None,
            youtube_title=build_youtube_title(record_metadata["original_filename"]),
        )

    def get_file(self, video_id: str) -> YouTubeFileRecord:
        for item in self.list_files():
            if item.video_id == video_id:
                return item
        raise YouTubeDownloadError("File not found in your YouTube library.")

    def download_video(self, *, video_id: str, output_dir: Path) -> Path:
        file_record = self.get_file(video_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_template = str(output_dir / f"{file_record.video_id}.%(ext)s")

        errors: list[str] = []
        for command in self._download_commands(file_record.watch_url, output_template):
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
            if completed.returncode == 0:
                downloaded_files = sorted(output_dir.glob(f"{file_record.video_id}.*"))
                if downloaded_files:
                    return downloaded_files[0]
                errors.append("yt-dlp completed without creating a file.")
            else:
                stderr = completed.stderr.strip() or completed.stdout.strip()
                if stderr:
                    errors.append(stderr)

            for candidate in output_dir.glob(f"{file_record.video_id}.*"):
                candidate.unlink(missing_ok=True)

        if errors:
            detail = errors[-1]
            lowered_detail = detail.lower()
            if "challenge solving failed" in lowered_detail or "only images are available" in lowered_detail:
                raise YouTubeDownloadError(
                    "YouTube recovery needed a local challenge solver. The app has been updated for that; try the download again."
                )
            if "sign in" in lowered_detail or "cookies" in lowered_detail:
                raise YouTubeDownloadError(
                    "Could not access that YouTube video. Sign into YouTube in Chrome or Safari on this Mac and try again."
                )
        raise YouTubeDownloadError("Could not download the YouTube video for recovery.")

    @property
    def privacy_status(self) -> str:
        configured_value = os.environ.get("YOUTUBE_PRIVACY_STATUS", "private").strip().lower()
        if configured_value not in {"private", "unlisted", "public"}:
            return "private"
        return configured_value

    def is_configured(self) -> bool:
        return bool(self._client_id and self._client_secret)

    @property
    def _client_id(self) -> str | None:
        runtime_client_id, _runtime_client_secret = self._store.get_client_config()
        return runtime_client_id or os.environ.get("YOUTUBE_CLIENT_ID") or os.environ.get("GOOGLE_CLIENT_ID")

    @property
    def _client_secret(self) -> str | None:
        _runtime_client_id, runtime_client_secret = self._store.get_client_config()
        return runtime_client_secret or os.environ.get("YOUTUBE_CLIENT_SECRET") or os.environ.get("GOOGLE_CLIENT_SECRET")

    def _ensure_configured(self) -> None:
        if not self.is_configured():
            raise YouTubeConfigurationError("Save a YouTube client ID and client secret first.")

    def _client_config(self, redirect_uri: str) -> dict[str, Any]:
        client_id = self._client_id
        client_secret = self._client_secret
        if not client_id or not client_secret:
            raise YouTubeConfigurationError("Save a YouTube client ID and client secret first.")

        return {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [redirect_uri],
            }
        }

    def _credentials(self) -> Any:
        self._ensure_configured()
        credentials_payload = self._store.get_credentials()
        if credentials_payload is None:
            raise YouTubeAuthError("Connect your YouTube account before uploading files.")

        Credentials, GoogleRequest = self._import_credentials()
        credentials = Credentials.from_authorized_user_info(credentials_payload, YOUTUBE_SCOPES)
        if credentials.expired and credentials.refresh_token:
            try:
                credentials.refresh(GoogleRequest())
                self._store.set_credentials(credentials)
            except Exception as exc:  # pragma: no cover - token refresh failure
                self.disconnect()
                raise YouTubeAuthError("The saved YouTube session expired. Connect YouTube again.") from exc

        if not credentials.valid:
            self.disconnect()
            raise YouTubeAuthError("The saved YouTube session is no longer valid. Connect YouTube again.")

        return credentials

    def _build_service(self) -> Any:
        build = self._import_google_build()
        return build("youtube", "v3", credentials=self._credentials(), cache_discovery=False)

    def _channel_info(self) -> tuple[str, str]:
        service = self._build_service()
        try:
            response = service.channels().list(part="snippet,contentDetails", mine=True).execute()
        except Exception as exc:  # pragma: no cover - network/api failure
            raise YouTubeAuthError("Could not read the connected YouTube channel.") from exc

        items = response.get("items", [])
        if not items:
            raise YouTubeAuthError("The connected Google account does not expose a YouTube channel.")

        item = items[0]
        channel_title = str(item.get("snippet", {}).get("title", "YouTube"))
        uploads_playlist_id = str(item.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", ""))
        if not uploads_playlist_id:
            raise YouTubeAuthError("Could not find the YouTube uploads playlist for this channel.")
        return channel_title, uploads_playlist_id

    def _record_from_video_item(
        self,
        item: dict[str, Any],
        playlist_published_at: dict[str, str],
    ) -> YouTubeFileRecord | None:
        video_id = str(item.get("id") or "")
        if not video_id:
            return None

        snippet = item.get("snippet", {})
        description = str(snippet.get("description", ""))
        metadata = parse_youtube_description(description)
        if metadata is None or metadata.get("app") != METADATA_TAG:
            return None

        uploaded_at = str(metadata.get("uploaded_at") or playlist_published_at.get(video_id) or "")
        thumbnails = snippet.get("thumbnails", {})
        thumbnail_url = None
        for key in ("maxres", "standard", "high", "medium", "default"):
            candidate = thumbnails.get(key, {})
            if candidate.get("url"):
                thumbnail_url = str(candidate["url"])
                break

        return YouTubeFileRecord(
            video_id=video_id,
            original_filename=str(metadata["original_filename"]),
            media_type=str(metadata["media_type"]),
            original_size=int(metadata["original_size"]),
            stored_size=int(metadata["stored_size"]),
            sha256=str(metadata["sha256"]),
            crc32=str(metadata["crc32"]),
            frame_count=int(metadata["frame_count"]),
            fps=int(metadata["fps"]),
            uploaded_at=uploaded_at,
            privacy_status=str(item.get("status", {}).get("privacyStatus") or metadata.get("privacy_status") or self.privacy_status),
            watch_url=watch_url(video_id),
            studio_url=studio_url(video_id),
            thumbnail_url=thumbnail_url,
            youtube_title=str(snippet.get("title") or build_youtube_title(str(metadata["original_filename"]))),
        )

    @staticmethod
    def _import_flow() -> Any:
        try:
            from google_auth_oauthlib.flow import Flow
        except ImportError as exc:  # pragma: no cover - dependency failure
            raise YouTubeConfigurationError(
                "Install google-auth-oauthlib and google-api-python-client to use YouTube uploads."
            ) from exc
        return Flow

    @staticmethod
    def _import_google_build() -> Any:
        try:
            from googleapiclient.discovery import build
        except ImportError as exc:  # pragma: no cover - dependency failure
            raise YouTubeConfigurationError(
                "Install google-api-python-client to use YouTube uploads."
            ) from exc
        return build

    @staticmethod
    def _import_media_file_upload() -> Any:
        try:
            from googleapiclient.http import MediaFileUpload
        except ImportError as exc:  # pragma: no cover - dependency failure
            raise YouTubeConfigurationError(
                "Install google-api-python-client to use YouTube uploads."
            ) from exc
        return MediaFileUpload

    @staticmethod
    def _import_credentials() -> tuple[Any, Any]:
        try:
            from google.auth.transport.requests import Request as GoogleRequest
            from google.oauth2.credentials import Credentials
        except ImportError as exc:  # pragma: no cover - dependency failure
            raise YouTubeConfigurationError(
                "Install google-auth and google-auth-oauthlib to use YouTube uploads."
            ) from exc
        return Credentials, GoogleRequest

    @staticmethod
    def _yt_dlp_executable() -> str:
        system_ytdlp = shutil.which("yt-dlp")
        if system_ytdlp:
            return system_ytdlp

        bundled_ytdlp = Path(sys.executable).resolve().parent / "yt-dlp"
        if bundled_ytdlp.exists():
            return str(bundled_ytdlp)

        raise YouTubeDownloadError("yt-dlp is not installed for YouTube recovery.")

    def _download_commands(self, url: str, output_template: str) -> list[list[str]]:
        base_command = [
            self._yt_dlp_executable(),
            "--no-playlist",
            "--no-progress",
            "--no-warnings",
            "--quiet",
            "--output",
            output_template,
            "-f",
            "bestvideo/best",
        ]
        js_runtime = self._preferred_js_runtime()
        if js_runtime:
            base_command.extend(["--js-runtimes", js_runtime])
            base_command.extend(["--remote-components", "ejs:github"])
        cookiefile = os.environ.get("YOUTUBE_DOWNLOAD_COOKIEFILE", "").strip()
        browser = os.environ.get("YOUTUBE_DOWNLOAD_BROWSER", "").strip().lower()

        commands: list[list[str]] = []
        if cookiefile:
            commands.append(base_command + ["--cookies", cookiefile, url])
            commands.append(base_command + [url])
            return commands

        browsers = [browser] if browser else ["chrome", "safari"]
        for candidate in browsers:
            commands.append(base_command + ["--cookies-from-browser", candidate, url])
        commands.append(base_command + [url])
        return commands

    @staticmethod
    def _preferred_js_runtime() -> str | None:
        configured = os.environ.get("YOUTUBE_DOWNLOAD_JS_RUNTIME", "").strip()
        if configured:
            return configured

        node_path = shutil.which("node")
        if node_path:
            return f"node:{node_path}"
        return None

    @staticmethod
    def _token_exchange_error_message(exc: Exception) -> str:
        normalized = YouTubeService._extract_google_error_text(exc).lower()
        if "invalid_client" in normalized or "unauthorized_client" in normalized:
            return "Google rejected the client secret. Save the matching client ID and client secret, then connect again."
        if "redirect_uri_mismatch" in normalized:
            return "Google rejected the callback URL. Add the exact redirect URI to the OAuth client and try again."
        if "access_denied" in normalized:
            return "Google denied access to this app. Add your Google account as a test user, then connect again."
        return "Google did not return a valid YouTube authorization token."

    @staticmethod
    def _extract_google_error_text(exc: Exception) -> str:
        parts = [str(exc)]
        response = getattr(exc, "response", None)
        if response is not None:
            try:
                payload = response.json()
            except Exception:  # pragma: no cover - defensive parsing
                payload = None
            if isinstance(payload, dict):
                error = payload.get("error")
                if isinstance(error, dict):
                    if error.get("status"):
                        parts.append(str(error["status"]))
                    if error.get("message"):
                        parts.append(str(error["message"]))
                elif error:
                    parts.append(str(error))
                if payload.get("error_description"):
                    parts.append(str(payload["error_description"]))
        return " ".join(part for part in parts if part)


def build_youtube_title(filename: str) -> str:
    max_filename_length = 100 - len(TITLE_PREFIX)
    trimmed_filename = filename
    if len(trimmed_filename) > max_filename_length:
        suffix = Path(trimmed_filename).suffix
        stem = Path(trimmed_filename).stem
        available_stem = max(1, max_filename_length - len(suffix) - 3)
        trimmed_filename = f"{stem[:available_stem]}...{suffix}"
    return f"{TITLE_PREFIX}{trimmed_filename}"


def build_youtube_description(metadata: dict[str, Any]) -> str:
    body = json.dumps(metadata, separators=(",", ":"), sort_keys=True)
    return (
        "StorageX encoded archive.\n"
        "This video was generated by the StorageX uploader.\n\n"
        f"[storagex]{body}[/storagex]"
    )


def parse_youtube_description(description: str) -> dict[str, Any] | None:
    match = METADATA_BLOCK_PATTERN.search(description)
    if match is None:
        return None
    try:
        payload = json.loads(match.group("body"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def studio_url(video_id: str) -> str:
    return f"https://studio.youtube.com/video/{video_id}/edit"
