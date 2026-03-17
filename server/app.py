from __future__ import annotations

import json
import logging
import mimetypes
import shutil
import threading
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

from .app_settings import AppSettingsError, AppSettingsStore
from .codec.constants import (
    ALLOWED_DECODE_EXTENSIONS,
    DATA_DIR,
    JOBS_DIR,
    MAX_DECODE_UPLOAD_SIZE,
    YOUTUBE_VIDEO_NAME,
)
from .codec.keyed import validate_numeric_key
from .file_utils import guess_media_type, sanitize_filename
from .library_index import (
    ROOT_FOLDER_ID,
    UNSET,
    FileEntryNotFoundError,
    FolderConflictError,
    FolderNotFoundError,
    LibraryIndexError,
    LibraryIndexStore,
)
from .quick_tunnel import QuickTunnelError, QuickTunnelManager
from .share_store import SHARE_ARTIFACTS_DIR, ShareAccessError, ShareRecord, ShareReuseRestorePoint, ShareStore
from .youtube import YouTubeAuthError, YouTubeConfigurationError, YouTubeError, YouTubeService, YouTubeSessionExpiredError

logger = logging.getLogger(__name__)
UPLOAD_STREAM_CHUNK_SIZE = 1024 * 1024


@dataclass(slots=True)
class JobRecord:
    job_id: str
    kind: str
    status: str
    progress: int
    message: str
    created_at: float
    updated_at: float
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = threading.Lock()

    def create(self, kind: str) -> JobRecord:
        now = time.time()
        job = JobRecord(
            job_id=uuid.uuid4().hex,
            kind=kind,
            status="queued",
            progress=0,
            message="Queued.",
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def get(self, job_id: str) -> JobRecord:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            return job

    def update(self, job_id: str, **changes: Any) -> JobRecord:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            for key, value in changes.items():
                setattr(job, key, value)
            job.updated_at = time.time()
            return job

    def snapshot(self, job_id: str) -> JobRecord:
        job = self.get(job_id)
        return JobRecord(**asdict(job))


jobs = JobStore()
youtube_service = YouTubeService()
library_index = LibraryIndexStore()
app_settings = AppSettingsStore()
share_store = ShareStore()
quick_tunnel_manager = QuickTunnelManager()


class YouTubeSettingsPayload(BaseModel):
    client_id: str
    client_secret: str


class AppSettingsPayload(BaseModel):
    public_app_url: str | None = ""


class ShareCreatePayload(BaseModel):
    key: str


class FolderCreatePayload(BaseModel):
    name: str
    parent_id: str | None = ROOT_FOLDER_ID


class FolderUpdatePayload(BaseModel):
    name: str | None = None
    parent_id: str | None = None


class FileUpdatePayload(BaseModel):
    folder_id: str | None = None
    display_name: str | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    SHARE_ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    _cleanup_expired_jobs()
    _cleanup_stale_share_artifacts()
    try:
        yield
    finally:
        quick_tunnel_manager.stop()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.middleware("http")
async def restrict_public_host_to_share_routes(request: Request, call_next):
    if _request_uses_public_host(request) and not _is_public_share_path(request.url.path):
        return PlainTextResponse("Not Found", status_code=404)
    return await call_next(request)


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(
        _render_html_file(
            Path("static/index.html"),
            asset_paths=[
                Path("static/index.html"),
                Path("static/app.js"),
                Path("static/styles.css"),
                Path("static/favicon.svg"),
            ],
        ),
        headers={"Cache-Control": "no-store"},
    )


@app.get("/s/{token}")
async def share_page(token: str) -> HTMLResponse:
    _cleanup_share_artifact_if_unavailable(token)
    share_payload = _public_share_payload(token)
    html = _render_html_file(
        Path("static/share.html"),
        asset_paths=[
            Path("static/share.html"),
            Path("static/share.js"),
            Path("static/share.css"),
            Path("static/favicon.svg"),
        ],
        replacements={
            "__SHARE_PAGE_DATA__": _json_for_html(share_payload),
        },
    )
    status_code = 200
    if share_payload["status"] == "invalid":
        status_code = 404
    elif share_payload["status"] not in {"active", "pending"}:
        status_code = 410
    return HTMLResponse(html, status_code=status_code, headers={"Cache-Control": "no-store"})


@app.get("/favicon.ico")
async def favicon() -> FileResponse:
    return FileResponse("static/favicon.svg", media_type="image/svg+xml")


@app.get("/auth/youtube/start", name="youtube_connect")
async def youtube_connect(request: Request) -> RedirectResponse:
    redirect_uri = str(request.url_for("youtube_callback"))
    try:
        authorization_url = youtube_service.authorization_url(redirect_uri)
    except YouTubeConfigurationError as exc:
        return RedirectResponse(
            url=f"/?{urlencode({'youtube': 'error', 'reason': str(exc)})}",
            status_code=302,
        )
    return RedirectResponse(authorization_url, status_code=302)


@app.get("/auth/youtube/callback", name="youtube_callback")
async def youtube_callback(
    request: Request,
    state: str | None = None,
    code: str | None = None,
    error: str | None = None,
) -> RedirectResponse:
    if error:
        return RedirectResponse(
            url=f"/?{urlencode({'youtube': 'error', 'reason': error})}",
            status_code=302,
        )

    if not state or not code:
        return RedirectResponse(
            url=f"/?{urlencode({'youtube': 'error', 'reason': 'Missing authorization response from Google.'})}",
            status_code=302,
        )

    redirect_uri = str(request.url_for("youtube_callback"))
    try:
        youtube_service.complete_authorization(state=state, code=code, redirect_uri=redirect_uri)
    except YouTubeError as exc:
        logger.warning("YouTube OAuth callback failed: %s", exc, exc_info=True)
        return RedirectResponse(
            url=f"/?{urlencode({'youtube': 'error', 'reason': str(exc)})}",
            status_code=302,
        )

    return RedirectResponse(url=f"/?{urlencode({'youtube': 'connected'})}", status_code=302)


@app.post("/api/auth/disconnect")
async def disconnect_youtube() -> dict[str, str]:
    youtube_service.disconnect()
    return {"status": "disconnected"}


@app.post("/api/auth/reset")
async def reset_youtube_local_state() -> dict[str, Any]:
    settings = youtube_service.reset_local_state()
    return {
        "status": "reset",
        "settings": settings,
    }


@app.get("/api/settings/youtube")
async def get_youtube_settings() -> dict[str, Any]:
    return youtube_service.settings_snapshot()


@app.post("/api/settings/youtube")
async def save_youtube_settings(payload: YouTubeSettingsPayload) -> dict[str, Any]:
    try:
        return youtube_service.set_runtime_client_config(
            client_id=payload.client_id,
            client_secret=payload.client_secret,
        )
    except YouTubeConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/settings/app")
async def get_app_settings() -> dict[str, Any]:
    return app_settings.snapshot()


@app.post("/api/settings/app")
async def save_app_settings(payload: AppSettingsPayload) -> dict[str, Any]:
    try:
        return app_settings.update(public_app_url=payload.public_app_url)
    except AppSettingsError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/settings/app/public-url/quick-tunnel")
async def create_quick_tunnel_public_url(request: Request) -> dict[str, Any]:
    local_origin = _local_origin_for_tunnel(request)
    try:
        public_app_url = quick_tunnel_manager.ensure_started(local_url=local_origin)
        return app_settings.update(public_app_url=public_app_url)
    except (QuickTunnelError, AppSettingsError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/library")
async def get_library(request: Request) -> dict[str, Any]:
    status = youtube_service.session_status()
    library_error: str | None = None
    remote_files: list[Any] | None = None

    if status.connected:
        try:
            remote_files = youtube_service.list_files()
        except YouTubeSessionExpiredError:
            youtube_service.disconnect()
            status = youtube_service.session_status()
            library_error = "Your YouTube session expired. Connect YouTube again."
        except YouTubeError as exc:
            library_error = str(exc)

    index_snapshot = library_index.snapshot(remote_files)
    files = []
    for remote_file in remote_files or []:
        entry = index_snapshot.files.get(remote_file.video_id)
        files.append(_serialize_library_file(remote_file, entry))

    return {
        "configured": status.configured,
        "connected": status.connected,
        "channel_title": status.channel_title,
        "privacy_status": status.privacy_status,
        "connect_url": str(request.url_for("youtube_connect")),
        "files": files,
        "folders": [folder.to_dict() for folder in index_snapshot.folders],
        "index_recovered": index_snapshot.recovered,
        "error": library_error,
    }


@app.post("/api/library/folders")
async def create_library_folder(payload: FolderCreatePayload) -> dict[str, Any]:
    try:
        folder = library_index.create_folder(
            name=payload.name,
            parent_id=(payload.parent_id or ROOT_FOLDER_ID).strip() or ROOT_FOLDER_ID,
        )
    except FolderNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (FolderConflictError, LibraryIndexError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "created",
        "folder": folder.to_dict(),
    }


@app.patch("/api/library/folders/{folder_id}")
async def update_library_folder(folder_id: str, payload: FolderUpdatePayload) -> dict[str, Any]:
    if not payload.model_fields_set:
        raise HTTPException(status_code=400, detail="Nothing to update.")

    try:
        folder = library_index.update_folder(
            folder_id,
            name=payload.name if "name" in payload.model_fields_set else UNSET,
            parent_id=payload.parent_id if "parent_id" in payload.model_fields_set else UNSET,
        )
    except FolderNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (FolderConflictError, LibraryIndexError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "updated",
        "folder": folder.to_dict(),
    }


@app.delete("/api/library/folders/{folder_id}")
async def delete_library_folder(folder_id: str) -> dict[str, Any]:
    try:
        result = library_index.delete_folder(folder_id)
    except FolderNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except LibraryIndexError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "deleted",
        "result": result.to_dict(),
    }


@app.patch("/api/library/files/{video_id}")
async def update_library_file(video_id: str, payload: FileUpdatePayload) -> dict[str, Any]:
    if not payload.model_fields_set:
        raise HTTPException(status_code=400, detail="Nothing to update.")

    _ensure_local_file_entry(video_id)

    try:
        entry = library_index.update_file(
            video_id,
            folder_id=payload.folder_id if "folder_id" in payload.model_fields_set else UNSET,
            display_name=payload.display_name if "display_name" in payload.model_fields_set else UNSET,
        )
    except FolderNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileEntryNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except LibraryIndexError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "updated",
        "file": entry.to_dict(),
    }


@app.delete("/api/library/files/{video_id}")
async def delete_library_file(video_id: str) -> dict[str, Any]:
    youtube_status = youtube_service.session_status()
    if not youtube_status.configured:
        raise HTTPException(
            status_code=503,
            detail="Save a YouTube client ID and client secret first.",
        )
    if not youtube_status.connected:
        raise HTTPException(status_code=409, detail="Connect YouTube before deleting files.")

    try:
        _ensure_local_file_entry(video_id)
        youtube_service.delete_video(video_id)
        entry = library_index.delete_file(video_id)
    except YouTubeSessionExpiredError as exc:
        youtube_service.disconnect()
        raise HTTPException(status_code=409, detail="Your YouTube session expired. Connect YouTube again.") from exc
    except YouTubeAuthError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileEntryNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except YouTubeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "status": "deleted",
        "file": entry.to_dict(),
    }


@app.post("/api/library/files/{video_id}/share")
async def create_library_file_share(video_id: str, payload: ShareCreatePayload, background_tasks: BackgroundTasks) -> dict[str, Any]:
    settings_snapshot = app_settings.snapshot()
    public_app_url = str(settings_snapshot.get("public_app_url") or "")
    if not public_app_url:
        raise HTTPException(status_code=400, detail="Create or save a Public App URL in Settings before sharing files.")

    try:
        normalized_key = validate_numeric_key(payload.key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    youtube_status = youtube_service.session_status()
    if not youtube_status.configured:
        raise HTTPException(
            status_code=503,
            detail="Save a YouTube client ID and client secret first.",
        )
    if not youtube_status.connected:
        raise HTTPException(status_code=409, detail="Connect YouTube before sharing files.")

    try:
        _ensure_local_file_entry(video_id)
        remote_file = youtube_service.get_file(video_id)
    except YouTubeSessionExpiredError as exc:
        youtube_service.disconnect()
        raise HTTPException(status_code=409, detail="Your YouTube session expired. Connect YouTube again.") from exc
    except (YouTubeError, FileEntryNotFoundError) as exc:
        raise HTTPException(status_code=404, detail="File not found.") from exc

    display_name = library_index.resolve_download_name(video_id, remote_file.original_filename)
    share = share_store.create_or_replace(
        video_id=video_id,
        display_name=display_name,
        original_filename=remote_file.original_filename,
        original_size=remote_file.original_size,
        media_type=remote_file.media_type,
    )
    _cleanup_stale_share_artifacts()
    job = jobs.create("share_prepare")
    jobs.update(
        job.job_id,
        metadata={
            "share_token": share.token,
            "share_url": _build_share_url(public_app_url, share.token),
            "display_filename": share.display_name,
        },
    )
    background_tasks.add_task(_run_share_prepare_job, job.job_id, share, normalized_key, public_app_url)
    return {"job_id": job.job_id, "status": job.status}


@app.post("/api/files")
async def start_remote_upload(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    key: str = Form(...),
    folder_id: str = Form(ROOT_FOLDER_ID),
) -> dict[str, str]:
    original_filename = sanitize_filename(file.filename, fallback="upload.bin")

    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    youtube_status = youtube_service.session_status()
    if not youtube_status.configured:
        raise HTTPException(
            status_code=503,
            detail="Save a YouTube client ID and client secret first.",
        )
    if not youtube_status.connected:
        raise HTTPException(status_code=409, detail="Connect YouTube before uploading files.")
    if not library_index.folder_exists(folder_id):
        raise HTTPException(status_code=404, detail="Folder not found.")

    media_type = file.content_type or guess_media_type(original_filename)
    job = jobs.create("youtube_upload")
    source_path = await _write_incoming_upload(job.job_id, file, original_filename)
    background_tasks.add_task(
        _run_remote_upload_job,
        job.job_id,
        source_path,
        original_filename,
        media_type,
        normalized_key,
        folder_id,
    )
    return {"job_id": job.job_id, "status": job.status}


@app.post("/api/files/{video_id}/download")
async def start_remote_download(
    video_id: str,
    background_tasks: BackgroundTasks,
    key: str = Form(...),
) -> dict[str, str]:
    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    youtube_status = youtube_service.session_status()
    if not youtube_status.configured:
        raise HTTPException(
            status_code=503,
            detail="Save a YouTube client ID and client secret first.",
        )
    if not youtube_status.connected:
        raise HTTPException(status_code=409, detail="Connect YouTube before downloading files.")

    job = jobs.create("youtube_download")
    background_tasks.add_task(_run_remote_download_job, job.job_id, video_id, normalized_key)
    return {"job_id": job.job_id, "status": job.status}


@app.post("/api/encode")
async def start_encode(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    key: str = Form(...),
    debug_artifacts: bool = Form(False),
) -> dict[str, str]:
    original_filename = sanitize_filename(file.filename, fallback="upload.bin")
    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job = jobs.create("encode")
    source_path = await _write_incoming_upload(job.job_id, file, original_filename)
    media_type = file.content_type or guess_media_type(original_filename)
    background_tasks.add_task(
        _run_encode_job,
        job.job_id,
        source_path,
        original_filename,
        media_type,
        normalized_key,
        debug_artifacts,
    )
    return {"job_id": job.job_id, "status": job.status}


@app.post("/api/decode")
async def start_decode(background_tasks: BackgroundTasks, file: UploadFile = File(...), key: str = Form(...)) -> dict[str, str]:
    original_filename = sanitize_filename(file.filename, fallback="video.mkv")
    extension = Path(original_filename).suffix.lower()
    if extension not in ALLOWED_DECODE_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only .webm and .mkv files generated by this app can be decoded.")
    try:
        normalized_key = validate_numeric_key(key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    job = jobs.create("decode")
    source_path = await _write_incoming_upload(
        job.job_id,
        file,
        original_filename,
        max_bytes=MAX_DECODE_UPLOAD_SIZE,
        oversize_detail="Video exceeds the decode upload limit.",
    )
    background_tasks.add_task(_run_decode_job, job.job_id, source_path, normalized_key)
    return {"job_id": job.job_id, "status": job.status}


@app.get("/api/jobs/{job_id}")
async def get_job(request: Request, job_id: str) -> dict[str, Any]:
    try:
        job = jobs.snapshot(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Job not found.") from exc
    return _serialize_job(request, job)


@app.get("/api/library/shares")
async def list_library_shares() -> dict[str, Any]:
    public_app_url = str(app_settings.snapshot().get("public_app_url") or "")
    return {
        "shares": [_serialize_owner_share(share, public_app_url=public_app_url) for share in share_store.list_records()],
    }


@app.post("/api/library/shares/{token}/revoke")
async def revoke_library_share(token: str) -> dict[str, Any]:
    try:
        share = share_store.revoke(token)
    except ShareAccessError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    _cleanup_share_artifact(share)
    public_app_url = str(app_settings.snapshot().get("public_app_url") or "")
    refreshed_share = share_store.get(token) or share
    return {
        "status": "revoked",
        "share": _serialize_owner_share(refreshed_share, public_app_url=public_app_url),
    }


@app.post("/api/library/shares/{token}/extend")
async def extend_library_share(token: str, payload: ShareCreatePayload, background_tasks: BackgroundTasks) -> dict[str, Any]:
    try:
        normalized_key = validate_numeric_key(payload.key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    youtube_status = youtube_service.session_status()
    if not youtube_status.configured:
        raise HTTPException(
            status_code=503,
            detail="Save a YouTube client ID and client secret first.",
        )
    if not youtube_status.connected:
        raise HTTPException(status_code=409, detail="Connect YouTube before extending links.")

    try:
        share, restore_point = share_store.reopen_used(token)
    except ShareAccessError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    _cleanup_stale_share_artifacts()
    public_app_url = str(app_settings.snapshot().get("public_app_url") or "")
    job = jobs.create("share_extend")
    jobs.update(
        job.job_id,
        metadata={
            "share_token": share.token,
            "share_url": _build_share_url(public_app_url, share.token),
            "display_filename": share.display_name,
        },
    )
    background_tasks.add_task(_run_share_prepare_job, job.job_id, share, normalized_key, public_app_url, restore_point=restore_point)
    return {
        "job_id": job.job_id,
        "status": job.status,
        "share": _serialize_owner_share(share, public_app_url=public_app_url),
    }


@app.get("/api/shares/{token}/download", name="share_download")
async def download_shared_file(request: Request, token: str) -> FileResponse:
    try:
        share = share_store.require_ready(token)
    except ShareAccessError as exc:
        _cleanup_share_artifact_if_unavailable(token)
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    candidate = _share_artifact_path(share)
    if candidate is None:
        raise HTTPException(status_code=404, detail="File not ready.")

    media_type = str(share.media_type or mimetypes.guess_type(candidate.name)[0] or "application/octet-stream")
    download_name = str(share.display_name or share.original_filename or candidate.name)
    try:
        claimed_share = share_store.claim_download(
            token,
            ip_address=_resolve_client_ip(request),
            user_agent=request.headers.get("user-agent"),
        )
    except ShareAccessError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    return FileResponse(
        candidate,
        media_type=media_type,
        filename=download_name,
        headers={"Cache-Control": "no-store"},
        background=BackgroundTask(_cleanup_share_artifact, claimed_share),
    )


@app.get("/api/artifacts/{job_id}/{artifact_path:path}", name="artifact_file")
async def get_artifact(job_id: str, artifact_path: str) -> FileResponse:
    candidate = _job_artifact_path(job_id, artifact_path)
    if candidate is None:
        raise HTTPException(status_code=404, detail="Artifact not found.")

    media_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
    return FileResponse(
        candidate,
        media_type=media_type,
        headers={"Cache-Control": "no-store"},
        background=BackgroundTask(_cleanup_job_artifact_after_download, job_id, artifact_path),
    )


def _run_remote_upload_job(
    job_id: str,
    source_path: Path,
    original_filename: str,
    media_type: str,
    key: str,
    folder_id: str,
) -> None:
    from .codec.service import CodecError, encode_file_for_youtube_upload

    job_dir = JOBS_DIR / job_id
    jobs.update(job_id, status="running", progress=1, message="Preparing encrypted upload.")

    try:
        result = encode_file_for_youtube_upload(
            source_path=source_path,
            original_filename=original_filename,
            media_type=media_type,
            key=key,
            job_dir=job_dir,
            progress=lambda progress, message: jobs.update(
                job_id,
                status="running",
                progress=_scaled_progress(progress, start=4, end=79),
                message=message,
            ),
        )
        remote_file = youtube_service.upload_video(
            video_path=result.video_path,
            manifest=result.manifest,
            progress=lambda progress, message: jobs.update(
                job_id,
                status="running",
                progress=progress,
                message=message,
            ),
        )
        local_entry = library_index.ensure_file(remote_file.video_id, folder_id=folder_id)
    except CodecError as exc:
        jobs.update(job_id, status="failed", error=str(exc), message="Upload failed.")
        return
    except YouTubeError as exc:
        jobs.update(job_id, status="failed", error=str(exc), message="Upload failed.")
        return
    except Exception as exc:  # pragma: no cover - defensive fallback
        jobs.update(job_id, status="failed", error=f"Unexpected error: {exc}", message="Upload failed.")
        return
    finally:
        _cleanup_job_dir(job_id)

    manifest = result.manifest
    remote_payload = _serialize_library_file(remote_file, local_entry)
    jobs.update(
        job_id,
        status="completed",
        progress=100,
        message="Uploaded encrypted video to YouTube.",
        metadata={
            "original_filename": manifest["original_filename"],
            "media_type": manifest["media_type"],
            "original_size": manifest["original_size"],
            "stored_size": manifest["stored_size"],
            "sha256": manifest["sha256"],
            "crc32": manifest["crc32"],
            "frame_count": manifest["total_frames"],
            "fps": manifest["fps"],
            "privacy_status": remote_file.privacy_status,
            "remote_file": remote_payload,
        },
        artifacts={
            "youtube_watch": remote_file.watch_url,
            "youtube_studio": remote_file.studio_url,
        },
    )


def _run_remote_download_job(job_id: str, video_id: str, key: str) -> None:
    from .codec.service import CodecError, decode_video

    job_dir = JOBS_DIR / job_id
    jobs.update(job_id, status="running", progress=2, message="Locating file in YouTube library.")

    try:
        remote_file = youtube_service.get_file(video_id)
        source_dir = job_dir / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        jobs.update(job_id, status="running", progress=15, message="Downloading video from YouTube.")
        downloaded_video_path = youtube_service.download_video(video_id=video_id, output_dir=source_dir)
        result = decode_video(
            video_path=downloaded_video_path,
            job_dir=job_dir,
            key=key,
            allow_duplicate_frame_chunks=True,
            progress=lambda progress, message: jobs.update(
                job_id,
                status="running",
                progress=_scaled_progress(progress, start=35, end=99),
                message=message,
            ),
        )
    except CodecError as exc:
        jobs.update(job_id, status="failed", error=str(exc), message="Download failed.")
        _cleanup_job_dir(job_id)
        return
    except YouTubeError as exc:
        jobs.update(job_id, status="failed", error=str(exc), message="Download failed.")
        _cleanup_job_dir(job_id)
        return
    except Exception as exc:  # pragma: no cover - defensive fallback
        jobs.update(job_id, status="failed", error=f"Unexpected error: {exc}", message="Download failed.")
        _cleanup_job_dir(job_id)
        return

    manifest = result.manifest
    display_filename = library_index.resolve_download_name(video_id, str(manifest["original_filename"]))
    recovered_relpath = str(result.restored_path.relative_to(job_dir))
    _prune_job_dir(job_id, keep_relpaths=[recovered_relpath])
    jobs.update(
        job_id,
        status="completed",
        progress=100,
        message="Recovered original file from YouTube."
        if result.integrity_ok
        else "Recovered file from YouTube, but integrity validation failed.",
        metadata={
            "original_filename": manifest["original_filename"],
            "display_filename": display_filename,
            "media_type": manifest["media_type"],
            "original_size": manifest["original_size"],
            "compressed": manifest["compressed"],
            "sha256": manifest["sha256"],
            "crc32": manifest["crc32"],
            "frame_count": manifest["total_frames"],
            "fps": manifest["fps"],
            "integrity_ok": result.integrity_ok,
            "remote_file": remote_file.to_dict(),
        },
        artifacts={
            "recovered_file": recovered_relpath,
        },
    )


def _run_share_prepare_job(
    job_id: str,
    share: ShareRecord,
    key: str,
    public_app_url: str,
    *,
    restore_point: ShareReuseRestorePoint | None = None,
) -> None:
    from .codec.service import CodecError, decode_video

    job_dir = JOBS_DIR / job_id

    def fail_share_prepare(error: str) -> None:
        if restore_point is None:
            _revoke_share_safely(share.token)
        else:
            share_store.restore_reopened(share.token, restore_point)
        shutil.rmtree(job_dir, ignore_errors=True)
        jobs.update(job_id, status="failed", error=error, message="Share failed.")

    jobs.update(
        job_id,
        status="running",
        progress=2,
        message="Preparing shared file.",
        metadata={
            "share_token": share.token,
            "display_filename": share.display_name,
            "media_type": share.media_type,
            "share_url": _build_share_url(public_app_url, share.token),
        },
    )

    try:
        source_dir = job_dir / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        jobs.update(job_id, status="running", progress=15, message="Downloading file.")
        downloaded_video_path = youtube_service.download_video(video_id=share.video_id, output_dir=source_dir)
        result = decode_video(
            video_path=downloaded_video_path,
            job_dir=job_dir,
            key=key,
            allow_duplicate_frame_chunks=True,
            progress=lambda progress, message: jobs.update(
                job_id,
                status="running",
                progress=_scaled_progress(progress, start=35, end=99),
                message=message,
            ),
        )
    except CodecError:
        logger.warning("Share preparation decode failed for token %s.", share.token, exc_info=True)
        fail_share_prepare("Could not prepare this share.")
        return
    except YouTubeError:
        logger.warning("Share preparation YouTube download failed for token %s.", share.token, exc_info=True)
        fail_share_prepare("Could not prepare this share.")
        return
    except Exception:
        logger.warning("Unexpected share preparation failure for token %s.", share.token, exc_info=True)
        fail_share_prepare("Could not prepare this share.")
        return

    if not result.integrity_ok:
        fail_share_prepare("That key does not unlock this file.")
        return

    manifest = result.manifest
    display_filename = share.display_name or str(manifest["original_filename"])
    artifact_path = _prepare_share_artifact(share.token, result.restored_path, display_filename)
    try:
        prepared_share = share_store.mark_prepared(
            share.token,
            artifact_relpath=str(artifact_path.relative_to(DATA_DIR)),
        )
    except ShareAccessError:
        logger.info("Discarding prepared artifact for unavailable share token %s.", share.token)
        shutil.rmtree(artifact_path.parent, ignore_errors=True)
        shutil.rmtree(job_dir, ignore_errors=True)
        jobs.update(job_id, status="failed", error="This share link is no longer available.", message="Share failed.")
        return
    shutil.rmtree(job_dir, ignore_errors=True)
    jobs.update(
        job_id,
        status="completed",
        progress=100,
        message="Share is ready.",
        metadata={
            "share_token": prepared_share.token,
            "display_filename": prepared_share.display_name,
            "original_filename": manifest["original_filename"],
            "media_type": manifest["media_type"],
            "original_size": manifest["original_size"],
            "compressed": manifest["compressed"],
            "sha256": manifest["sha256"],
            "crc32": manifest["crc32"],
            "frame_count": manifest["total_frames"],
            "fps": manifest["fps"],
            "share_url": _build_share_url(public_app_url, prepared_share.token),
        },
    )


def _run_encode_job(
    job_id: str,
    source_path: Path,
    original_filename: str,
    media_type: str,
    key: str,
    debug_artifacts: bool,
) -> None:
    from .codec.service import CodecError, encode_file

    jobs.update(job_id, status="running", progress=1, message="Starting encode.")
    job_dir = JOBS_DIR / job_id

    try:
        result = encode_file(
            source_path=source_path,
            original_filename=original_filename,
            media_type=media_type,
            key=key,
            job_dir=job_dir,
            debug_artifacts=debug_artifacts,
            progress=lambda progress, message: jobs.update(
                job_id,
                status="running",
                progress=progress,
                message=message,
            ),
        )
    except CodecError as exc:
        jobs.update(job_id, status="failed", error=str(exc), message="Encode failed.")
        _cleanup_job_dir(job_id)
        return
    except Exception as exc:  # pragma: no cover - defensive fallback
        jobs.update(job_id, status="failed", error=f"Unexpected error: {exc}", message="Encode failed.")
        _cleanup_job_dir(job_id)
        return

    manifest = result.manifest
    metadata: dict[str, Any] = {
        "original_filename": manifest["original_filename"],
        "media_type": manifest["media_type"],
        "original_size": manifest["original_size"],
        "stored_size": manifest["stored_size"],
        "compressed": manifest["compressed"],
        "sha256": manifest["sha256"],
        "crc32": manifest["crc32"],
        "frame_count": manifest["total_frames"],
        "fps": manifest["fps"],
        "duration_seconds": round(int(manifest["total_frames"]) / int(manifest["fps"]), 2),
    }
    if result.frame_paths:
        metadata["frame_files"] = [
            {
                "index": index + 1,
                "name": path.name,
            }
            for index, path in enumerate(result.frame_paths)
        ]

    artifacts: dict[str, Any] = {
        "video": str(result.video_path.relative_to(job_dir)),
    }
    if result.frames_zip_path is not None:
        artifacts["frames_zip"] = str(result.frames_zip_path.relative_to(job_dir))
    _prune_job_dir(job_id, keep_relpaths=list(artifacts.values()))

    jobs.update(
        job_id,
        status="completed",
        progress=100,
        message="Encode complete.",
        metadata=metadata,
        artifacts=artifacts,
    )


def _run_decode_job(job_id: str, source_path: Path, key: str) -> None:
    from .codec.service import CodecError, decode_video

    jobs.update(job_id, status="running", progress=1, message="Starting decode.")
    job_dir = JOBS_DIR / job_id

    try:
        result = decode_video(
            video_path=source_path,
            job_dir=job_dir,
            key=key,
            progress=lambda progress, message: jobs.update(
                job_id,
                status="running",
                progress=progress,
                message=message,
            ),
        )
    except CodecError as exc:
        jobs.update(job_id, status="failed", error=str(exc), message="Decode failed.")
        _cleanup_job_dir(job_id)
        return
    except Exception as exc:  # pragma: no cover - defensive fallback
        jobs.update(job_id, status="failed", error=f"Unexpected error: {exc}", message="Decode failed.")
        _cleanup_job_dir(job_id)
        return

    manifest = result.manifest
    recovered_relpath = str(result.restored_path.relative_to(job_dir))
    _prune_job_dir(job_id, keep_relpaths=[recovered_relpath])
    jobs.update(
        job_id,
        status="completed",
        progress=100,
        message="Decode complete." if result.integrity_ok else "Decode complete, but integrity validation failed.",
        metadata={
            "original_filename": manifest["original_filename"],
            "media_type": manifest["media_type"],
            "original_size": manifest["original_size"],
            "compressed": manifest["compressed"],
            "sha256": manifest["sha256"],
            "crc32": manifest["crc32"],
            "frame_count": manifest["total_frames"],
            "fps": manifest["fps"],
            "integrity_ok": result.integrity_ok,
        },
        artifacts={
            "recovered_file": recovered_relpath,
        },
    )


def _serialize_library_file(remote_file: Any, entry: Any) -> dict[str, Any]:
    payload = remote_file.to_dict()
    display_name_override = entry.display_name if entry is not None else None
    payload["folder_id"] = entry.folder_id if entry is not None else ROOT_FOLDER_ID
    payload["display_name"] = display_name_override or payload["original_filename"]
    payload["display_name_override"] = display_name_override
    return payload


def _ensure_local_file_entry(video_id: str) -> None:
    if library_index.has_file(video_id):
        return

    if not youtube_service.session_status().connected:
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        youtube_service.get_file(video_id)
    except YouTubeError as exc:
        raise HTTPException(status_code=404, detail="File not found.") from exc

    library_index.ensure_file(video_id)


async def _write_incoming_upload(
    job_id: str,
    upload: UploadFile,
    original_filename: str,
    *,
    max_bytes: int | None = None,
    oversize_detail: str = "Upload exceeds the allowed size.",
) -> Path:
    job_dir = JOBS_DIR / job_id
    source_dir = job_dir / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_name = sanitize_filename(original_filename)
    source_path = source_dir / source_name

    total_bytes = 0
    try:
        with source_path.open("wb") as handle:
            while True:
                chunk = await upload.read(UPLOAD_STREAM_CHUNK_SIZE)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if max_bytes is not None and total_bytes > max_bytes:
                    raise HTTPException(status_code=413, detail=oversize_detail)
                handle.write(chunk)
    except Exception:
        source_path.unlink(missing_ok=True)
        shutil.rmtree(job_dir, ignore_errors=True)
        raise
    finally:
        await upload.close()

    return source_path


def _serialize_job(request: Request, job: JobRecord) -> dict[str, Any]:
    payload = asdict(job)
    payload["created_at"] = int(job.created_at)
    payload["updated_at"] = int(job.updated_at)
    payload["artifacts"] = {
        name: _artifact_url(request, job.job_id, artifact_path)
        for name, artifact_path in job.artifacts.items()
    }

    frame_files = job.metadata.get("frame_files")
    if isinstance(frame_files, list) and frame_files and all(isinstance(item, str) for item in frame_files):
        payload["metadata"]["frame_files"] = [
            {
                "index": index + 1,
                "name": Path(relative_path).name,
                "url": str(request.url_for("artifact_file", job_id=job.job_id, artifact_path=relative_path)),
            }
            for index, relative_path in enumerate(frame_files)
        ]

    return payload


def _artifact_url(request: Request, job_id: str, artifact_path: Any) -> Any:
    if not isinstance(artifact_path, str):
        return artifact_path
    if artifact_path.startswith(("http://", "https://")):
        return artifact_path
    return str(request.url_for("artifact_file", job_id=job_id, artifact_path=artifact_path))


def _job_artifact_path(job_id: str, artifact_path: str) -> Path | None:
    if not artifact_path:
        return None

    job_dir = (JOBS_DIR / job_id).resolve()
    if not job_dir.exists():
        return None

    candidate = (job_dir / artifact_path).resolve()
    try:
        candidate.relative_to(job_dir)
    except ValueError:
        return None

    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


def _cleanup_job_dir(job_id: str) -> None:
    shutil.rmtree(JOBS_DIR / job_id, ignore_errors=True)


def _prune_job_dir(job_id: str, *, keep_relpaths: list[str]) -> None:
    job_dir = JOBS_DIR / job_id
    if not job_dir.exists():
        return

    job_root = job_dir.resolve()
    keep_paths: set[Path] = set()
    keep_dirs: set[Path] = {job_root}

    for relpath in keep_relpaths:
        if not relpath:
            continue
        candidate = (job_dir / relpath).resolve()
        try:
            candidate.relative_to(job_root)
        except ValueError:
            continue
        if not candidate.exists():
            continue
        keep_paths.add(candidate)
        for parent in candidate.parents:
            keep_dirs.add(parent.resolve())
            if parent.resolve() == job_root:
                break

    if not keep_paths:
        _cleanup_job_dir(job_id)
        return

    for candidate in sorted(job_dir.rglob("*"), key=lambda path: len(path.parts), reverse=True):
        resolved = candidate.resolve()
        if resolved in keep_paths or resolved in keep_dirs:
            continue
        if candidate.is_dir():
            shutil.rmtree(candidate, ignore_errors=True)
            continue
        candidate.unlink(missing_ok=True)


def _cleanup_job_artifact_after_download(job_id: str, artifact_path: str) -> None:
    candidate = _job_artifact_path(job_id, artifact_path)
    if candidate is not None:
        candidate.unlink(missing_ok=True)
    _cleanup_empty_job_dirs(job_id)


def _cleanup_empty_job_dirs(job_id: str) -> None:
    job_dir = JOBS_DIR / job_id
    if not job_dir.exists():
        return

    for directory in sorted((path for path in job_dir.rglob("*") if path.is_dir()), key=lambda path: len(path.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            continue

    try:
        job_dir.rmdir()
    except OSError:
        return


def _build_share_url(public_app_url: str, token: str) -> str:
    return f"{public_app_url.rstrip('/')}/s/{token}"


def _public_share_payload(token: str) -> dict[str, Any]:
    share = share_store.get(token)
    if share is None:
        return {
            "token": token,
            "status": "invalid",
            "message": "This share link is invalid.",
            "share": None,
        }

    status = share_store.get_status(token)
    return {
        "token": token,
        "status": status,
        "message": _share_status_message(status),
        "share": _serialize_public_share(share),
    }


def _serialize_owner_share(share: ShareRecord, *, public_app_url: str) -> dict[str, Any]:
    status = share_store.get_status(share.token)
    return {
        "token": share.token,
        "video_id": share.video_id,
        "display_name": share.display_name,
        "original_filename": share.original_filename,
        "original_size": share.original_size,
        "media_type": share.media_type,
        "created_at": share.created_at,
        "expires_at": share.expires_at,
        "prepared_at": share.prepared_at,
        "revoked_at": share.revoked_at,
        "status": status,
        "share_url": _build_share_url(public_app_url, share.token) if public_app_url else None,
        "download_count": len(share.downloads),
        "downloads": [_serialize_owner_download(download) for download in reversed(share.downloads)],
    }


def _serialize_owner_download(download: Any) -> dict[str, Any]:
    return {
        "downloaded_at": download.downloaded_at,
        "ip_address": download.ip_address,
        "user_agent": download.user_agent,
    }


def _serialize_public_share(share: ShareRecord) -> dict[str, Any]:
    return {
        "display_name": share.display_name,
        "original_filename": share.original_filename,
        "original_size": share.original_size,
        "media_type": share.media_type,
        "expires_at": share.expires_at,
        "download_url": f"/api/shares/{share.token}/download" if share.artifact_relpath else None,
        "download_count": len(share.downloads),
    }


def _share_status_message(status: str) -> str:
    if status == "pending":
        return "This file is preparing."
    if status == "used":
        return "This share link has already been used."
    if status == "expired":
        return "This share link has expired."
    if status == "revoked":
        return "This share link is no longer available."
    if status == "invalid":
        return "This share link is invalid."
    return ""


def _json_for_html(payload: dict[str, Any]) -> str:
    return (
        json.dumps(payload, separators=(",", ":"), sort_keys=True)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )


def _render_html_file(path: Path, *, asset_paths: list[Path], replacements: dict[str, str] | None = None) -> str:
    html = path.read_text(encoding="utf-8")
    html = html.replace("__ASSET_VERSION__", str(_asset_version(asset_paths)))
    for placeholder, value in (replacements or {}).items():
        html = html.replace(placeholder, value)
    return html


def _asset_version(paths: list[Path]) -> int:
    return max(int(path.stat().st_mtime) for path in paths if path.exists())


def _local_origin_for_tunnel(request: Request) -> str:
    current_base = str(request.base_url).rstrip("/")
    parsed = urlsplit(current_base)
    if parsed.hostname in {"127.0.0.1", "localhost"} and parsed.scheme in {"http", "https"}:
        return current_base
    port = parsed.port or 8000
    scheme = parsed.scheme if parsed.scheme in {"http", "https"} else "http"
    return f"{scheme}://127.0.0.1:{port}"


def _resolve_client_ip(request: Request) -> str:
    for header_name in ("cf-connecting-ip", "true-client-ip", "x-real-ip", "x-forwarded-for", "forwarded"):
        raw_value = str(request.headers.get(header_name) or "").strip()
        if not raw_value:
            continue
        if header_name == "forwarded":
            candidate = _forwarded_for_ip(raw_value)
            if candidate:
                return candidate
            continue
        if header_name == "x-forwarded-for":
            return _normalize_ip_candidate(_first_forwarded_value(raw_value))
        return _normalize_ip_candidate(raw_value)
    if request.client and request.client.host:
        return _normalize_ip_candidate(request.client.host)
    return "unknown"


def _first_forwarded_value(value: str) -> str:
    first = value.split(",", 1)[0].strip()
    return first or "unknown"


def _forwarded_for_ip(value: str) -> str | None:
    for part in value.split(";"):
        key, _, raw = part.strip().partition("=")
        if key.lower() != "for" or not raw:
            continue
        return _normalize_ip_candidate(raw.strip().strip('"'))
    return None


def _normalize_ip_candidate(value: str) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return "unknown"
    if candidate.startswith("[") and "]" in candidate:
        return candidate[1:candidate.index("]")]
    if candidate.count(":") == 1 and "." in candidate:
        host, _, port = candidate.partition(":")
        if port.isdigit():
            return host
    return candidate


def _request_uses_public_host(request: Request) -> bool:
    public_app_url = str(app_settings.snapshot().get("public_app_url") or "")
    if not public_app_url:
        return False
    current_host = (request.headers.get("host") or "").strip().lower()
    public_host = (urlsplit(public_app_url).netloc or "").strip().lower()
    return bool(current_host and public_host and current_host == public_host)


def _is_public_share_path(path: str) -> bool:
    return path.startswith("/s/") or path.startswith("/api/shares/") or path in {
        "/static/share.css",
        "/static/share.js",
        "/static/favicon.svg",
        "/favicon.ico",
    }


def _prepare_share_artifact(token: str, restored_path: Path, display_name: str) -> Path:
    artifact_dir = SHARE_ARTIFACTS_DIR / token / uuid.uuid4().hex
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_name = sanitize_filename(display_name, fallback=restored_path.name)
    artifact_path = artifact_dir / artifact_name
    shutil.move(str(restored_path), artifact_path)
    return artifact_path


def _share_artifact_path(share: ShareRecord) -> Path | None:
    artifact_relpath = str(share.artifact_relpath or "")
    if not artifact_relpath:
        return None
    candidate = (DATA_DIR / artifact_relpath).resolve()
    try:
        candidate.relative_to(DATA_DIR.resolve())
    except ValueError:
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate


def _revoke_share_safely(token: str) -> None:
    share = share_store.get(token)
    if share is None:
        return
    try:
        share_store.revoke(token)
    except ShareAccessError:
        return
    _cleanup_share_artifact(share)


def _cleanup_share_artifact(share: ShareRecord) -> None:
    artifact_path = _share_artifact_path(share)
    if artifact_path is None:
        return
    artifact_dir = artifact_path.parent
    shutil.rmtree(artifact_dir, ignore_errors=True)
    _cleanup_empty_share_artifact_dirs(artifact_dir.parent)
    try:
        share_store.clear_artifact(share.token)
    except ShareAccessError:
        return


def _cleanup_share_artifact_if_unavailable(token: str) -> None:
    share = share_store.get(token)
    if share is None:
        return
    if share_store.get_status(token) not in {"used", "expired", "revoked"}:
        return
    _cleanup_share_artifact(share)


def _cleanup_stale_share_artifacts() -> None:
    for share in share_store.artifact_cleanup_candidates():
        _cleanup_share_artifact(share)


def _cleanup_empty_share_artifact_dirs(start_dir: Path) -> None:
    root = SHARE_ARTIFACTS_DIR.resolve()
    current = start_dir.resolve()
    try:
        current.relative_to(root)
    except ValueError:
        return

    while current != root:
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _scaled_progress(progress: int, *, start: int, end: int) -> int:
    clamped = max(0, min(100, progress))
    return start + round(((end - start) * clamped) / 100)


def _cleanup_expired_jobs() -> None:
    if not JOBS_DIR.exists():
        return
    for job_dir in JOBS_DIR.iterdir():
        if not job_dir.is_dir():
            continue
        shutil.rmtree(job_dir, ignore_errors=True)
