from __future__ import annotations

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
from urllib.parse import urlencode

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .codec.constants import (
    ALLOWED_DECODE_EXTENSIONS,
    JOBS_DIR,
    JOB_TTL_SECONDS,
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


class YouTubeSettingsPayload(BaseModel):
    client_id: str
    client_secret: str


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
    _cleanup_expired_jobs()
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def index() -> HTMLResponse:
    index_path = Path("static/index.html")
    asset_paths = [
        index_path,
        Path("static/app.js"),
        Path("static/styles.css"),
        Path("static/favicon.svg"),
    ]
    asset_version = max(int(path.stat().st_mtime) for path in asset_paths if path.exists())
    html = index_path.read_text(encoding="utf-8").replace("__ASSET_VERSION__", str(asset_version))
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})


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


@app.get("/api/artifacts/{job_id}/{artifact_path:path}", name="artifact_file")
async def get_artifact(job_id: str, artifact_path: str) -> FileResponse:
    job_dir = (JOBS_DIR / job_id).resolve()
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="Job not found.")

    candidate = (job_dir / artifact_path).resolve()
    try:
        candidate.relative_to(job_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid artifact path.") from exc

    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Artifact not found.")

    media_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
    return FileResponse(candidate, media_type=media_type)


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
        shutil.rmtree(job_dir, ignore_errors=True)

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
        return
    except YouTubeError as exc:
        jobs.update(job_id, status="failed", error=str(exc), message="Download failed.")
        return
    except Exception as exc:  # pragma: no cover - defensive fallback
        jobs.update(job_id, status="failed", error=f"Unexpected error: {exc}", message="Download failed.")
        return

    manifest = result.manifest
    display_filename = library_index.resolve_download_name(video_id, str(manifest["original_filename"]))
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
            "recovered_file": str(result.restored_path.relative_to(job_dir)),
            "manifest": str(result.manifest_path.relative_to(job_dir)),
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
        return
    except Exception as exc:  # pragma: no cover - defensive fallback
        jobs.update(job_id, status="failed", error=f"Unexpected error: {exc}", message="Encode failed.")
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
        metadata["frame_files"] = [str(path.relative_to(job_dir)) for path in result.frame_paths]

    artifacts: dict[str, Any] = {
        "video": str(result.video_path.relative_to(job_dir)),
        "manifest": str(result.manifest_path.relative_to(job_dir)),
    }
    if result.frames_zip_path is not None:
        artifacts["frames_zip"] = str(result.frames_zip_path.relative_to(job_dir))

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
        return
    except Exception as exc:  # pragma: no cover - defensive fallback
        jobs.update(job_id, status="failed", error=f"Unexpected error: {exc}", message="Decode failed.")
        return

    manifest = result.manifest
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
            "recovered_file": str(result.restored_path.relative_to(job_dir)),
            "manifest": str(result.manifest_path.relative_to(job_dir)),
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
    if isinstance(frame_files, list):
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


def _scaled_progress(progress: int, *, start: int, end: int) -> int:
    clamped = max(0, min(100, progress))
    return start + round(((end - start) * clamped) / 100)


def _cleanup_expired_jobs() -> None:
    now = time.time()
    for job_dir in JOBS_DIR.iterdir():
        if not job_dir.is_dir():
            continue
        age = now - job_dir.stat().st_mtime
        if age > JOB_TTL_SECONDS:
            shutil.rmtree(job_dir, ignore_errors=True)
