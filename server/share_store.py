from __future__ import annotations

import json
import os
import secrets
import threading
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .codec.constants import DATA_DIR

SHARES_PATH = DATA_DIR / "shares.json"
SHARE_ARTIFACTS_DIR = DATA_DIR / "share-artifacts"
SHARES_VERSION = 3
SHARE_TTL_SECONDS = 24 * 60 * 60


class ShareStoreError(Exception):
    pass


class ShareAccessError(ShareStoreError):
    def __init__(self, detail: str, *, status_code: int) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


@dataclass(slots=True)
class ShareDownloadRecord:
    downloaded_at: str
    ip_address: str
    user_agent: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ShareRecord:
    token: str
    video_id: str
    display_name: str
    original_filename: str
    original_size: int
    media_type: str
    created_at: str
    expires_at: str
    prepared_at: str | None = None
    artifact_relpath: str | None = None
    used_at: str | None = None
    revoked_at: str | None = None
    downloads: list[ShareDownloadRecord] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["downloads"] = [download.to_dict() for download in self.downloads]
        return payload


@dataclass(slots=True)
class ShareReuseRestorePoint:
    expires_at: str
    prepared_at: str | None = None
    artifact_relpath: str | None = None
    used_at: str | None = None


class ShareStore:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or SHARES_PATH
        self._lock = threading.Lock()

    def create_or_replace(
        self,
        *,
        video_id: str,
        display_name: str,
        original_filename: str,
        original_size: int,
        media_type: str,
    ) -> ShareRecord:
        created_at = _now_iso()
        expires_at = _at_offset_iso(seconds=SHARE_TTL_SECONDS)

        with self._lock:
            state = self._load_state()
            for payload in state["shares"].values():
                if payload.get("video_id") != video_id:
                    continue
                status = self._status_from_payload(payload)
                if status in {"used", "revoked", "expired"}:
                    continue
                payload["revoked_at"] = created_at

            token = secrets.token_urlsafe(24)
            record = ShareRecord(
                token=token,
                video_id=video_id,
                display_name=display_name,
                original_filename=original_filename,
                original_size=original_size,
                media_type=media_type,
                created_at=created_at,
                expires_at=expires_at,
            )
            state["shares"][token] = record.to_dict()
            self._persist_state(state)
            return record

    def get(self, token: str) -> ShareRecord | None:
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                return None
            return self._record_from_payload(payload)

    def get_status(self, token: str) -> str:
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                return "invalid"
            return self._status_from_payload(payload)

    def list_records(self) -> list[ShareRecord]:
        with self._lock:
            state = self._load_state()
            shares = [self._record_from_payload(payload) for payload in state["shares"].values()]
        shares.sort(key=lambda share: _parse_iso(share.created_at), reverse=True)
        return shares

    def revoke(self, token: str) -> ShareRecord:
        revoked_at = _now_iso()
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                raise ShareAccessError("This share link is invalid.", status_code=404)
            payload["revoked_at"] = revoked_at
            self._persist_state(state)
            return self._record_from_payload(payload)

    def mark_prepared(self, token: str, *, artifact_relpath: str) -> ShareRecord:
        prepared_at = _now_iso()
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                raise ShareAccessError("This share link is invalid.", status_code=404)
            status = self._status_from_payload(payload)
            if status != "pending":
                raise ShareAccessError("This share link is no longer available.", status_code=410)
            payload["prepared_at"] = prepared_at
            payload["artifact_relpath"] = artifact_relpath
            self._persist_state(state)
            return self._record_from_payload(payload)

    def require_ready(self, token: str) -> ShareRecord:
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                raise ShareAccessError("This share link is invalid.", status_code=404)
            status = self._status_from_payload(payload)
            if status == "used":
                raise ShareAccessError("This share link has already been used.", status_code=410)
            if status == "expired":
                raise ShareAccessError("This share link has expired.", status_code=410)
            if status == "revoked":
                raise ShareAccessError("This share link is no longer available.", status_code=410)
            if status == "pending":
                raise ShareAccessError("This file is not ready yet.", status_code=409)
            return self._record_from_payload(payload)

    def claim_download(self, token: str, *, ip_address: str, user_agent: str | None = None) -> ShareRecord:
        normalized_ip = str(ip_address or "").strip() or "unknown"
        normalized_user_agent = str(user_agent or "").strip() or None
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                raise ShareAccessError("This share link is invalid.", status_code=404)
            status = self._status_from_payload(payload)
            if status != "active":
                raise ShareAccessError("This share link is not available.", status_code=410)
            downloads = payload.setdefault("downloads", [])
            downloads.append(
                ShareDownloadRecord(
                    downloaded_at=_now_iso(),
                    ip_address=normalized_ip,
                    user_agent=normalized_user_agent,
                ).to_dict()
            )
            payload["used_at"] = _now_iso()
            self._persist_state(state)
            return self._record_from_payload(payload)

    def reopen_used(self, token: str) -> tuple[ShareRecord, ShareReuseRestorePoint]:
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                raise ShareAccessError("This share link is invalid.", status_code=404)

            status = self._status_from_payload(payload)
            if status == "pending":
                raise ShareAccessError("This share link is already preparing.", status_code=409)
            if status == "active":
                raise ShareAccessError("This share link is already active.", status_code=409)
            if status == "expired":
                raise ShareAccessError("This share link has expired.", status_code=410)
            if status == "revoked":
                raise ShareAccessError("This share link is no longer available.", status_code=410)
            if status != "used":
                raise ShareAccessError("This share link cannot be extended.", status_code=409)

            restore_point = ShareReuseRestorePoint(
                expires_at=str(payload["expires_at"]),
                prepared_at=str(payload["prepared_at"]) if payload.get("prepared_at") else None,
                artifact_relpath=str(payload["artifact_relpath"]) if payload.get("artifact_relpath") else None,
                used_at=str(payload["used_at"]) if payload.get("used_at") else None,
            )
            payload["expires_at"] = _at_offset_iso(seconds=SHARE_TTL_SECONDS)
            payload["used_at"] = None
            payload["prepared_at"] = None
            payload["artifact_relpath"] = None
            self._persist_state(state)
            return self._record_from_payload(payload), restore_point

    def restore_reopened(self, token: str, restore_point: ShareReuseRestorePoint) -> ShareRecord | None:
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                return None
            if payload.get("revoked_at"):
                return self._record_from_payload(payload)
            if self._status_from_payload(payload) != "pending":
                return self._record_from_payload(payload)

            artifact_relpath = restore_point.artifact_relpath
            if artifact_relpath:
                candidate = (DATA_DIR / artifact_relpath).resolve()
                try:
                    candidate.relative_to(DATA_DIR.resolve())
                except ValueError:
                    artifact_relpath = None
                else:
                    if not candidate.exists() or not candidate.is_file():
                        artifact_relpath = None

            payload["expires_at"] = restore_point.expires_at
            payload["prepared_at"] = restore_point.prepared_at
            payload["artifact_relpath"] = artifact_relpath
            payload["used_at"] = restore_point.used_at
            self._persist_state(state)
            return self._record_from_payload(payload)

    def clear_artifact(self, token: str) -> ShareRecord:
        with self._lock:
            state = self._load_state()
            payload = state["shares"].get(token)
            if payload is None:
                raise ShareAccessError("This share link is invalid.", status_code=404)
            payload["artifact_relpath"] = None
            self._persist_state(state)
            return self._record_from_payload(payload)

    def artifact_cleanup_candidates(self) -> list[ShareRecord]:
        with self._lock:
            state = self._load_state()
            candidates: list[ShareRecord] = []
            for payload in state["shares"].values():
                if not payload.get("artifact_relpath"):
                    continue
                status = self._status_from_payload(payload)
                if status == "active":
                    continue
                candidates.append(self._record_from_payload(payload))
            return candidates

    def _load_state(self) -> dict[str, Any]:
        if not self._path.exists():
            return self._default_state()

        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            state = self._default_state()
            self._persist_state(state)
            return state

        try:
            return self._validate_state(payload)
        except ShareStoreError:
            state = self._default_state()
            self._persist_state(state)
            return state

    def _validate_state(self, payload: Any) -> dict[str, Any]:
        migrated = self._migrate_state(payload)
        shares_payload = migrated.get("shares")
        if not isinstance(shares_payload, dict):
            raise ShareStoreError("Share store is invalid.")

        shares: dict[str, dict[str, Any]] = {}
        for token, record in shares_payload.items():
            if not isinstance(token, str) or not isinstance(record, dict):
                raise ShareStoreError("Share store is invalid.")
            share = self._record_from_payload(record)
            shares[token] = share.to_dict()

        return {
            "version": SHARES_VERSION,
            "shares": shares,
        }

    def _migrate_state(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ShareStoreError("Share store is invalid.")

        version = payload.get("version")
        shares_payload = payload.get("shares")
        if not isinstance(shares_payload, dict):
            raise ShareStoreError("Share store is invalid.")

        if version == SHARES_VERSION:
            return payload

        if version == 2:
            migrated_shares: dict[str, dict[str, Any]] = {}
            for token, record in shares_payload.items():
                if not isinstance(token, str) or not isinstance(record, dict):
                    raise ShareStoreError("Share store is invalid.")
                migrated_record = dict(record)
                migrated_record.setdefault("downloads", [])
                migrated_shares[token] = migrated_record
            return {
                "version": SHARES_VERSION,
                "shares": migrated_shares,
            }

        raise ShareStoreError("Share store is invalid.")

    def _persist_state(self, state: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self._path)
        try:
            os.chmod(self._path, 0o600)
        except OSError:
            pass

    def _record_from_payload(self, payload: dict[str, Any]) -> ShareRecord:
        try:
            return ShareRecord(
                token=str(payload["token"]),
                video_id=str(payload["video_id"]),
                display_name=str(payload["display_name"]),
                original_filename=str(payload["original_filename"]),
                original_size=int(payload["original_size"]),
                media_type=str(payload["media_type"]),
                created_at=str(payload["created_at"]),
                expires_at=str(payload["expires_at"]),
                prepared_at=str(payload["prepared_at"]) if payload.get("prepared_at") else None,
                artifact_relpath=str(payload["artifact_relpath"]) if payload.get("artifact_relpath") else None,
                used_at=str(payload["used_at"]) if payload.get("used_at") else None,
                revoked_at=str(payload["revoked_at"]) if payload.get("revoked_at") else None,
                downloads=self._downloads_from_payload(payload.get("downloads", [])),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ShareStoreError("Share store is invalid.") from exc

    def _downloads_from_payload(self, payload: Any) -> list[ShareDownloadRecord]:
        if payload in (None, ""):
            return []
        if not isinstance(payload, list):
            raise ShareStoreError("Share store is invalid.")

        downloads: list[ShareDownloadRecord] = []
        for entry in payload:
            if not isinstance(entry, dict):
                raise ShareStoreError("Share store is invalid.")
            try:
                downloads.append(
                    ShareDownloadRecord(
                        downloaded_at=str(entry["downloaded_at"]),
                        ip_address=str(entry["ip_address"]),
                        user_agent=str(entry["user_agent"]) if entry.get("user_agent") else None,
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ShareStoreError("Share store is invalid.") from exc
        return downloads

    def _status_from_payload(self, payload: dict[str, Any]) -> str:
        if payload.get("revoked_at"):
            return "revoked"
        if payload.get("used_at"):
            return "used"
        expires_at = payload.get("expires_at")
        try:
            expires_at_dt = _parse_iso(str(expires_at))
        except ValueError as exc:
            raise ShareStoreError("Share store is invalid.") from exc
        if expires_at_dt <= datetime.now(UTC):
            return "expired"
        if not payload.get("prepared_at") or not payload.get("artifact_relpath"):
            return "pending"
        return "active"

    @staticmethod
    def _default_state() -> dict[str, Any]:
        return {
            "version": SHARES_VERSION,
            "shares": {},
        }


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _at_offset_iso(*, seconds: int) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"
    return datetime.fromisoformat(candidate)
