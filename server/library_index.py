from __future__ import annotations

import json
import os
import re
import threading
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .codec.constants import DATA_DIR

LIBRARY_INDEX_PATH = DATA_DIR / "library-index.json"
INDEX_VERSION = 1
ROOT_FOLDER_ID = "root"
ROOT_FOLDER_NAME = "All files"
INVALID_NAME_PATTERN = re.compile("[\\\\/\x00-\x1F]")
MAX_NAME_LENGTH = 255
UNSET = object()


class LibraryIndexError(ValueError):
    pass


class FolderNotFoundError(LibraryIndexError):
    pass


class FolderConflictError(LibraryIndexError):
    pass


class FileEntryNotFoundError(LibraryIndexError):
    pass


@dataclass(slots=True)
class FolderDeleteResult:
    deleted_folder_ids: list[str]
    moved_file_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FolderRecord:
    id: str
    name: str
    parent_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FileEntry:
    video_id: str
    folder_id: str
    display_name: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class LibraryIndexSnapshot:
    folders: list[FolderRecord]
    files: dict[str, FileEntry]
    recovered: bool


def default_index_state() -> dict[str, Any]:
    return {
        "version": INDEX_VERSION,
        "folders": {
            ROOT_FOLDER_ID: {
                "id": ROOT_FOLDER_ID,
                "name": ROOT_FOLDER_NAME,
                "parent_id": None,
            }
        },
        "files": {},
    }


class LibraryIndexStore:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or LIBRARY_INDEX_PATH
        self._lock = threading.Lock()

    def snapshot(self, remote_files: list[Any] | None = None) -> LibraryIndexSnapshot:
        with self._lock:
            state, recovered = self._load_state()
            changed = False

            if remote_files is not None:
                remote_ids = {self._video_id(file) for file in remote_files if self._video_id(file)}
                for video_id in remote_ids:
                    if video_id not in state["files"]:
                        state["files"][video_id] = {
                            "folder_id": ROOT_FOLDER_ID,
                            "display_name": None,
                        }
                        changed = True

                stale_video_ids = [video_id for video_id in state["files"] if video_id not in remote_ids]
                for video_id in stale_video_ids:
                    del state["files"][video_id]
                    changed = True

            if changed or recovered:
                self._persist_state(state)

            return self._snapshot_from_state(state, recovered=recovered)

    def create_folder(self, *, name: str, parent_id: str = ROOT_FOLDER_ID) -> FolderRecord:
        with self._lock:
            state, recovered = self._load_state()
            if parent_id not in state["folders"]:
                raise FolderNotFoundError("Folder not found.")

            normalized_name = self._normalize_folder_name(name)
            self._ensure_unique_sibling_name(
                folders=state["folders"],
                parent_id=parent_id,
                name=normalized_name,
            )

            folder_id = uuid.uuid4().hex
            state["folders"][folder_id] = {
                "id": folder_id,
                "name": normalized_name,
                "parent_id": parent_id,
            }
            self._persist_state(state)
            return self._folder_record(state["folders"][folder_id])

    def update_folder(
        self,
        folder_id: str,
        *,
        name: str | object = UNSET,
        parent_id: str | object = UNSET,
    ) -> FolderRecord:
        with self._lock:
            state, recovered = self._load_state()
            folders = state["folders"]
            folder = folders.get(folder_id)
            if folder is None:
                raise FolderNotFoundError("Folder not found.")

            if folder_id == ROOT_FOLDER_ID and (name is not UNSET or parent_id is not UNSET):
                raise LibraryIndexError("The root folder cannot be changed.")

            next_name = folder["name"]
            next_parent_id = folder["parent_id"]

            if name is not UNSET:
                next_name = self._normalize_folder_name(str(name))

            if parent_id is not UNSET:
                next_parent_id = str(parent_id or "").strip() or ROOT_FOLDER_ID
                if next_parent_id not in folders:
                    raise FolderNotFoundError("Folder not found.")
                if next_parent_id == folder_id or self._is_descendant(
                    folders=folders,
                    folder_id=next_parent_id,
                    ancestor_id=folder_id,
                ):
                    raise LibraryIndexError("Folders cannot be moved into themselves.")

            self._ensure_unique_sibling_name(
                folders=folders,
                parent_id=next_parent_id,
                name=next_name,
                ignore_folder_id=folder_id,
            )

            folder["name"] = next_name
            folder["parent_id"] = next_parent_id
            self._persist_state(state)
            return self._folder_record(folder)

    def delete_folder(self, folder_id: str) -> FolderDeleteResult:
        with self._lock:
            state, recovered = self._load_state()
            folders = state["folders"]
            if folder_id == ROOT_FOLDER_ID:
                raise LibraryIndexError("The root folder cannot be deleted.")

            if folder_id not in folders:
                raise FolderNotFoundError("Folder not found.")

            deleted_folder_ids = sorted(self._collect_descendant_folder_ids(folders=folders, folder_id=folder_id))
            for deleted_folder_id in deleted_folder_ids:
                folders.pop(deleted_folder_id, None)

            moved_file_ids: list[str] = []
            for video_id, entry in state["files"].items():
                if entry["folder_id"] in deleted_folder_ids:
                    entry["folder_id"] = ROOT_FOLDER_ID
                    moved_file_ids.append(video_id)

            self._persist_state(state)
            return FolderDeleteResult(
                deleted_folder_ids=deleted_folder_ids,
                moved_file_ids=sorted(moved_file_ids),
            )

    def update_file(
        self,
        video_id: str,
        *,
        folder_id: str | object = UNSET,
        display_name: str | object = UNSET,
    ) -> FileEntry:
        with self._lock:
            state, recovered = self._load_state()
            entry = state["files"].get(video_id)
            if entry is None:
                raise FileEntryNotFoundError("File not found.")

            if folder_id is not UNSET:
                next_folder_id = str(folder_id or "").strip() or ROOT_FOLDER_ID
                if next_folder_id not in state["folders"]:
                    raise FolderNotFoundError("Folder not found.")
                entry["folder_id"] = next_folder_id

            if display_name is not UNSET:
                entry["display_name"] = self._normalize_display_name(display_name)

            self._persist_state(state)
            return self._file_entry(video_id, entry)

    def ensure_file(
        self,
        video_id: str,
        *,
        folder_id: str = ROOT_FOLDER_ID,
        display_name: str | None = None,
    ) -> FileEntry:
        with self._lock:
            state, recovered = self._load_state()
            if folder_id not in state["folders"]:
                folder_id = ROOT_FOLDER_ID

            entry = state["files"].get(video_id)
            if entry is None:
                entry = {
                    "folder_id": folder_id,
                    "display_name": self._normalize_display_name(display_name),
                }
                state["files"][video_id] = entry
                self._persist_state(state)
            return self._file_entry(video_id, entry)

    def has_file(self, video_id: str) -> bool:
        with self._lock:
            state, _recovered = self._load_state()
            return video_id in state["files"]

    def delete_file(self, video_id: str) -> FileEntry:
        with self._lock:
            state, _recovered = self._load_state()
            entry = state["files"].pop(video_id, None)
            if entry is None:
                raise FileEntryNotFoundError("File not found.")
            self._persist_state(state)
            return self._file_entry(video_id, entry)

    def folder_exists(self, folder_id: str) -> bool:
        with self._lock:
            state, _recovered = self._load_state()
            return folder_id in state["folders"]

    def resolve_download_name(self, video_id: str, original_filename: str) -> str:
        with self._lock:
            state, _recovered = self._load_state()
            entry = state["files"].get(video_id)
            if not entry:
                return original_filename
            return str(entry.get("display_name") or original_filename)

    def _snapshot_from_state(self, state: dict[str, Any], *, recovered: bool) -> LibraryIndexSnapshot:
        folders = [
            self._folder_record(folder)
            for folder in state["folders"].values()
        ]
        folders.sort(key=lambda folder: (folder.id != ROOT_FOLDER_ID, folder.name.casefold(), folder.id))
        files = {
            video_id: self._file_entry(video_id, entry)
            for video_id, entry in state["files"].items()
        }
        return LibraryIndexSnapshot(folders=folders, files=files, recovered=recovered)

    def _load_state(self) -> tuple[dict[str, Any], bool]:
        if not self._path.exists():
            return default_index_state(), False

        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
            state = self._validate_state(payload)
        except Exception:
            state = default_index_state()
            self._persist_state(state)
            return state, True

        return state, False

    def _validate_state(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise LibraryIndexError("Index is invalid.")
        if payload.get("version") != INDEX_VERSION:
            raise LibraryIndexError("Index version is invalid.")

        folders_payload = payload.get("folders")
        if not isinstance(folders_payload, dict):
            raise LibraryIndexError("Folder index is invalid.")

        folders: dict[str, dict[str, Any]] = {}
        for folder_id, folder in folders_payload.items():
            if not isinstance(folder_id, str) or not isinstance(folder, dict):
                raise LibraryIndexError("Folder index is invalid.")
            if folder_id == ROOT_FOLDER_ID:
                folders[ROOT_FOLDER_ID] = {
                    "id": ROOT_FOLDER_ID,
                    "name": ROOT_FOLDER_NAME,
                    "parent_id": None,
                }
                continue

            parent_id = folder.get("parent_id")
            if not isinstance(parent_id, str) or not parent_id.strip():
                raise LibraryIndexError("Folder index is invalid.")
            folders[folder_id] = {
                "id": folder_id,
                "name": self._normalize_folder_name(folder.get("name")),
                "parent_id": parent_id.strip(),
            }

        if ROOT_FOLDER_ID not in folders:
            raise LibraryIndexError("Root folder is missing.")

        for folder_id, folder in folders.items():
            parent_id = folder["parent_id"]
            if folder_id == ROOT_FOLDER_ID:
                continue
            if parent_id not in folders:
                raise LibraryIndexError("Folder tree is invalid.")
            if self._is_descendant(folders=folders, folder_id=parent_id, ancestor_id=folder_id):
                raise LibraryIndexError("Folder tree contains a cycle.")

        folders_by_parent: dict[str | None, list[dict[str, Any]]] = {}
        for folder in folders.values():
            folders_by_parent.setdefault(folder["parent_id"], []).append(folder)
        for siblings in folders_by_parent.values():
            names: set[str] = set()
            for folder in siblings:
                key = str(folder["name"]).casefold()
                if key in names:
                    raise FolderConflictError("Folder names must be unique within a parent.")
                names.add(key)

        files_payload = payload.get("files")
        if not isinstance(files_payload, dict):
            raise LibraryIndexError("File index is invalid.")

        files: dict[str, dict[str, Any]] = {}
        for video_id, entry in files_payload.items():
            if not isinstance(video_id, str) or not isinstance(entry, dict):
                raise LibraryIndexError("File index is invalid.")
            folder_id = str(entry.get("folder_id") or "").strip() or ROOT_FOLDER_ID
            if folder_id not in folders:
                raise LibraryIndexError("File index is invalid.")
            files[video_id] = {
                "folder_id": folder_id,
                "display_name": self._normalize_display_name(entry.get("display_name")),
            }

        return {
            "version": INDEX_VERSION,
            "folders": folders,
            "files": files,
        }

    def _persist_state(self, state: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temp_path, self._path)
        try:
            os.chmod(self._path, 0o600)
        except OSError:
            pass

    def _normalize_folder_name(self, value: Any) -> str:
        if not isinstance(value, str):
            raise LibraryIndexError("Folder name is required.")
        name = value.strip()
        if not name:
            raise LibraryIndexError("Folder name is required.")
        if INVALID_NAME_PATTERN.search(name):
            raise LibraryIndexError("Names cannot include slashes or control characters.")
        return name[:MAX_NAME_LENGTH]

    def _normalize_display_name(self, value: Any) -> str | None:
        if value is None:
            return None
        name = str(value).strip()
        if not name:
            return None
        if INVALID_NAME_PATTERN.search(name):
            raise LibraryIndexError("Names cannot include slashes or control characters.")
        return name[:MAX_NAME_LENGTH]

    def _ensure_unique_sibling_name(
        self,
        *,
        folders: dict[str, dict[str, Any]],
        parent_id: str | None,
        name: str,
        ignore_folder_id: str | None = None,
    ) -> None:
        normalized_name = name.casefold()
        for sibling_id, folder in folders.items():
            if sibling_id == ignore_folder_id:
                continue
            if folder["parent_id"] != parent_id:
                continue
            if str(folder["name"]).casefold() == normalized_name:
                raise FolderConflictError("Folder names must be unique within a parent.")

    def _is_descendant(
        self,
        *,
        folders: dict[str, dict[str, Any]],
        folder_id: str,
        ancestor_id: str,
    ) -> bool:
        current_id: str | None = folder_id
        seen: set[str] = set()
        while current_id is not None:
            if current_id == ancestor_id:
                return True
            if current_id in seen:
                return True
            seen.add(current_id)
            folder = folders.get(current_id)
            if folder is None:
                return False
            current_id = folder["parent_id"]
        return False

    def _collect_descendant_folder_ids(
        self,
        *,
        folders: dict[str, dict[str, Any]],
        folder_id: str,
    ) -> set[str]:
        collected = {folder_id}
        changed = True
        while changed:
            changed = False
            for candidate_id, folder in folders.items():
                if candidate_id in collected:
                    continue
                if folder["parent_id"] in collected:
                    collected.add(candidate_id)
                    changed = True
        return collected

    def _video_id(self, file: Any) -> str:
        return str(getattr(file, "video_id", "") or "")

    def _folder_record(self, folder: dict[str, Any]) -> FolderRecord:
        return FolderRecord(
            id=str(folder["id"]),
            name=str(folder["name"]),
            parent_id=str(folder["parent_id"]) if folder["parent_id"] is not None else None,
        )

    def _file_entry(self, video_id: str, entry: dict[str, Any]) -> FileEntry:
        return FileEntry(
            video_id=video_id,
            folder_id=str(entry["folder_id"]),
            display_name=str(entry["display_name"]) if entry.get("display_name") else None,
        )
