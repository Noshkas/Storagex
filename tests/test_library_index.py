from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from server.library_index import (
    ROOT_FOLDER_ID,
    FileEntryNotFoundError,
    FolderConflictError,
    LibraryIndexError,
    LibraryIndexStore,
)


def test_snapshot_recovers_from_corrupt_index_and_keeps_remote_files_visible(tmp_path) -> None:
    index_path = tmp_path / "library-index.json"
    index_path.write_text("{broken", encoding="utf-8")
    store = LibraryIndexStore(index_path)

    snapshot = store.snapshot([SimpleNamespace(video_id="video-1")])

    assert snapshot.recovered is True
    assert [folder.id for folder in snapshot.folders] == [ROOT_FOLDER_ID]
    assert snapshot.files["video-1"].folder_id == ROOT_FOLDER_ID

    persisted = json.loads(index_path.read_text(encoding="utf-8"))
    assert persisted["folders"][ROOT_FOLDER_ID]["name"] == "All files"
    assert persisted["files"]["video-1"]["folder_id"] == ROOT_FOLDER_ID


def test_folder_and_file_assignments_persist_across_store_restart(tmp_path) -> None:
    index_path = tmp_path / "library-index.json"
    store = LibraryIndexStore(index_path)
    folder = store.create_folder(name="Receipts")
    store.ensure_file("video-1")
    store.update_file("video-1", folder_id=folder.id, display_name="March_Receipt.pdf")

    reloaded = LibraryIndexStore(index_path)
    snapshot = reloaded.snapshot([SimpleNamespace(video_id="video-1")])

    assert any(item.id == folder.id and item.name == "Receipts" for item in snapshot.folders)
    assert snapshot.files["video-1"].folder_id == folder.id
    assert snapshot.files["video-1"].display_name == "March_Receipt.pdf"
    assert reloaded.resolve_download_name("video-1", "receipt.pdf") == "March_Receipt.pdf"


def test_deleting_local_index_restores_root_defaults(tmp_path) -> None:
    index_path = tmp_path / "library-index.json"
    store = LibraryIndexStore(index_path)
    folder = store.create_folder(name="Archive")
    store.ensure_file("video-1", folder_id=folder.id, display_name="Archived_File.txt")

    index_path.unlink()

    reloaded = LibraryIndexStore(index_path)
    snapshot = reloaded.snapshot([SimpleNamespace(video_id="video-1")])

    assert [folder.id for folder in snapshot.folders] == [ROOT_FOLDER_ID]
    assert snapshot.files["video-1"].folder_id == ROOT_FOLDER_ID
    assert snapshot.files["video-1"].display_name is None
    assert reloaded.resolve_download_name("video-1", "hello.txt") == "hello.txt"


def test_folder_updates_reject_cycles_and_duplicate_names(tmp_path) -> None:
    store = LibraryIndexStore(tmp_path / "library-index.json")
    parent = store.create_folder(name="Work")
    child = store.create_folder(name="Reports", parent_id=parent.id)

    with pytest.raises(LibraryIndexError, match="cannot be changed"):
        store.update_folder(ROOT_FOLDER_ID, name="Renamed")

    with pytest.raises(LibraryIndexError, match="Folders cannot be moved into themselves"):
        store.update_folder(parent.id, parent_id=child.id)

    with pytest.raises(FolderConflictError, match="unique"):
        store.create_folder(name="work")

    with pytest.raises(LibraryIndexError, match="cannot be deleted"):
        store.delete_folder(ROOT_FOLDER_ID)


def test_delete_folder_removes_subtree_and_moves_files_to_root(tmp_path) -> None:
    store = LibraryIndexStore(tmp_path / "library-index.json")
    parent = store.create_folder(name="Work")
    child = store.create_folder(name="Reports", parent_id=parent.id)
    store.ensure_file("video-1", folder_id=parent.id)
    store.ensure_file("video-2", folder_id=child.id)

    result = store.delete_folder(parent.id)
    snapshot = store.snapshot([SimpleNamespace(video_id="video-1"), SimpleNamespace(video_id="video-2")])

    assert set(result.deleted_folder_ids) == {parent.id, child.id}
    assert set(result.moved_file_ids) == {"video-1", "video-2"}
    assert [folder.id for folder in snapshot.folders] == [ROOT_FOLDER_ID]
    assert snapshot.files["video-1"].folder_id == ROOT_FOLDER_ID
    assert snapshot.files["video-2"].folder_id == ROOT_FOLDER_ID


def test_update_file_rejects_missing_entries(tmp_path) -> None:
    store = LibraryIndexStore(tmp_path / "library-index.json")

    with pytest.raises(FileEntryNotFoundError, match="File not found"):
        store.update_file("missing-video", display_name="Anything.txt")


def test_delete_file_removes_local_entry(tmp_path) -> None:
    store = LibraryIndexStore(tmp_path / "library-index.json")
    store.ensure_file("video-1", display_name="Receipt.pdf")

    deleted = store.delete_file("video-1")
    snapshot = store.snapshot([])

    assert deleted.video_id == "video-1"
    assert snapshot.files == {}
    assert store.resolve_download_name("video-1", "hello.txt") == "hello.txt"
