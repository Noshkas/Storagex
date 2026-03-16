from __future__ import annotations

import json

from server.youtube import (
    InMemoryYouTubeStore,
    PersistentYouTubeStore,
    YouTubeService,
    build_youtube_description,
    build_youtube_title,
    parse_youtube_description,
)


class DummyCredentials:
    def to_json(self) -> str:
        return json.dumps(
            {
                "token": "token",
                "refresh_token": "refresh-token",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "scopes": ["https://www.googleapis.com/auth/youtube.readonly"],
            }
        )


class FakeExecutable:
    def __init__(self, payload):
        self._payload = payload

    def execute(self):
        return self._payload


class FakePlaylistItemsApi:
    def __init__(self, pages):
        self._pages = pages

    def list(self, *, part, playlistId, maxResults, pageToken=None):
        del part, playlistId, maxResults
        key = pageToken or "__first__"
        return FakeExecutable(self._pages[key])


class FakeVideosApi:
    def list(self, *, part, id):
        del part
        items = []
        for video_id in id.split(","):
            items.append(
                {
                    "id": video_id,
                    "snippet": {
                        "title": build_youtube_title(f"{video_id}.txt"),
                        "description": build_youtube_description(
                            {
                                "app": "storagex",
                                "format_version": 2,
                                "original_filename": f"{video_id}.txt",
                                "media_type": "text/plain",
                                "original_size": 100,
                                "stored_size": 100,
                                "sha256": f"sha-{video_id}",
                                "crc32": f"crc-{video_id}",
                                "frame_count": 4,
                                "fps": 24,
                                "privacy_status": "private",
                                "uploaded_at": "2026-03-16T10:00:00Z",
                            }
                        ),
                        "thumbnails": {},
                    },
                    "status": {"privacyStatus": "private"},
                }
            )
        return FakeExecutable({"items": items})


class FakeYouTubeApi:
    def __init__(self, pages):
        self._playlist_items_api = FakePlaylistItemsApi(pages)
        self._videos_api = FakeVideosApi()

    def playlistItems(self):
        return self._playlist_items_api

    def videos(self):
        return self._videos_api


def test_youtube_description_round_trip() -> None:
    metadata = {
        "app": "storagex",
        "format_version": 2,
        "original_filename": "sample.pdf",
        "media_type": "application/pdf",
        "original_size": 1234,
        "stored_size": 1234,
        "sha256": "abc",
        "crc32": "def",
        "frame_count": 8,
        "fps": 24,
        "privacy_status": "private",
        "uploaded_at": "2026-03-16T10:00:00Z",
    }

    description = build_youtube_description(metadata)
    parsed = parse_youtube_description(description)

    assert parsed == metadata


def test_youtube_title_is_trimmed_to_youtube_limit() -> None:
    title = build_youtube_title("x" * 140 + ".txt")

    assert title.startswith("StorageX · ")
    assert len(title) <= 100


def test_persistent_youtube_store_round_trip(tmp_path) -> None:
    store_path = tmp_path / "youtube-auth.json"
    store = PersistentYouTubeStore(store_path)
    store.set_client_config("client-id", "client-secret")
    store.set_credentials(DummyCredentials())
    store.add_state("oauth-state", "http://127.0.0.1:8000/auth/youtube/callback", "verifier-123")

    reloaded = PersistentYouTubeStore(store_path)
    assert reloaded.get_client_config() == ("client-id", "client-secret")
    assert reloaded.get_credentials()["refresh_token"] == "refresh-token"
    assert reloaded.pop_state("oauth-state") == ("http://127.0.0.1:8000/auth/youtube/callback", "verifier-123")

    after_pop = PersistentYouTubeStore(store_path)
    assert after_pop.pop_state("oauth-state") is None


def test_persistent_youtube_store_reset_removes_saved_state(tmp_path) -> None:
    store_path = tmp_path / "youtube-auth.json"
    store = PersistentYouTubeStore(store_path)
    store.set_client_config("client-id", "client-secret")
    store.set_credentials(DummyCredentials())

    store.reset()

    assert store.get_client_config() == (None, None)
    assert store.get_credentials() is None
    assert not store_path.exists()


def test_list_files_fetches_entire_upload_history(monkeypatch) -> None:
    total_videos = 235
    video_ids = [f"video-{index:03d}" for index in range(total_videos)]
    pages = {
        "__first__": {
            "items": [
                {"contentDetails": {"videoId": video_id}, "snippet": {"publishedAt": "2026-03-16T10:00:00Z"}}
                for video_id in video_ids[:50]
            ],
            "nextPageToken": "page-2",
        },
        "page-2": {
            "items": [
                {"contentDetails": {"videoId": video_id}, "snippet": {"publishedAt": "2026-03-16T10:00:00Z"}}
                for video_id in video_ids[50:100]
            ],
            "nextPageToken": "page-3",
        },
        "page-3": {
            "items": [
                {"contentDetails": {"videoId": video_id}, "snippet": {"publishedAt": "2026-03-16T10:00:00Z"}}
                for video_id in video_ids[100:150]
            ],
            "nextPageToken": "page-4",
        },
        "page-4": {
            "items": [
                {"contentDetails": {"videoId": video_id}, "snippet": {"publishedAt": "2026-03-16T10:00:00Z"}}
                for video_id in video_ids[150:200]
            ],
            "nextPageToken": "page-5",
        },
        "page-5": {
            "items": [
                {"contentDetails": {"videoId": video_id}, "snippet": {"publishedAt": "2026-03-16T10:00:00Z"}}
                for video_id in video_ids[200:]
            ],
        },
    }

    service = YouTubeService(store=InMemoryYouTubeStore())
    monkeypatch.setattr(service, "_channel_info", lambda: ("Test Channel", "uploads-playlist"))
    monkeypatch.setattr(service, "_build_service", lambda: FakeYouTubeApi(pages))

    files = service.list_files()

    assert len(files) == total_videos
    assert {item.video_id for item in files} == set(video_ids)


def test_download_commands_enable_js_runtime_when_node_is_available(monkeypatch) -> None:
    service = YouTubeService(store=InMemoryYouTubeStore())
    monkeypatch.setattr(service, "_yt_dlp_executable", lambda: "/tmp/yt-dlp")
    monkeypatch.setattr("server.youtube.shutil.which", lambda name: "/opt/homebrew/bin/node" if name == "node" else None)
    monkeypatch.delenv("YOUTUBE_DOWNLOAD_COOKIEFILE", raising=False)
    monkeypatch.delenv("YOUTUBE_DOWNLOAD_BROWSER", raising=False)
    monkeypatch.delenv("YOUTUBE_DOWNLOAD_JS_RUNTIME", raising=False)

    commands = service._download_commands("https://www.youtube.com/watch?v=abc", "/tmp/out.%(ext)s")

    assert commands[0][:8] == [
        "/tmp/yt-dlp",
        "--no-playlist",
        "--no-progress",
        "--no-warnings",
        "--quiet",
        "--output",
        "/tmp/out.%(ext)s",
        "-f",
    ]
    assert "--js-runtimes" in commands[0]
    assert "node:/opt/homebrew/bin/node" in commands[0]
    assert "--remote-components" in commands[0]
    assert "ejs:github" in commands[0]
