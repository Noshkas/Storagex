# StorageX

StorageX is an experimental local-first tool that turns supported files into encrypted bit-video uploads, stores them in your own YouTube account, and later recovers the original bytes.

It is built for technical users who want to run the whole flow locally on their own machine with their own Google OAuth client.

![StorageX screenshot](docs/assets/storagex-home.png)

## Why this exists

- Encrypt a supported file with a 24-digit key.
- Encode the encrypted payload into a reversible bit-grid video format.
- Upload a YouTube-compatible archive video to your own channel.
- Recover the original file later by downloading the YouTube video and decoding it locally.

## Status

StorageX is an experimental tool, not a guaranteed backup service.

Use it when you are comfortable with:

- bringing your own Google OAuth credentials
- running a local FastAPI app
- keeping local auth state on your own machine
- YouTube being a third-party dependency that can fail or change

## Requirements

- macOS or another machine that can run Python 3.13 and `ffmpeg`
- a local browser for Google OAuth
- your own Google Cloud OAuth client with YouTube Data API v3 enabled
- the same YouTube account signed into Chrome or Safari on this Mac if recovery needs browser cookies

## Quickstart

1. Install dependencies and start the app:

```bash
cd /path/to/storagex
./run.sh
```

2. Open `http://127.0.0.1:8000`.
3. Open `Settings`.
4. Paste your Google OAuth `Client ID` and `Client Secret`.
5. Click `Connect` and finish the Google / YouTube sign-in flow.
6. Upload a supported file.
7. Use `Download` later to recover the original file.

## Google OAuth setup

Create your own OAuth client in Google Cloud:

- enable `YouTube Data API v3`
- create an OAuth client of type `Web application`
- add this origin:
  - `http://127.0.0.1:8000`
- add this redirect URI:
  - `http://127.0.0.1:8000/auth/youtube/callback`
- if the app is in Google testing mode, add your Google account as a test user

StorageX expects you to bring your own client credentials. It does not ship with shared Google credentials.

## Supported files and limits

- Upload inputs:
  - `.txt`
  - `.mp4`
  - `.pdf`
  - `.jpeg`
  - `.jpg`
  - `.png`
- Remote upload size limit: `10 MB`
- Local decode upload size limit: `250 MB`
- Encryption key format: exactly `24` digits

## Local state and privacy

StorageX stores local runtime state in this folder:

- `data/youtube-auth.json`

That file can contain:

- your saved OAuth client ID
- your saved OAuth client secret
- your Google refresh token / access token state
- pending OAuth PKCE state during sign-in

Behavior notes:

- the file is local-only and written with `0600` permissions when possible
- the UI encryption key is stored only in browser session storage if you save it there
- local recovery may use browser cookies from Chrome or Safari when `yt-dlp` needs them to access your video

If you want to wipe local YouTube state, use `Settings` → `Reset local YouTube setup`.

## Optional environment configuration

The UI setup is the primary path. Environment variables are still supported for technical users:

- `YOUTUBE_CLIENT_ID`
- `YOUTUBE_CLIENT_SECRET`
- `YOUTUBE_PRIVACY_STATUS`
- `YOUTUBE_DOWNLOAD_BROWSER`
- `YOUTUBE_DOWNLOAD_COOKIEFILE`
- `YOUTUBE_DOWNLOAD_JS_RUNTIME`

## Common failure cases

- `redirect_uri_mismatch`
  - your Google OAuth redirect URI does not exactly match `http://127.0.0.1:8000/auth/youtube/callback`
- `access_denied`
  - your Google account is not listed as a test user for the OAuth app
- `Google rejected the client secret`
  - the client ID and client secret do not belong to the same OAuth client
- `Could not access that YouTube video`
  - sign into the same YouTube account in Chrome or Safari on this Mac and try recovery again
- `The saved YouTube session expired`
  - reconnect the account in the app

## Development

Run the test suite:

```bash
cd /path/to/storagex
uv run pytest -q
```

Manual release smoke steps live in [docs/release-smoke-test.md](docs/release-smoke-test.md).

## License

[MIT](LICENSE)
