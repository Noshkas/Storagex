# Local State

StorageX is local-first. The app keeps these persistent files on disk:

- `data/youtube-auth.json`
- `data/library-index.json`
- `data/app-settings.json`
- `data/shares.json`
- `data/share-artifacts/`

Encode and download job workdirs are not persistent state.
StorageX now places them in the OS temp area and removes them after the final artifact download or on app restart.

## `data/youtube-auth.json`

This file is local to the current machine and can contain:

- the saved YouTube OAuth client ID
- the saved YouTube OAuth client secret
- Google OAuth credential state such as refresh/access token data
- pending OAuth state during sign-in

StorageX reloads this file on restart so you do not need to paste the YouTube client credentials again every time.

## `data/library-index.json`

This file is also local to the current machine and stores the library organization layer:

- the virtual folder tree
- per-file folder placement
- per-file local display-name overrides

This index is not synced to YouTube. A different machine can still access the same uploaded files, but they will appear unorganized until that machine creates its own local index.

If the file is missing, corrupt, or deleted, StorageX rebuilds a clean root-only index and still shows discovered YouTube files in `All files`.

## `data/app-settings.json`

This file stores app-level local settings such as:

- the saved `Public App URL` used to build copied share links

If you use the `Create public URL` button in `Settings`, the returned temporary public root URL is saved here.

## `data/shares.json`

This file stores public share tokens for single-file sharing:

- the share token
- the referenced `video_id`
- the shared file display name, size, and media type
- `created_at`
- `expires_at`
- `prepared_at`
- `artifact_relpath`
- `used_at`
- `revoked_at`
- per-download audit entries with IP address, timestamp, and user agent

The owner enters the file key when the share is created. Recipients do not receive or enter the 24-digit key.

## `data/share-artifacts/`

This directory stores temporary prepared downloads for active public share links.

Those prepared files exist only so recipients can download without the encryption key. StorageX removes them after the first successful shared download, or when a share is replaced, revoked, or cleaned up after expiry.

## What does not persist

The encryption key does not persist to disk. If you save it in the UI, it is stored only in browser `sessionStorage` for the current browser session.

That means:

- app restarts keep YouTube credentials
- new browser sessions do not keep the encryption key
- public share links never persist the 24-digit key
- share metadata persists locally so links survive app restarts until they expire or are revoked, even after a link has been used
- download audit metadata persists locally so the owner can review who accessed a link
- active public shares may temporarily persist a prepared decrypted file on disk until the share is used, expires, is replaced, or is revoked
