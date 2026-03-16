# Local State

StorageX is local-first. The app keeps only two categories of persistent state on disk:

- `data/youtube-auth.json`
- `data/library-index.json`

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

## What does not persist

The encryption key does not persist to disk. If you save it in the UI, it is stored only in browser `sessionStorage` for the current browser session.

That means:

- app restarts keep YouTube credentials
- new browser sessions do not keep the encryption key
- sharing decryption capability is intentionally out of scope
