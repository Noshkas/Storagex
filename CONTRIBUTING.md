# Contributing

## Development setup

Start the app locally:

```bash
cd /path/to/storagex
./run.sh
```

Run tests:

```bash
cd /path/to/storagex
uv run pytest -q
```

## Contribution guidelines

- keep StorageX local-first
- do not add new storage backends in the first open-source release work
- prefer tightening failure handling, docs, tests, and setup over adding surface area
- do not commit local runtime state such as `data/youtube-auth.json` or generated job artifacts
- keep the web UI in light mode

## Pull requests

- explain the user-facing change
- include or update tests when behavior changes
- keep README and setup instructions accurate if the workflow changes
- call out any Google OAuth, YouTube, or `yt-dlp` behavior changes explicitly
