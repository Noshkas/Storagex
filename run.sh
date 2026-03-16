#!/bin/zsh

set -euo pipefail

cd "$(dirname "$0")"

PYTHON_BIN="/opt/homebrew/bin/python3.13"
APP_HOST="${APP_HOST:-127.0.0.1}"
APP_PORT="${APP_PORT:-8000}"
PROJECT_ROOT="$(pwd -P)"
PROJECT_HASH="$(printf "%s" "$PROJECT_ROOT" | shasum -a 256 | cut -c1-12)"
APP_ENV_DIR="${APP_ENV_DIR:-$HOME/.cache/storagex-venv-$PROJECT_HASH}"
APP_REQUIRED_FILES=(
  "pyproject.toml"
  "uv.lock"
  "server/app.py"
  "static/index.html"
  "static/app.js"
  "static/styles.css"
)

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python 3.13 was not found at $PYTHON_BIN" >&2
  exit 1
fi

for required_file in "${APP_REQUIRED_FILES[@]}"; do
  if [[ ! -e "$required_file" ]]; then
    echo "Required project file is missing: $required_file" >&2
    exit 1
  fi
  if ls -ldO "$required_file" 2>/dev/null | grep -q 'dataless'; then
    echo "Project file is offloaded by iCloud and not fully local: $required_file" >&2
    echo "In Finder, right-click the storagex folder and choose 'Download Now', or move the project out of iCloud-managed Documents." >&2
    exit 1
  fi
done

EXISTING_PID="$(lsof -tiTCP:"$APP_PORT" -sTCP:LISTEN 2>/dev/null || true)"
if [[ -n "$EXISTING_PID" ]]; then
  EXISTING_COMMAND="$(ps -p "$EXISTING_PID" -o command= 2>/dev/null || true)"
  if [[ "$EXISTING_COMMAND" == *"uvicorn server.app:app"* ]]; then
    kill "$EXISTING_PID" 2>/dev/null || true
    sleep 1
    if ps -p "$EXISTING_PID" >/dev/null 2>&1; then
      kill -9 "$EXISTING_PID" 2>/dev/null || true
    fi
  else
    echo "Port $APP_PORT is already in use by another process:" >&2
    echo "$EXISTING_COMMAND" >&2
    exit 1
  fi
fi

mkdir -p "$APP_ENV_DIR"

UV_PROJECT_ENVIRONMENT="$APP_ENV_DIR" uv sync --python "$PYTHON_BIN" --dev

exec "$APP_ENV_DIR/bin/uvicorn" server.app:app --host "$APP_HOST" --port "$APP_PORT"
