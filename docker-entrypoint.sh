#!/usr/bin/env sh
set -eu

if [ -x ".venv/bin/python" ]; then
  PYTHON_BIN=.venv/bin/python
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN=python
else
  PYTHON_BIN=python3
fi

if [ -x ".venv/bin/uvicorn" ]; then
  UVICORN_BIN=.venv/bin/uvicorn
else
  UVICORN_BIN=uvicorn
fi

echo "[entrypoint] initializing database schema"
"$PYTHON_BIN" -m app.init_db --skip-compile --attached-only

echo "[entrypoint] starting application"
exec "$UVICORN_BIN" app.main:app --host "${APP_HOST:-0.0.0.0}" --port "${APP_PORT:-8000}"
