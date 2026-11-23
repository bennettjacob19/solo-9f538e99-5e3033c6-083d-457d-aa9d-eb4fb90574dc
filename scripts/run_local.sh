#!/usr/bin/env bash
# Run the drill locally (no docker) with three data directories.
# Note: plain local directories share one block device; use docker compose
# for the two-tmpfs layout where executor roots are on different file systems.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${SHUFFLE_DATA_ROOT:-$ROOT/data}"
export SHUFFLE_ORIGINALS_DIR="$DATA/originals"
export SHUFFLE_EXEC_A_DIR="$DATA/exec-a"
export SHUFFLE_EXEC_B_DIR="$DATA/exec-b"
export SHUFFLE_MANIFEST="$DATA/state/manifest.json"
mkdir -p "$SHUFFLE_ORIGINALS_DIR" "$SHUFFLE_EXEC_A_DIR" "$SHUFFLE_EXEC_B_DIR" \
         "$(dirname "$SHUFFLE_MANIFEST")"

# Widened race window so the concurrency behaviour is easy to observe locally.
export SHUFFLE_MERGE_READ_DELAY="${SHUFFLE_MERGE_READ_DELAY:-0}"
export SHUFFLE_HANDOFF_STEP_DELAY="${SHUFFLE_HANDOFF_STEP_DELAY:-0}"

exec "$ROOT/.venv/bin/uvicorn" app.main:app \
  --app-dir "$ROOT" --host 0.0.0.0 --port "${PORT:-8000}" --workers 1
