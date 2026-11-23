#!/usr/bin/env python3 -I
"""Start uvicorn in-process, then run the live reset-race drill.

Used only for local verification (no Docker daemon available in this
environment). Data directories live under /dev/shm (tmpfs) when writable so
the two executor roots still sit on real file systems; layout otherwise
mirrors docker-compose.yml.
"""
from __future__ import annotations

import os
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BASE = Path(os.environ.get("SHUFFLE_DEMO_ROOT", "/dev/shm/shuffle-reset-demo"))
os.environ.setdefault("SHUFFLE_ORIGINALS_DIR", str(BASE / "originals"))
os.environ.setdefault("SHUFFLE_EXEC_A_DIR", str(BASE / "exec-a"))
os.environ.setdefault("SHUFFLE_EXEC_B_DIR", str(BASE / "exec-b"))
os.environ.setdefault("SHUFFLE_MANIFEST", str(BASE / "state" / "manifest.json"))
os.environ["SHUFFLE_DEMO_BASE"] = "http://127.0.0.1:8000"
for d in (os.environ["SHUFFLE_ORIGINALS_DIR"], os.environ["SHUFFLE_EXEC_A_DIR"],
          os.environ["SHUFFLE_EXEC_B_DIR"], os.path.dirname(os.environ["SHUFFLE_MANIFEST"])):
    os.makedirs(d, exist_ok=True)

import uvicorn  # noqa: E402

config = uvicorn.Config(
    "app.main:app", host="127.0.0.1", port=8000,
    workers=1, log_level="warning",
)
server = uvicorn.Server(config)


def _run_demo_when_ready() -> None:
    for _ in range(100):
        try:
            urllib.request.urlopen("http://127.0.0.1:8000/docs", timeout=1)
            break
        except OSError:
            time.sleep(0.1)
    import scripts.reset_race_demo as demo

    try:
        demo.main()
    finally:
        server.should_exit = True


threading.Thread(target=_run_demo_when_ready, daemon=True).start()
server.run()
