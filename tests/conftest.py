"""Pytest fixtures: build an isolated app backed by temporary directories."""
from __future__ import annotations

import os
import sys
import threading
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402
from app.store import Store  # noqa: E402


@pytest.fixture
def env(tmp_path_factory):
    base = tmp_path_factory.mktemp("shuffle")
    dirs = {
        "exec-a": str(base / "exec-a"),
        "exec-b": str(base / "exec-b"),
    }
    originals = str(base / "originals")
    manifest = str(base / "state" / "manifest.json")
    for path in [*dirs.values(), originals]:
        os.makedirs(path, exist_ok=True)
    cfg = Settings(
        originals_dir=originals,
        executor_dirs=dict(dirs),
        manifest_path=manifest,
        merge_read_delay=0.0,
        handoff_step_delay=0.0,
    )
    store = Store(cfg)
    app = create_app(store)
    with TestClient(app) as client:
        yield {
            "client": client,
            "store": store,
            "cfg": cfg,
            "dirs": dirs,
            "originals": originals,
            "base": base,
        }


def set_delays(env, merge_delay=0.0, handoff_delay=0.0):
    """Swap the store's config for one with widened race-window delays."""
    env["store"].cfg = replace(
        env["cfg"],
        merge_read_delay=merge_delay,
        handoff_step_delay=handoff_delay,
    )
    return env["store"].cfg


class MergeThread(threading.Thread):
    """Run a merge request in a real OS thread to exercise the race window."""

    def __init__(self, client, job_id):
        super().__init__(daemon=True)
        self.client = client
        self.job_id = job_id
        self.response = None
        self.exc = None

    def run(self):
        try:
            self.response = self.client.get(f"/jobs/{self.job_id}/word-count")
        except Exception as exc:  # pragma: no cover - surfaced in assertions
            self.exc = exc
