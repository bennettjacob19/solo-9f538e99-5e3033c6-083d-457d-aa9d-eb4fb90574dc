"""Real cross-file-system handoff test.

The staging dir under /tmp lives on the container's overlay FS while
/dev/shm is a separate tmpfs (different st_dev). When both are available,
this proves the handoff really copies bytes rather than relying on
os.rename, which raises EXDEV across file systems.
"""
from __future__ import annotations

import os
import shutil

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.store import Store

pytestmark = pytest.mark.skipif(
    not (os.path.isdir("/dev/shm") and os.access("/dev/shm", os.W_OK)),
    reason="/dev/shm tmpfs not available",
)


def _different_device_than(path: str) -> bool:
    return os.stat(path).st_dev != os.stat("/dev/shm").st_dev


def test_handoff_between_two_distinct_file_systems(tmp_path):
    if not _different_device_than(str(tmp_path)):
        pytest.skip("tmp path is not on a different device than /dev/shm")

    base_a = tmp_path / "exec-a"            # overlay/tmpfs of the test root
    base_b_parent = "/dev/shm"              # separate tmpfs
    originals = tmp_path / "originals"
    base_a.mkdir()
    originals.mkdir()
    b_root = None
    try:
        b_root = os.path.join(base_b_parent, f"shuffle-xdev-{os.getpid()}")
        os.makedirs(b_root, exist_ok=False)
        cfg = Settings(
            originals_dir=str(originals),
            executor_dirs={"exec-a": str(base_a), "exec-b": b_root},
            manifest_path=str(tmp_path / "state" / "manifest.json"),
        )
        store = Store(cfg)
        info = store.filesystem_info()
        dev_a = info["executors"]["exec-a"]["device"]
        dev_b = info["executors"]["exec-b"]["device"]
        assert dev_a != dev_b, "test setup did not produce two file systems"

        with TestClient(create_app(store)) as client:
            for sid, text in {0: "cross device words " * 20, 1: "small",
                              2: "middle words " * 8}.items():
                r = client.post(
                    "/jobs/shards",
                    json={"job_id": "xdev", "executor_id": "exec-a",
                          "text": text, "shard_id": sid},
                )
                assert r.status_code == 201

            dec = client.post(
                "/executors/decommission",
                json={"executor_id": "exec-a", "copy_byte_budget": 10**9},
            ).json()
            assert dec["success"] is True
            assert dec["recomputed_shard_id"] is not None
            assert len(dec["copied_shard_ids"]) == 2

            # Every moved file physically resides on the second device.
            for moved in dec["moved_shards"]:
                assert os.stat(moved["path"]).st_dev == dev_b

            merged = client.get("/jobs/xdev/word-count").json()
            assert merged["shards_merged"] == 3
            assert all(d["executor_id"] == "exec-b"
                       for d in merged["shard_detail"])

            # Source tree on the first device is empty.
            leftovers = []
            for root, _, files in os.walk(base_a):
                leftovers.extend(files)
            assert leftovers == []
    finally:
        if b_root and os.path.isdir(b_root):
            shutil.rmtree(b_root)
