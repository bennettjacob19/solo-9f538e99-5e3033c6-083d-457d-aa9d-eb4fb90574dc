"""Reset-vs-inflight-decommission races (the generation-fencing fix).

Scenarios exercised here
------------------------
1. A decommission paused in phase 1 (copy) while /admin/reset is issued:
   the stale handoff must end with 409, the reset must return immediately,
   and reusing the same job/shard ids must produce new files the old handoff
   can neither overwrite nor delete.
2. A decommission paused in phase 3 (cleanup) while /admin/reset is issued:
   the handoff already switched the manifest before the reset, so it keeps
   its original success receipt, yet its cleanup cannot touch the new round.
3. Reset reclaims tmpfs space of unopened files *before* its response, by
   unlinking only (no original-text copy); files still opened by a request
   survive until the last holder closes them.
4. The new round's merge reads every accepted shard while a same-round
   decommission runs in parallel.
"""
from __future__ import annotations

import os
import threading
import time
from collections import Counter

from app.store import STATE_ACTIVE, ConflictError

TEXTS = {
    0: "alpha beta alpha gamma",
    1: "beta beta gamma delta",
    2: "alpha delta epsilon",
}
EXPECTED = Counter()
for _t in TEXTS.values():
    EXPECTED.update(_t.split())


def _submit(client, job, sid, executor, text):
    return client.post(
        "/jobs/shards",
        json={"job_id": job, "shard_id": sid, "executor_id": executor, "text": text},
    )


def _seed(env, job="j1"):
    for sid, text in TEXTS.items():
        r = _submit(env["client"], job, sid, "exec-a", text)
        assert r.status_code == 201, r.text


def _wc_files(root):
    out = []
    for dirpath, _, files in os.walk(root):
        for name in files:
            if name.endswith(".wc.json"):
                out.append(os.path.join(dirpath, name))
    return sorted(out)


def _tree_bytes(root):
    total = 0
    for path in _wc_files(root):
        total += os.path.getsize(path)
    return total


class Gate:
    """Two-thread rendezvous: the worker waits, the test releases it."""

    def __init__(self):
        self.reached = threading.Event()
        self.release = threading.Event()

    def park(self):
        self.reached.set()
        self.release.wait(timeout=30)


def test_reset_during_copy_old_handoff_gets_409_and_new_round_is_untouchable(env):
    client, store = env["client"], env["store"]
    _seed(env)
    exec_a = env["dirs"]["exec-a"]
    assert len(_wc_files(exec_a)) == 3

    gate = Gate()

    def pause_on_first_copy(kind, shard):
        if kind == "copy":
            gate.park()

    store.on_handoff_stage = pause_on_first_copy

    dec_result = {}

    def old_handoff():
        try:
            dec_result["resp"] = client.post(
                "/executors/decommission",
                json={"executor_id": "exec-a", "copy_byte_budget": 10**9},
            )
        except Exception as exc:  # pragma: no cover - surfaced in assertions
            dec_result["exc"] = exc

    worker = threading.Thread(target=old_handoff)
    worker.start()
    assert gate.reached.wait(timeout=10), "old handoff never reached its copy step"

    # Reset must not wait for the parked handoff: the metadata-only reset
    # returns quickly even though the copy is suspended outside the lock.
    t0 = time.monotonic()
    r = client.post("/admin/reset")
    reset_elapsed = time.monotonic() - t0
    assert r.status_code == 204, r.text
    assert reset_elapsed < 5.0, f"reset blocked on the old handoff: {reset_elapsed:.2f}s"

    # Same job id and same shard numbers are reusable immediately.
    new_text = "reset reset new words here"
    r2 = _submit(env["client"], "j1", 0, "exec-a", new_text)
    assert r2.status_code == 201, r2.text
    new_path = r2.json()["path"]
    assert os.path.exists(new_path)
    new_inode = os.stat(new_path).st_ino

    # Let the stale handoff resume; it must fail with 409.
    gate.release.set()
    worker.join(timeout=30)
    assert "exc" not in dec_result, dec_result.get("exc")
    resp = dec_result["resp"]
    assert resp.status_code == 409, resp.text
    assert "generation" in resp.json()["detail"]

    # The new round's file is still there, untouched, with the same inode:
    # the old handoff neither overwrote nor deleted it.
    assert os.path.exists(new_path)
    assert os.stat(new_path).st_ino == new_inode
    with open(new_path) as fh:
        import json
        assert json.load(fh) == {"reset": 2, "new": 1, "words": 1, "here": 1}

    # New round manifest and executor states are exactly what reset set.
    statuses = {e["executor_id"]: e["state"] for e in client.get("/executors").json()}
    assert statuses == {"exec-a": STATE_ACTIVE, "exec-b": STATE_ACTIVE}
    merged = client.get("/jobs/j1/word-count").json()
    assert merged["shards_merged"] == 1
    assert merged["word_counts"] == {"reset": 2, "new": 1, "words": 1, "here": 1}

    # Old generation files were reclaimed; only gen-2 content remains.
    leftovers = [p for p in _wc_files(exec_a) if "gen-1" in p]
    assert leftovers == []
    assert all("gen-2" in p for p in _wc_files(exec_a))


def test_reset_during_cleanup_switched_handoff_keeps_success_but_cannot_touch_new_round(env):
    client, store = env["client"], env["store"]
    _seed(env, job="jc")

    gate = Gate()
    store.on_handoff_cleanup = lambda epoch: gate.park()

    dec_result = {}

    def old_handoff():
        dec_result["resp"] = client.post(
            "/executors/decommission",
            json={"executor_id": "exec-a", "copy_byte_budget": 10**9},
        )

    worker = threading.Thread(target=old_handoff)
    worker.start()
    assert gate.reached.wait(timeout=10), "old handoff never reached its cleanup step"

    # The manifest switch already happened before cleanup; the executor is
    # DECOMMISSIONED in the manifest the reset is about to replace.
    pre = {e["executor_id"]: e["state"] for e in client.get("/executors").json()}
    assert pre["exec-a"] == "DECOMMISSIONED"

    r = client.post("/admin/reset")
    assert r.status_code == 204

    # Reuse the same ids in the new round.
    new_text = "fresh round fresh data"
    r2 = _submit(env["client"], "jc", 0, "exec-a", new_text)
    assert r2.status_code == 201
    new_path = r2.json()["path"]
    new_inode = os.stat(new_path).st_ino
    assert "gen-2" in new_path

    # Resume the old handoff's phase-3 cleanup. It already switched in
    # gen-1, so it keeps its success receipt ...
    gate.release.set()
    worker.join(timeout=30)
    resp = dec_result["resp"]
    assert resp.status_code == 200
    assert resp.json()["success"] is True

    # ... but its unlinks were confined to gen-1 and never reached gen-2.
    assert os.path.exists(new_path)
    assert os.stat(new_path).st_ino == new_inode
    assert client.get("/jobs/jc/word-count").json()["word_counts"] == {
        "fresh": 2, "round": 1, "data": 1,
    }
    statuses = {e["executor_id"]: e["state"] for e in client.get("/executors").json()}
    assert statuses == {"exec-a": STATE_ACTIVE, "exec-b": STATE_ACTIVE}


def test_reset_reclaims_unopened_files_before_response_without_copying_originals(env):
    client, store = env["client"], env["store"]
    _seed(env, job="jr")
    exec_a = env["dirs"]["exec-a"]
    originals = env["originals"]

    occupied_before = _tree_bytes(exec_a)
    assert occupied_before > 0
    original_bytes_before = sum(
        os.path.getsize(os.path.join(dp, n))
        for dp, _, files in os.walk(originals) for n in files
    )
    assert original_bytes_before > 0

    # No handoff in flight: every old file is unopened.
    r = client.post("/admin/reset")
    assert r.status_code == 204

    # Tmpfs space held by the retired generation is released straight away,
    # purely via unlink ...
    assert _tree_bytes(exec_a) == 0
    assert not os.path.exists(os.path.join(exec_a, "gen-1"))
    # ... and no originals were copied anywhere to make that room.
    for root in env["dirs"].values():
        for dirpath, dirnames, filenames in os.walk(root):
            for name in filenames:
                assert not name.endswith(".txt"), f"original copied into {root}"
    # Originals themselves are cleared as part of the full reset.
    original_left = sum(
        os.path.getsize(os.path.join(dp, n))
        for dp, _, files in os.walk(originals) for n in files
    )
    assert original_left == 0


def test_file_opened_by_inflight_request_survives_until_that_request_exits(env):
    """Linux unlink semantics: an open fd keeps the inode (and its bytes)."""
    client, store = env["client"], env["store"]
    _seed(env, job="jo")

    opened = Gate()
    release_merge = threading.Event()

    def hold_merge_open(path):
        if "shard-0" in path:
            opened.reached.set()
            release_merge.wait(timeout=30)

    store.on_merge_file_open = hold_merge_open

    merge_result = {}

    def merge_worker():
        merge_result["resp"] = client.get("/jobs/jo/word-count")

    worker = threading.Thread(target=merge_worker)
    worker.start()
    assert opened.reached.wait(timeout=10)

    # Find the file the merge is holding and note its inode.
    held = next(p for p in _wc_files(env["dirs"]["exec-a"]) if "shard-0" in p)
    held_inode = os.stat(held).st_ino

    r = client.post("/admin/reset")
    assert r.status_code == 204
    # The directory entry is gone immediately; the inode (and its tmpfs
    # bytes) stays alive while the merge's file descriptor is open.
    assert not os.path.exists(held)

    # Reuse the same id in the new round while the old merge is still parked.
    r2 = _submit(env["client"], "jo", 0, "exec-a", "brand new round")
    assert r2.status_code == 201
    assert os.stat(r2.json()["path"]).st_ino != held_inode

    # Releasing the merge lets its last reference close; the old inode frees.
    release_merge.set()
    worker.join(timeout=30)
    # A merge that snapshotted the old generation loses its fallback lookup
    # when the reset clears the manifest; its failure must not damage the new
    # round. The new round itself merges cleanly.
    assert merge_result["resp"].status_code in (200, 500)
    body = client.get("/jobs/jo/word-count").json()
    assert body["shards_merged"] == 1
    assert body["word_counts"] == {"brand": 1, "new": 1, "round": 1}
    # Nothing from gen-1 remains once every holder has exited.
    assert _tree_bytes(env["dirs"]["exec-a"]) == os.path.getsize(r2.json()["path"])


def test_new_round_merge_reads_all_shards_while_same_round_handoff_runs(env):
    client, store = env["client"], env["store"]
    _seed(env, job="jp")

    gate = Gate()
    store.on_handoff_stage = lambda kind, shard: gate.park() if kind == "copy" else None

    dec_result = {}

    def handoff_worker():
        dec_result["resp"] = client.post(
            "/executors/decommission",
            json={"executor_id": "exec-a", "copy_byte_budget": 10**9},
        )

    worker = threading.Thread(target=handoff_worker)
    worker.start()
    assert gate.reached.wait(timeout=10)

    # A second-round-style submission on the still-ACTIVE peer while the
    # same-generation handoff is suspended.
    r = _submit(env["client"], "jp-extra", 0, "exec-b", "peer keeps accepting")
    assert r.status_code == 201

    gate.release.set()
    worker.join(timeout=30)
    assert dec_result["resp"].status_code == 200
    assert dec_result["resp"].json()["success"] is True

    # All accepted shards of each job are mergeable, each counted once.
    merged = client.get("/jobs/jp/word-count").json()
    assert merged["shards_merged"] == 3
    assert Counter(merged["word_counts"]) == EXPECTED
    extra = client.get("/jobs/jp-extra/word-count").json()
    assert extra["shards_merged"] == 1


def test_reset_keeps_layout_roots_queryable_and_round_trips_through_restart(env):
    """Regression: reclaim must not delete the executor root directories."""
    client = env["client"]
    _seed(env, job="jk")
    assert client.post("/admin/reset").status_code == 204

    r = client.get("/filesystems")
    assert r.status_code == 200, r.text
    devices = r.json()["executors"]
    assert set(devices) == {"exec-a", "exec-b"}
    for exec_id in ("exec-a", "exec-b"):
        assert os.path.isdir(env["dirs"][exec_id])

    # A brand-new Store on the same manifest sees the bumped epoch and keeps
    # working (its startup sweep finds nothing belonging to the live epoch).
    from app.config import Settings as _S  # noqa: F401
    from app.main import create_app
    from app.store import Store
    from fastapi.testclient import TestClient

    restarted = Store(env["cfg"])
    assert restarted.manifest.epoch == env["store"].manifest.epoch
    with TestClient(create_app(restarted)) as new_client:
        assert new_client.get("/filesystems").status_code == 200
        r2 = _submit(new_client, "jk", 0, "exec-a", "after restart works")
        assert r2.status_code == 201


def test_stale_handoff_direct_store_call_raises_conflict_and_keeps_new_manifest(env):
    """The same fence at the Store layer (HTTP maps ConflictError to 409)."""
    store = env["store"]
    _seed(env, job="js")

    gate = Gate()
    store.on_handoff_stage = lambda kind, shard: gate.park() if kind == "copy" else None
    outcome = {}

    def worker():
        try:
            store.decommission("exec-a", 10**9)
        except Exception as exc:
            outcome["exc"] = exc

    t = threading.Thread(target=worker)
    t.start()
    assert gate.reached.wait(timeout=10)
    store.reset()
    _submit(env["client"], "js", 0, "exec-a", "new generation payload")
    gate.release.set()
    t.join(timeout=30)

    assert isinstance(outcome["exc"], ConflictError)
    # New-round manifest untouched: exactly one shard, on exec-a, ACTIVE.
    body = env["client"].get("/jobs/js/word-count").json()
    assert body["shards_merged"] == 1
    assert body["shard_detail"][0]["executor_id"] == "exec-a"
