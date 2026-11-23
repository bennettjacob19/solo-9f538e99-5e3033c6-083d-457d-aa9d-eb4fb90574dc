#!/usr/bin/env python3
"""Live-server drill for the reset-vs-inflight-decommission fix.

Run against a real uvicorn process (see scripts/run_reset_demo.sh):

  1. seed a job, then park a decommission inside its phase-1 *copy* step,
     issue /admin/reset, and reuse the same job/shard id in the new round;
  2. assert the stale handoff answers 409 and never touches the new file;
  3. park another decommission in its phase-3 *cleanup* step, reset again,
     and assert that handoff keeps its original success receipt while the
     new round's files remain intact;
  4. verify generation directories (gen-<epoch>) isolate the two rounds.

The pause hooks are attached to the Store instance inside this process, so
the script is meant to be imported by the server process (the companion
shell wrapper runs uvicorn with a startup hook), not against a remote host.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request

from app.store import Store

BASE = os.environ.get("SHUFFLE_DEMO_BASE", "http://127.0.0.1:8000")


def _request(method: str, url: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode()
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


class Gate:
    def __init__(self):
        self.reached = threading.Event()
        self.release = threading.Event()

    def park(self, *_args):
        self.reached.set()
        self.release.wait(timeout=60)


def check(cond: bool, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        raise SystemExit(f"assertion failed: {label}")


def install_hooks() -> Store:
    from app.main import app

    store: Store = app.state.store
    return store


def scenario_reset_during_copy(store: Store) -> None:
    print("A) reset while the old decommission is paused in phase-1 copy")
    _request("POST", f"{BASE}/admin/reset")
    for sid, text in {0: "alpha beta alpha", 1: "gamma gamma delta",
                      2: "epsilon zeta"}.items():
        status, _ = _request(
            "POST", f"{BASE}/jobs/shards",
            {"job_id": "job-a", "shard_id": sid, "executor_id": "exec-a",
             "text": text},
        )
        check(status == 201, f"seed shard {sid}")

    gate = Gate()
    store.on_handoff_stage = lambda kind, shard: gate.park() if kind == "copy" else None
    outcome: dict = {}

    def handoff():
        outcome["status"], outcome["body"] = _request(
            "POST", f"{BASE}/executors/decommission",
            {"executor_id": "exec-a", "copy_byte_budget": 10**9},
        )

    t = threading.Thread(target=handoff)
    t.start()
    check(gate.reached.wait(10), "handoff reached the parked copy step")

    t0 = time.monotonic()
    status, _ = _request("POST", f"{BASE}/admin/reset")
    elapsed = time.monotonic() - t0
    check(status == 204, "reset accepted while the old handoff is suspended")
    check(elapsed < 5.0, f"reset did not wait for the old handoff ({elapsed:.2f}s)")

    status, info = _request(
        "POST", f"{BASE}/jobs/shards",
        {"job_id": "job-a", "shard_id": 0, "executor_id": "exec-a",
         "text": "new round payload"},
    )
    check(status == 201, "same job id + shard id reused immediately")
    new_path = info["path"]
    check(os.path.basename(os.path.dirname(os.path.dirname(new_path))).startswith("gen-"),
          f"new file lands in a fresh generation dir: {new_path}")
    inode_before = os.stat(new_path).st_ino

    gate.release.set()
    t.join(timeout=30)
    check(outcome["status"] == 409,
          f"stale pre-switch handoff rejected with 409 (got {outcome['status']})")
    check(os.path.exists(new_path), "new-round file still exists")
    check(os.stat(new_path).st_ino == inode_before, "new-round file was not replaced")
    with open(new_path) as fh:
        check(json.load(fh) == {"new": 1, "round": 1, "payload": 1},
              "new-round file content untouched")
    store.on_handoff_stage = None


def scenario_reset_during_cleanup(store: Store) -> None:
    print("B) reset while an already-switched decommission is paused in cleanup")
    _request("POST", f"{BASE}/admin/reset")
    for sid, text in {0: "one two two", 1: "three three one"}.items():
        status, _ = _request(
            "POST", f"{BASE}/jobs/shards",
            {"job_id": "job-b", "shard_id": sid, "executor_id": "exec-a",
             "text": text},
        )
        check(status == 201, f"seed shard {sid}")

    gate = Gate()
    store.on_handoff_cleanup = lambda epoch: gate.park()
    outcome: dict = {}

    def handoff():
        outcome["status"], outcome["body"] = _request(
            "POST", f"{BASE}/executors/decommission",
            {"executor_id": "exec-a", "copy_byte_budget": 10**9},
        )

    t = threading.Thread(target=handoff)
    t.start()
    check(gate.reached.wait(10), "handoff reached the parked cleanup step")

    status, _ = _request("POST", f"{BASE}/admin/reset")
    check(status == 204, "reset during the parked cleanup")
    status, info = _request(
        "POST", f"{BASE}/jobs/shards",
        {"job_id": "job-b", "shard_id": 0, "executor_id": "exec-a",
         "text": "fresh generation"},
    )
    check(status == 201, "reuse ids in the new generation")
    new_path = info["path"]
    inode_before = os.stat(new_path).st_ino

    gate.release.set()
    t.join(timeout=30)
    check(outcome["status"] == 200 and outcome["body"]["success"] is True,
          "post-switch handoff keeps its original success receipt")
    check(os.stat(new_path).st_ino == inode_before,
          "old cleanup did not delete/replace the new file")
    status, merged = _request("GET", f"{BASE}/jobs/job-b/word-count")
    check(status == 200 and merged["shards_merged"] == 1,
          "new round merges exactly its own shard")
    store.on_handoff_cleanup = None

    # Only the live generation directory may remain on each executor (an
    # executor that received no new-round shards simply has no gen dir yet).
    live_epoch = store.manifest.epoch
    for exec_id, root_env in (("exec-a", "SHUFFLE_EXEC_A_DIR"),
                              ("exec-b", "SHUFFLE_EXEC_B_DIR")):
        base = os.environ.get(root_env)
        if base and os.path.isdir(base):
            gens = sorted(n for n in os.listdir(base) if n.startswith("gen-"))
            check(all(int(n[4:]) == live_epoch for n in gens),
                  f"{exec_id}: no retired generation remains, got {gens}")


def main() -> None:
    store = install_hooks()
    print(f"== live reset drill against {BASE} (pid {os.getpid()})")
    scenario_reset_during_copy(store)
    scenario_reset_during_cleanup(store)
    print("\nALL RESET-DRILL CHECKS PASSED")


if __name__ == "__main__":
    main()
