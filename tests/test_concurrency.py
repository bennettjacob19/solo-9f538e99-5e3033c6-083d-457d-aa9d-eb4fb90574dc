"""Concurrency tests: merge and handoff run simultaneously, never waiting.

The store injects small per-shard delays (merge read delay / handoff step
delay) so the race window between switching a manifest entry and deleting
the old file is wide enough to hit deterministically. Every merge must
still count each shard exactly once, regardless of who wins the race.
"""
from __future__ import annotations

import threading
from collections import Counter

from tests.conftest import MergeThread, set_delays

TEXTS = {
    0: "alpha beta alpha",
    1: "beta gamma gamma gamma",
    2: "delta delta epsilon",
    3: "alpha epsilon zeta",
}

EXPECTED = Counter()
for t in TEXTS.values():
    EXPECTED.update(t.split())


def _seed(env):
    client = env["client"]
    for sid, text in TEXTS.items():
        r = client.post(
            "/jobs/shards",
            json={"job_id": "race", "executor_id": "exec-a", "text": text,
                  "shard_id": sid},
        )
        assert r.status_code == 201, r.text


def _set_delays(env, merge_delay=0.15, handoff_delay=0.12):
    set_delays(env, merge_delay=merge_delay, handoff_delay=handoff_delay)


def _total_intermediate_bytes(env):
    statuses = env["client"].get("/executors").json()
    a = next(e for e in statuses if e["executor_id"] == "exec-a")
    return sum(s["size_bytes"] for s in a["shards"])


def test_decommission_during_inflight_merge(env):
    """Merge starts first; the handoff finishes while it is still reading."""
    _seed(env)
    _set_delays(env)

    merge = MergeThread(env["client"], "race")
    merge.start()
    # Let the merge take its snapshot and begin reading shard 0.
    threading.Event().wait(0.1)

    budget = _total_intermediate_bytes(env)
    dec = env["client"].post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": budget},
    )
    merge.join(timeout=30)

    assert dec.status_code == 200 and dec.json()["success"] is True
    assert merge.exc is None
    assert merge.response is not None and merge.response.status_code == 200
    body = merge.response.json()
    assert body["shards_merged"] == 4
    assert Counter(body["word_counts"]) == EXPECTED

    # Follow-up merges (after the source directory was cleared) also work.
    again = env["client"].get("/jobs/race/word-count")
    assert again.status_code == 200
    assert Counter(again.json()["word_counts"]) == EXPECTED
    assert {d["executor_id"] for d in again.json()["shard_detail"]} == {"exec-b"}


def test_many_merges_while_handoff_runs(env):
    """Hammer merges from multiple threads while shards move one by one."""
    _seed(env)
    _set_delays(env, merge_delay=0.05, handoff_delay=0.1)

    errors: list[Exception] = []
    results: list[dict] = []
    results_lock = threading.Lock()

    def merge_worker():
        try:
            r = env["client"].get("/jobs/race/word-count")
            assert r.status_code == 200, r.text
            with results_lock:
                results.append(r.json())
        except Exception as exc:  # pragma: no cover - surfaced below
            with results_lock:
                errors.append(exc)

    workers = [threading.Thread(target=merge_worker) for _ in range(6)]
    for w in workers:
        w.start()
    threading.Event().wait(0.05)
    dec = env["client"].post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 10**9},
    )
    for w in workers:
        w.join(timeout=30)

    assert not errors
    assert dec.json()["success"] is True
    assert len(results) == 6
    for body in results:
        assert body["shards_merged"] == 4
        assert Counter(body["word_counts"]) == EXPECTED


def test_over_budget_handoff_never_disturbs_concurrent_merge(env):
    _seed(env)
    _set_delays(env)

    merge = MergeThread(env["client"], "race")
    merge.start()
    threading.Event().wait(0.1)

    dec = env["client"].post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 0},
    )
    merge.join(timeout=30)

    assert dec.json()["success"] is False
    assert merge.response.status_code == 200
    assert Counter(merge.response.json()["word_counts"]) == EXPECTED
    # All shards remain on exec-a and are still readable in place.
    detail = env["client"].get("/jobs/race/word-count").json()["shard_detail"]
    assert {d["executor_id"] for d in detail} == {"exec-a"}


def test_handoff_does_not_wait_for_merge_lock_during_io(env):
    """Handoff file IO happens outside the manifest lock; timings prove it."""
    _seed(env)
    _set_delays(env, merge_delay=0.2, handoff_delay=0.0)

    merge = MergeThread(env["client"], "race")
    merge.start()
    threading.Event().wait(0.05)

    import time

    t0 = time.monotonic()
    dec = env["client"].post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 10**9},
    )
    elapsed = time.monotonic() - t0
    merge.join(timeout=30)

    # If the handoff blocked on the merge's IO the call would take roughly
    # 4 * 0.2s; in practice it only takes brief manifest-lock acquisitions.
    assert dec.json()["success"] is True
    assert elapsed < 0.6, f"handoff appears to block on merge IO: {elapsed:.2f}s"
