"""Budget behaviour: over-budget handoff must not move or delete anything."""
from __future__ import annotations

import os


def _submit(client, job, executor, text, shard_id=None):
    body = {"job_id": job, "executor_id": executor, "text": text}
    if shard_id is not None:
        body["shard_id"] = shard_id
    return client.post("/jobs/shards", json=body)


def _shard_sizes(env, executor):
    sizes = []
    for root, _, files in os.walk(env["dirs"][executor]):
        for name in files:
            if name.endswith(".wc.json"):
                sizes.append(os.path.getsize(os.path.join(root, name)))
    return sorted(sizes)


def test_budget_short_fails_before_copying_and_keeps_original_locations(env):
    client = env["client"]
    # Make shards of clearly different sizes so the largest is recomputed.
    _submit(client, "job", "exec-a", "word " * 200, shard_id=0)
    _submit(client, "job", "exec-a", "tiny", shard_id=1)
    _submit(client, "job", "exec-a", "mid " * 50, shard_id=2)

    sizes_before = _shard_sizes(env, "exec-a")
    assert len(sizes_before) == 3
    smallest = min(sizes_before)

    # Budget below even the cheapest copy plan (two smallest files copied).
    r = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": smallest - 1},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is False
    assert body["copied_bytes"] == 0
    assert body["copied_shard_ids"] == []
    assert body["recomputed_shard_id"] is None
    assert "exceeds budget" in (body["reason"] or "")

    # Nothing was copied to the peer, nothing deleted from the source.
    assert _shard_sizes(env, "exec-b") == []
    assert _shard_sizes(env, "exec-a") == sizes_before

    # Executor stays ACTIVE and merges still read every shard in place.
    statuses = {e["executor_id"]: e["state"] for e in client.get("/executors").json()}
    assert statuses == {"exec-a": "ACTIVE", "exec-b": "ACTIVE"}
    merged = client.get("/jobs/job/word-count").json()
    assert merged["shards_merged"] == 3
    assert all(d["executor_id"] == "exec-a" for d in merged["shard_detail"])

    # A retry with an adequate budget then succeeds: the largest shard is
    # recomputed, so the copy need is the sum of the two smaller files.
    need = sum(sizes_before) - max(sizes_before)
    r2 = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": need},
    )
    assert r2.json()["success"] is True
    assert r2.json()["copied_bytes"] == need


def test_zero_budget_with_single_shard_succeeds_via_recompute(env):
    client = env["client"]
    _submit(client, "solo", "exec-a", "only one shard here")
    r = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 0},
    )
    body = r.json()
    assert body["success"] is True
    assert body["copied_bytes"] == 0
    assert body["recomputed_shard_id"] == 0
    merged = client.get("/jobs/solo/word-count").json()
    assert merged["word_counts"] == {"only": 1, "one": 1, "shard": 1, "here": 1}
    assert merged["shard_detail"][0]["executor_id"] == "exec-b"


def test_budget_exactly_equals_required_copy_bytes(env):
    client = env["client"]
    _submit(client, "j", "exec-a", "aaaa " * 100, shard_id=0)
    _submit(client, "j", "exec-a", "bb", shard_id=1)
    sizes = _shard_sizes(env, "exec-a")
    need = min(sizes)  # largest is recomputed, only the smaller file is copied
    r = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": need},
    )
    assert r.json()["success"] is True
    assert r.json()["copied_bytes"] == need


def test_originals_are_never_in_executor_trees(env):
    client = env["client"]
    _submit(client, "j", "exec-a", "data")
    for root, _, files in os.walk(env["originals"]):
        for name in files:
            full = os.path.join(root, name)
            assert not full.startswith(env["dirs"]["exec-a"])
            assert not full.startswith(env["dirs"]["exec-b"])
