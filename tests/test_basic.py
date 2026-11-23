"""End-to-end happy-path tests: submit, tokenize, merge, handoff."""
from __future__ import annotations

import json
import os

from app.store import STATE_ACTIVE, STATE_DECOMMISSIONED


def _submit(client, job, executor, text, shard_id=None):
    body = {"job_id": job, "executor_id": executor, "text": text}
    if shard_id is not None:
        body["shard_id"] = shard_id
    return client.post("/jobs/shards", json=body)


def test_submit_creates_word_count_and_keeps_original_outside(env):
    client = client_of(env)
    r = _submit(client, "job-1", "exec-a", "a b a c")
    assert r.status_code == 201, r.text
    info = r.json()
    assert info["shard_id"] == 0
    assert info["executor_id"] == "exec-a"

    # Word-count (shuffle) file lives inside the executor directory ...
    assert info["path"].startswith(env["dirs"]["exec-a"])
    with open(info["path"]) as fh:
        assert json.load(fh) == {"a": 2, "b": 1, "c": 1}
    # ... while the original text is outside both executor directories.
    assert info["original_path"].startswith(env["originals"])
    assert not info["original_path"].startswith(env["dirs"]["exec-a"])
    assert not info["original_path"].startswith(env["dirs"]["exec-b"])
    with open(info["original_path"]) as fh:
        assert fh.read() == "a b a c"


def test_auto_shard_ids_are_sequential(env):
    client = client_of(env)
    ids = [_submit(client, "j", "exec-a", "x").json()["shard_id"] for _ in range(3)]
    assert ids == [0, 1, 2]
    assert _submit(client, "j", "exec-a", "x", shard_id=1).status_code == 409


def test_merge_aggregates_all_shards_once(env):
    client = client_of(env)
    _submit(client, "job", "exec-a", "hello world hello")
    _submit(client, "job", "exec-b", "world there")
    r = client.get("/jobs/job/word-count")
    assert r.status_code == 200
    body = r.json()
    assert body["word_counts"] == {"hello": 2, "world": 2, "there": 1}
    assert body["shards_merged"] == 2
    assert {d["shard_id"] for d in body["shard_detail"]} == {0, 1}


def test_whitespace_tokenizer_handles_tabs_and_newlines(env):
    client = client_of(env)
    _submit(client, "t", "exec-a", "one\ttwo\n three  four ")
    body = client.get("/jobs/t/word-count").json()
    assert body["word_counts"] == {"one": 1, "two": 1, "three": 1, "four": 1}


def test_unknown_job_and_bad_inputs(env):
    client = client_of(env)
    assert client.get("/jobs/nope/word-count").status_code == 404
    r = _submit(client, "j", "exec-zzz", "x")
    assert r.status_code == 404
    r = _submit(client, "../escape", "exec-a", "x")
    assert r.status_code == 400


def test_successful_decommission_moves_shards_and_clears_source(env):
    client = client_of(env)
    _submit(client, "job", "exec-a", "alpha alpha beta")   # shard 0 on a
    _submit(client, "job", "exec-a", "gamma")              # shard 1 on a
    _submit(client, "job", "exec-b", "alpha")              # shard 2 stays on b

    before = client.get("/executors").json()
    a_before = next(e for e in before if e["executor_id"] == "exec-a")
    total = sum(s["size_bytes"] for s in a_before["shards"])

    r = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": total},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["success"] is True
    # Exactly one shard is recomputed (the largest), the rest are copied.
    assert body["recomputed_shard_id"] in (0, 1)
    assert len(body["copied_shard_ids"]) == 1
    assert body["copied_bytes"] >= 0
    for moved in body["moved_shards"]:
        assert moved["executor_id"] == "exec-b"
        assert moved["path"].startswith(env["dirs"]["exec-b"])

    # Source directory is empty after decommission.
    leftovers: list[str] = []
    for root, _, files in os.walk(env["dirs"]["exec-a"]):
        leftovers.extend(files)
    assert leftovers == []

    # Merge still returns every shard exactly once, now all read from b.
    merged = client.get("/jobs/job/word-count").json()
    assert merged["word_counts"] == {"alpha": 3, "beta": 1, "gamma": 1}
    assert merged["shards_merged"] == 3
    assert all(d["executor_id"] == "exec-b" for d in merged["shard_detail"])

    status = {e["executor_id"]: e for e in client.get("/executors").json()}
    assert status["exec-a"]["state"] == STATE_DECOMMISSIONED
    assert status["exec-b"]["state"] == STATE_ACTIVE

    # Submissions to a decommissioned executor are refused.
    assert _submit(client, "job", "exec-a", "x").status_code == 409
    # The surviving executor keeps accepting work.
    assert _submit(client, "job2", "exec-b", "x").status_code == 201


def test_decommission_without_shards_is_a_noop_success(env):
    client = client_of(env)
    r = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 0},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["success"] is True
    assert body["copied_bytes"] == 0
    assert body["recomputed_shard_id"] is None
    assert client.get("/executors").json()[0]["state"] == STATE_DECOMMISSIONED


def client_of(env):
    return env["client"]


def test_decommission_needs_a_live_peer(env):
    client = client_of(env)
    _submit(client, "j", "exec-a", "a")
    # Decommission exec-b first (no shards -> immediate success).
    r = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-b", "copy_byte_budget": 0},
    )
    assert r.json()["success"] is True
    # exec-a now has no ACTIVE peer to hand its shard to.
    r2 = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 10**9},
    )
    assert r2.status_code == 409
    assert "DECOMMISSIONED" in r2.json()["detail"]
    # Its shard stays readable.
    body = client.get("/jobs/j/word-count").json()
    assert body["shards_merged"] == 1
    assert body["shard_detail"][0]["executor_id"] == "exec-a"


def test_double_decommission_is_conflict(env):
    client = client_of(env)
    client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 0},
    )
    r = client.post(
        "/executors/decommission",
        json={"executor_id": "exec-a", "copy_byte_budget": 0},
    )
    assert r.status_code == 409
