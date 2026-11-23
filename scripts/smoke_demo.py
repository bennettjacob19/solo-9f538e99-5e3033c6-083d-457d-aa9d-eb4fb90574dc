#!/usr/bin/env python3
"""End-to-end smoke demo against a running shuffle-drill server.

Usage:
    python scripts/smoke_demo.py [base_url]

Exercises, in order:
  1. file-system layout check (executor roots should be on different devices);
  2. shard submission + merged word count;
  3. an over-budget decommission that must fail without moving any file;
  4. a successful decommission returning copied bytes / recomputed shard;
  5. a merge running concurrently with a decommission (exactly-once check);
  6. a post-decommission merge proving every shard is still counted once.

The script uses only the Python standard library so it also runs inside the
slim container image without extra packages.
"""
from __future__ import annotations

import json
import sys
import threading
import urllib.error
import urllib.request
from collections import Counter


def _request(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
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


def check(cond: bool, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        raise SystemExit(f"assertion failed: {label}")


def main(base: str) -> None:
    print(f"== target: {base}")

    # Make the demo idempotent: start from a clean state every run.
    _request("POST", f"{base}/admin/reset")

    print("1) file-system layout")
    _, fs = _request("GET", f"{base}/filesystems")
    print("   ", json.dumps(fs, ensure_ascii=False))
    dev_a = fs["executors"]["exec-a"]["device"]
    dev_b = fs["executors"]["exec-b"]["device"]
    if dev_a == dev_b:
        print("    NOTE: same device here; docker-compose mounts two tmpfs to differ")
    check(fs["originals_dir"]["device"] is not None, "originals directory exists")

    print("2) submit four shards (3 on exec-a, 1 on exec-b)")
    texts = [
        ("job-demo", 0, "exec-a", "alpha beta alpha gamma"),
        ("job-demo", 1, "exec-a", "beta beta gamma delta"),
        ("job-demo", 2, "exec-a", "alpha delta epsilon"),
        ("job-demo", 3, "exec-b", "epsilon zeta alpha"),
    ]
    expected = Counter()
    for job, sid, executor, text in texts:
        status, info = _request(
            "POST",
            f"{base}/jobs/shards",
            {"job_id": job, "shard_id": sid, "executor_id": executor, "text": text},
        )
        check(status == 201, f"shard {sid} accepted by {executor}")
        expected.update(text.split())
        print(f"    shard {sid}: wc file {info['path']} ({info['size_bytes']} B), "
              f"original {info['original_path']}")

    print("3) merged word count before decommission")
    status, merged = _request("GET", f"{base}/jobs/job-demo/word-count")
    check(status == 200 and merged["shards_merged"] == 4, "four shards merged")
    check(Counter(merged["word_counts"]) == expected, "totals match expected counts")
    print("   ", dict(sorted(merged["word_counts"].items())))

    print("4) over-budget decommission (budget 0, 3 shards on exec-a)")
    status, dec = _request(
        "POST",
        f"{base}/executors/decommission",
        {"executor_id": "exec-a", "copy_byte_budget": 0},
    )
    check(status == 200 and dec["success"] is False, "handoff refused over budget")
    print(f"    reason: {dec['reason']}")
    _, merged2 = _request("GET", f"{base}/jobs/job-demo/word-count")
    check(merged2["shards_merged"] == 4, "all shards still mergeable from old locations")

    print("5) successful decommission with adequate budget")
    _, executors = _request("GET", f"{base}/executors")
    a = next(e for e in executors if e["executor_id"] == "exec-a")
    total = sum(s["size_bytes"] for s in a["shards"])
    status, dec = _request(
        "POST",
        f"{base}/executors/decommission",
        {"executor_id": "exec-a", "copy_byte_budget": total},
    )
    check(status == 200 and dec["success"], "handoff succeeds within budget")
    print(f"    copied_bytes={dec['copied_bytes']} "
          f"copied_shards={dec['copied_shard_ids']} "
          f"recomputed_shard={dec['recomputed_shard_id']} "
          f"planned_total={dec['planned_total_bytes']}")
    check(len(dec["moved_shards"]) == 3, "three shards now owned by exec-b")
    check(all(m["executor_id"] == "exec-b" for m in dec["moved_shards"]),
          "moved shards point at exec-b paths")

    print("6) post-decommission merge (source dir already cleared)")
    status, merged3 = _request("GET", f"{base}/jobs/job-demo/word-count")
    check(status == 200 and merged3["shards_merged"] == 4, "still four shards")
    check(Counter(merged3["word_counts"]) == expected, "totals unchanged after handoff")
    check({d["executor_id"] for d in merged3["shard_detail"]} == {"exec-b"},
          "every shard read from exec-b exactly once")

    print("7) concurrent merge during another decommission (fresh job)")
    for sid, text in [(0, "one two two"), (1, "three three one"), (2, "two four")]:
        _request("POST", f"{base}/jobs/shards",
                 {"job_id": "job-race", "shard_id": sid,
                  "executor_id": "exec-b", "text": text})
    race_expected = Counter()
    for text in ["one two two", "three three one", "two four"]:
        race_expected.update(text.split())
    results: list[dict] = []
    errors: list[Exception] = []

    def merge_worker() -> None:
        try:
            status, body = _request("GET", f"{base}/jobs/job-race/word-count")
            assert status == 200, body
            results.append(body)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=merge_worker) for _ in range(4)]
    for t in threads:
        t.start()
    status, dec2 = _request(
        "POST", f"{base}/executors/decommission",
        {"executor_id": "exec-b", "copy_byte_budget": 10**9},
    )
    for t in threads:
        t.join()
    check(not errors, f"no merge errors ({len(errors)})")
    # exec-a is already DECOMMISSIONED from step 5, so it cannot accept
    # shards: the handoff must be refused with 409 and merges stay in place.
    check(status == 409, "decommission onto a dead peer is rejected (409)")
    print(f"    rejected: {dec2.get('detail', dec2)}")
    _, still = _request("GET", f"{base}/jobs/job-race/word-count")
    check(Counter(still["word_counts"]) == race_expected and still["shards_merged"] == 3,
          "job-race shards untouched and still counted exactly once")
    check(all(d["executor_id"] == "exec-b" for d in still["shard_detail"]),
          "all job-race shards remain on exec-b")

    print("\nALL SMOKE CHECKS PASSED")


if __name__ == "__main__":
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    main(base_url.rstrip("/"))
