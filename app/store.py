"""Core state machine, merge reader and executor decommission/handoff logic.

Design summary
--------------
A single JSON *manifest* is the authoritative registry of where each shard's
word-count file currently lives. Every mutation rewrites it atomically
(tmp file + ``os.replace``), so any reader either sees the old or the new
generation, never a torn file.

Handoff (decommission) moves each shard of the leaving executor to its peer:

* the largest shard may be *recomputed* from the original text (at most one
  recompute, as required by the drill); its bytes never count against the
  copy budget;
* every other shard is copied byte-for-byte; its size counts against the
  copy budget;
* the manifest entry is switched to the new path only after the destination
  file is durably in place;
* the old file is deleted after the manifest switch has been persisted.

A merge that races a handoff reads a snapshot first. If its source file has
already been removed, the merge looks up that shard's *current* manifest
entry and reads it there instead. Each shard therefore contributes exactly
once: the snapshot fixes *which* shards are merged, the fallback only
changes *where* a given shard is read from.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Optional

from .config import Settings, settings as default_settings

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

STATE_ACTIVE = "ACTIVE"
STATE_DECOMMISSIONING = "DECOMMISSIONING"
STATE_DECOMMISSIONED = "DECOMMISSIONED"


class StoreError(Exception):
    """Base error carrying an HTTP status code."""

    status_code = 400


class NotFoundError(StoreError):
    status_code = 404


class ConflictError(StoreError):
    status_code = 409


class HandoffError(StoreError):
    status_code = 500


@dataclass
class Shard:
    job_id: str
    shard_id: int
    executor_id: str
    path: str
    original_path: str
    size_bytes: int

    def to_info(self) -> dict:
        return asdict(self)


@dataclass
class Executor:
    executor_id: str
    dir: str
    state: str = STATE_ACTIVE


@dataclass
class Manifest:
    shards: dict[str, dict[int, Shard]] = field(default_factory=dict)
    executors: dict[str, Executor] = field(default_factory=dict)
    next_shard: dict[str, int] = field(default_factory=dict)


def tokenize(text: str) -> list[str]:
    """Whitespace tokenizer, matching the drill's '按空白分词' rule."""
    return text.split()


def count_words(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for token in tokenize(text):
        counts[token] = counts.get(token, 0) + 1
    return counts


def _validate_id(kind: str, value: str) -> None:
    if not _ID_RE.match(value):
        raise StoreError(f"invalid {kind}: {value!r} (allowed: letters, digits, _ . -)")


def _atomic_write_text(path: str, data: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _atomic_write_json(path: str, payload: dict) -> None:
    _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _copy_bytes(src: str, dst: str) -> int:
    """Copy a file across potentially different file systems, return size."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    tmp = f"{dst}.tmp.{os.getpid()}.{threading.get_ident()}"
    total = 0
    try:
        with open(src, "rb") as inp, open(tmp, "wb") as out:
            while True:
                chunk = inp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                total += len(chunk)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, dst)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return total


def _read_word_count(path: str) -> dict[str, int]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise HandoffError(f"corrupt word-count file: {path}")
    return {str(k): int(v) for k, v in data.items()}


def _fs_info(path: str) -> dict:
    st = os.stat(path)
    return {"path": path, "device": st.st_dev, "inode": st.st_ino}


class Store:
    def __init__(self, config: Optional[Settings] = None):
        self.cfg = config or default_settings
        self._lock = threading.RLock()
        self.manifest = Manifest()
        for exec_id, directory in self.cfg.executor_dirs.items():
            self.manifest.executors[exec_id] = Executor(exec_id, directory)
        self._ensure_dirs()
        self._load_manifest()

    # ------------------------------------------------------------------ setup
    def _ensure_dirs(self) -> None:
        for directory in [*self.cfg.executor_dirs.values(), self.cfg.originals_dir]:
            os.makedirs(directory, exist_ok=True)
        os.makedirs(os.path.dirname(self.cfg.manifest_path), exist_ok=True)

    def _load_manifest(self) -> None:
        if not os.path.exists(self.cfg.manifest_path):
            self._persist_locked()
            return
        with open(self.cfg.manifest_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        for exec_id, ex in raw.get("executors", {}).items():
            if exec_id in self.manifest.executors:
                self.manifest.executors[exec_id].state = ex.get(
                    "state", STATE_ACTIVE
                )
        for job_id, shards in raw.get("shards", {}).items():
            bucket: dict[int, Shard] = {}
            for key, val in shards.items():
                bucket[int(key)] = Shard(**val)
            self.manifest.shards[job_id] = bucket
        self.manifest.next_shard = {
            k: int(v) for k, v in raw.get("next_shard", {}).items()
        }
        # Crash recovery: a half-finished handoff leaves the source intact,
        # so every executor is usable again after a process restart.
        for executor in self.manifest.executors.values():
            if executor.state == STATE_DECOMMISSIONING:
                executor.state = STATE_ACTIVE

    def _persist_locked(self) -> None:
        payload = {
            "executors": {
                eid: {"executor_id": e.executor_id, "dir": e.dir, "state": e.state}
                for eid, e in self.manifest.executors.items()
            },
            "shards": {
                job: {str(sid): asdict(sh) for sid, sh in bucket.items()}
                for job, bucket in self.manifest.shards.items()
            },
            "next_shard": dict(self.manifest.next_shard),
        }
        _atomic_write_json(self.cfg.manifest_path, payload)

    # ---------------------------------------------------------------- helpers
    def _executor(self, exec_id: str) -> Executor:
        executor = self.manifest.executors.get(exec_id)
        if executor is None:
            raise NotFoundError(
                f"unknown executor {exec_id!r}; known: {sorted(self.manifest.executors)}"
            )
        return executor

    def _shard_paths(self, job_id: str, shard_id: int, executor: Executor) -> tuple[str, str]:
        job_dir = os.path.join(executor.dir, job_id)
        wc_path = os.path.join(job_dir, f"shard-{shard_id}.wc.json")
        original_path = os.path.join(
            self.cfg.originals_dir, job_id, f"shard-{shard_id}.txt"
        )
        return wc_path, original_path

    def _snapshot_job_locked(self, job_id: str) -> list[Shard]:
        bucket = self.manifest.shards.get(job_id)
        if not bucket:
            raise NotFoundError(f"job {job_id!r} has no shards")
        return [bucket[sid] for sid in sorted(bucket)]

    # ---------------------------------------------------------------- submit
    def submit_shard(
        self,
        job_id: str,
        shard_id: Optional[int],
        executor_id: str,
        text: str,
    ) -> Shard:
        _validate_id("job_id", job_id)
        _validate_id("executor_id", executor_id)
        with self._lock:
            executor = self._executor(executor_id)
            if executor.state != STATE_ACTIVE:
                raise ConflictError(
                    f"executor {executor_id!r} is {executor.state}; "
                    "submissions are only accepted by ACTIVE executors"
                )
            bucket = self.manifest.shards.setdefault(job_id, {})
            if shard_id is None:
                shard_id = self.manifest.next_shard.get(job_id, 0)
                self.manifest.next_shard[job_id] = shard_id + 1
            else:
                if shard_id in bucket:
                    raise ConflictError(
                        f"shard {shard_id} of job {job_id!r} already exists"
                    )
                self.manifest.next_shard[job_id] = max(
                    self.manifest.next_shard.get(job_id, 0), shard_id + 1
                )
            wc_path, original_path = self._shard_paths(job_id, shard_id, executor)
            counts = count_words(text)
            # Original text first, outside any executor directory.
            _atomic_write_text(original_path, text)
            # Then the per-shard word-count (shuffle) file in the executor dir.
            _atomic_write_json(wc_path, counts)
            size = os.path.getsize(wc_path)
            shard = Shard(
                job_id=job_id,
                shard_id=shard_id,
                executor_id=executor_id,
                path=wc_path,
                original_path=original_path,
                size_bytes=size,
            )
            bucket[shard_id] = shard
            self._persist_locked()
            return shard

    # ----------------------------------------------------------------- merge
    def merge_word_counts(self, job_id: str) -> tuple[dict[str, int], list[Shard]]:
        """Read and merge every shard's word-count file, exactly once each.

        Snapshots the shard list under the lock, then performs file IO
        without holding it so a concurrent handoff is not blocked. If a
        shard's file was deleted between snapshot and open, the current
        manifest location of *that same shard* is used instead.
        """
        with self._lock:
            snapshot = self._snapshot_job_locked(job_id)
        merged: dict[str, int] = {}
        detail: list[Shard] = []
        for shard in snapshot:
            counts, located = self._read_one_with_fallback(shard)
            for word, n in counts.items():
                merged[word] = merged.get(word, 0) + n
            detail.append(located)
        return merged, detail

    def _read_one_with_fallback(self, shard: Shard) -> tuple[dict[str, int], Shard]:
        last_error: Optional[Exception] = None
        current = shard
        for _ in range(3):
            path = current.path
            if self.cfg.merge_read_delay:
                time.sleep(self.cfg.merge_read_delay)
            try:
                return _read_word_count(path), current
            except FileNotFoundError as exc:
                last_error = exc
                # The handoff may have moved this shard; look it up again.
                with self._lock:
                    bucket = self.manifest.shards.get(shard.job_id, {})
                    refreshed = bucket.get(shard.shard_id)
                    if refreshed is None or refreshed.path == path:
                        # Nothing better to try (or shard genuinely gone).
                        break
                    current = refreshed
            except json.JSONDecodeError as exc:
                raise HandoffError(f"unreadable word-count file {path}: {exc}") from exc
        raise HandoffError(
            f"word-count file for shard {shard.shard_id} of job "
            f"{shard.job_id!r} disappeared: {last_error}"
        )

    # ---------------------------------------------------------- decommission
    def _plan_handoff_locked(
        self, source: Executor
    ) -> tuple[list[Shard], Optional[Shard], int, int]:
        """Return (to_copy, recompute_target, copy_cost, total_intermediate_bytes)."""
        owned: list[Shard] = []
        for bucket in self.manifest.shards.values():
            owned.extend(s for s in bucket.values() if s.executor_id == source.executor_id)
        owned.sort(key=lambda s: s.shard_id)
        total = sum(s.size_bytes for s in owned)
        # Recompute at most one shard: pick the largest so the copy cost is
        # minimized, giving the handoff the best chance to fit the budget.
        recompute = max(owned, key=lambda s: s.size_bytes) if owned else None
        to_copy = [s for s in owned if s is not recompute]
        copy_cost = sum(s.size_bytes for s in to_copy)
        return to_copy, recompute, copy_cost, total

    def decommission(
        self, executor_id: str, budget: int
    ) -> dict:
        _validate_id("executor_id", executor_id)
        with self._lock:
            source = self._executor(executor_id)
            if source.state == STATE_DECOMMISSIONED:
                raise ConflictError(f"executor {executor_id!r} is already decommissioned")
            if source.state == STATE_DECOMMISSIONING:
                raise ConflictError(f"executor {executor_id!r} is already being decommissioned")
            peer_ids = [eid for eid in self.manifest.executors if eid != executor_id]
            if not peer_ids:
                raise ConflictError("no peer executor available for handoff")
            peer = self.manifest.executors[peer_ids[0]]
            if peer.state != STATE_ACTIVE:
                raise ConflictError(
                    f"peer executor {peer.executor_id!r} is {peer.state}; cannot receive shards"
                )
            to_copy, recompute, copy_cost, total = self._plan_handoff_locked(source)
            # Budget is a hard ceiling: over-budget requests fail before any
            # byte is copied, leaving every shard at its original location.
            if copy_cost > budget:
                return {
                    "success": False,
                    "reason": (
                        f"copy need {copy_cost}B exceeds budget {budget}B "
                        f"(recomputing shard {recompute.shard_id if recompute else '-'} "
                        "from original text is already factored in)"
                    ),
                    "copied_bytes": 0,
                    "copied_shard_ids": [],
                    "recomputed_shard_id": None,
                    "planned_total_bytes": total,
                    "moved": [],
                    "peer_executor_id": peer.executor_id,
                }
            source.state = STATE_DECOMMISSIONING
            self._persist_locked()

        staged: list[tuple[Shard, str, str]] = []  # (shard, old_path, new_path)
        moved: list[Shard] = []
        copied_bytes = 0
        copied_ids: list[int] = []
        recomputed_id: Optional[int] = None
        try:
            # Phase 1 - stage ALL destination files while the manifest still
            # points at the originals. No source file is touched here, so a
            # failure at any point leaves the live configuration unchanged.
            for shard in to_copy:
                dst, _ = self._shard_paths(shard.job_id, shard.shard_id, peer)
                if os.path.abspath(dst) == os.path.abspath(shard.path):
                    raise HandoffError("source and destination path collide")
                try:
                    n = _copy_bytes(shard.path, dst)
                except OSError as exc:
                    raise HandoffError(
                        f"failed copying shard {shard.shard_id} of job "
                        f"{shard.job_id!r}: {exc}"
                    ) from exc
                if self.cfg.handoff_step_delay:
                    time.sleep(self.cfg.handoff_step_delay)
                staged.append((shard, shard.path, dst))
                copied_bytes += n
                copied_ids.append(shard.shard_id)

            # At most one shard is recomputed from the original text.
            if recompute is not None:
                if not os.path.exists(recompute.original_path):
                    raise HandoffError(
                        f"original text missing for recompute: {recompute.original_path}"
                    )
                try:
                    with open(recompute.original_path, "r", encoding="utf-8") as fh:
                        text = fh.read()
                    dst, _ = self._shard_paths(
                        recompute.job_id, recompute.shard_id, peer
                    )
                    _atomic_write_json(dst, count_words(text))
                except OSError as exc:
                    raise HandoffError(
                        f"failed recomputing shard {recompute.shard_id} of job "
                        f"{recompute.job_id!r}: {exc}"
                    ) from exc
                if self.cfg.handoff_step_delay:
                    time.sleep(self.cfg.handoff_step_delay)
                staged.append((recompute, recompute.path, dst))
                recomputed_id = recompute.shard_id

            # Phase 2 - every destination file is in place; switch all
            # manifest entries atomically (one persisted manifest revision).
            with self._lock:
                for shard, old_path, dst in staged:
                    shard.executor_id = peer.executor_id
                    shard.path = dst
                    shard.size_bytes = os.path.getsize(dst)
                    self.manifest.shards[shard.job_id][shard.shard_id] = shard
                    moved.append(shard)
                source.state = STATE_DECOMMISSIONED
                self._persist_locked()

            # Phase 3 - only now are the old files removed. A racing merge
            # that snapshotted earlier either still finds the old file (it
            # cannot be deleted before phase 2) or follows the new manifest
            # entry to the staged destination; both contain identical counts.
            for _, old_path, _ in staged:
                self._safe_unlink(old_path)
            self._clear_source_dir(source.dir)
        except HandoffError:
            self._rollback(source, [dst for _, _, dst in staged])
            raise
        except Exception as exc:
            self._rollback(source, [dst for _, _, dst in staged])
            raise HandoffError(f"handoff failed: {exc}") from exc

        return {
            "success": True,
            "reason": None,
            "copied_bytes": copied_bytes,
            "copied_shard_ids": sorted(copied_ids),
            "recomputed_shard_id": recomputed_id,
            "planned_total_bytes": total,
            "moved": moved,
            "peer_executor_id": peer.executor_id,
        }

    @staticmethod
    def _safe_unlink(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def _clear_source_dir(self, directory: str) -> None:
        """Remove leftover files/job sub-directories of a decommissioned executor."""
        for root, dirs, files in os.walk(directory, topdown=False):
            for name in files:
                self._safe_unlink(os.path.join(root, name))
            for name in dirs:
                full = os.path.join(root, name)
                try:
                    os.rmdir(full)
                except OSError:
                    pass

    def _rollback(self, source: Executor, staged_dsts: list[str]) -> None:
        """Remove staged destination files; manifest entries were never changed."""
        for dst in staged_dsts:
            self._safe_unlink(dst)
        with self._lock:
            source.state = STATE_ACTIVE
            self._persist_locked()

    # ------------------------------------------------------------------ views
    def executor_status(self) -> list[Executor]:
        return list(self.manifest.executors.values())

    def shards_of(self, executor_id: str) -> list[Shard]:
        executor = self._executor(executor_id)
        out: list[Shard] = []
        for bucket in self.manifest.shards.values():
            out.extend(s for s in bucket.values() if s.executor_id == executor.executor_id)
        return sorted(out, key=lambda s: (s.job_id, s.shard_id))

    def filesystem_info(self) -> dict:
        return {
            "originals_dir": _fs_info(self.cfg.originals_dir),
            "executors": {
                eid: _fs_info(ex.dir) for eid, ex in self.manifest.executors.items()
            },
            "note": "different 'device' values mean different mounted file systems",
        }

    def reset(self) -> None:
        """Test-only: wipe data and return to the empty initial state."""
        with self._lock:
            for directory in list(self.cfg.executor_dirs.values()) + [
                self.cfg.originals_dir
            ]:
                self._clear_source_dir(directory)
            self.manifest.shards.clear()
            self.manifest.next_shard.clear()
            for executor in self.manifest.executors.values():
                executor.state = STATE_ACTIVE
            self._persist_locked()
