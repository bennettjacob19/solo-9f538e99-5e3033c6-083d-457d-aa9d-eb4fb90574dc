"""Runtime configuration.

The drill mounts the two executor directories on different file systems;
the original text lives in a third directory, outside both executor trees.
Every path is overridable through environment variables so the test-suite
and the docker-compose deployment can point at their own locations.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _path(env: str, default: str) -> str:
    return os.environ.get(env, default)


@dataclass(frozen=True)
class Settings:
    originals_dir: str = field(
        default_factory=lambda: _path("SHUFFLE_ORIGINALS_DIR", "/data/originals")
    )
    executor_dirs: dict[str, str] = field(
        default_factory=lambda: {
            "exec-a": _path("SHUFFLE_EXEC_A_DIR", "/data/exec-a"),
            "exec-b": _path("SHUFFLE_EXEC_B_DIR", "/data/exec-b"),
        }
    )
    manifest_path: str = field(
        default_factory=lambda: _path("SHUFFLE_MANIFEST", "/data/state/manifest.json")
    )
    # Artificial per-shard read delay, used by tests to widen the
    # merge/handoff race window. Seconds; 0 in normal operation.
    merge_read_delay: float = field(
        default_factory=lambda: float(os.environ.get("SHUFFLE_MERGE_READ_DELAY", "0"))
    )
    # Artificial delay between moving a shard in the manifest and the next
    # step, again only used by the concurrency tests.
    handoff_step_delay: float = field(
        default_factory=lambda: float(os.environ.get("SHUFFLE_HANDOFF_STEP_DELAY", "0"))
    )


settings = Settings()
