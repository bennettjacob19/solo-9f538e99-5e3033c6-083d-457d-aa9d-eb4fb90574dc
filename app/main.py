"""FastAPI application for the local shuffle-handoff drill.

Endpoints
---------
POST /jobs/shards                 submit one text shard for one executor
GET  /jobs/{job_id}/word-count    merge per-shard word-count files
POST /executors/decommission      hand a leaving executor's shards to its peer
GET  /executors                   executor states and owned shards
GET  /filesystems                 prove the executor dirs are on different FS
POST /admin/reset                 test-only reset
"""
from __future__ import annotations

import os

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .schemas import (
    DecommissionRequest,
    DecommissionResponse,
    ExecutorStatus,
    FilesystemInfo,
    ShardInfo,
    ShardSubmit,
    WordCountResponse,
)
from .store import (
    Store,
    StoreError,
)


def create_app(store: Store | None = None) -> FastAPI:
    # The default store is built on startup (not import time) so importing
    # the module does not require the container's /data directories to exist.
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if app.state.store is None:
            app.state.store = Store()
        yield

    app = FastAPI(
        title="Shuffle Handoff Drill",
        version="1.0.0",
        description=(
            "Local MapReduce-style executor decommission drill: submit text "
            "shards, merge per-shard word counts, and hand shuffle files "
            "between executors mounted on different file systems."
        ),
        lifespan=lifespan,
    )
    app.state.store = store

    @app.exception_handler(StoreError)
    async def _store_error_handler(_: Request, exc: StoreError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": type(exc).__name__, "detail": str(exc)},
        )

    @app.post("/jobs/shards", response_model=ShardInfo, status_code=201)
    def submit_shard(payload: ShardSubmit) -> ShardInfo:
        shard = app.state.store.submit_shard(
            job_id=payload.job_id,
            shard_id=payload.shard_id,
            executor_id=payload.executor_id,
            text=payload.text,
        )
        return ShardInfo(**shard.to_info())

    @app.get("/jobs/{job_id}/word-count", response_model=WordCountResponse)
    def merge_word_counts(job_id: str) -> WordCountResponse:
        counts, detail = app.state.store.merge_word_counts(job_id)
        return WordCountResponse(
            job_id=job_id,
            word_counts=dict(sorted(counts.items())),
            shards_merged=len(detail),
            shard_detail=[ShardInfo(**s.to_info()) for s in detail],
        )

    @app.post("/executors/decommission", response_model=DecommissionResponse)
    def decommission(payload: DecommissionRequest) -> DecommissionResponse:
        result = app.state.store.decommission(
            executor_id=payload.executor_id,
            budget=payload.copy_byte_budget,
        )
        return DecommissionResponse(
            executor_id=payload.executor_id,
            peer_executor_id=result["peer_executor_id"],
            success=result["success"],
            reason=result["reason"],
            copied_bytes=result["copied_bytes"],
            copied_shard_ids=result["copied_shard_ids"],
            recomputed_shard_id=result["recomputed_shard_id"],
            planned_total_bytes=result["planned_total_bytes"],
            moved_shards=[ShardInfo(**s.to_info()) for s in result["moved"]],
        )

    @app.get("/executors", response_model=list[ExecutorStatus])
    def list_executors() -> list[ExecutorStatus]:
        out: list[ExecutorStatus] = []
        for executor in app.state.store.executor_status():
            out.append(
                ExecutorStatus(
                    executor_id=executor.executor_id,
                    state=executor.state,
                    dir=executor.dir,
                    device=os.stat(executor.dir).st_dev,
                    shards=[
                        ShardInfo(**s.to_info())
                        for s in app.state.store.shards_of(executor.executor_id)
                    ],
                )
            )
        return out

    @app.get("/filesystems", response_model=FilesystemInfo)
    def filesystems() -> FilesystemInfo:
        return FilesystemInfo(**app.state.store.filesystem_info())

    @app.post("/admin/reset", status_code=204)
    def reset() -> None:
        app.state.store.reset()

    return app


app = create_app()
