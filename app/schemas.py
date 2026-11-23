"""Pydantic request/response models for the shuffle drill API."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ShardSubmit(BaseModel):
    job_id: str = Field(..., min_length=1, examples=["job-1"])
    shard_id: Optional[int] = Field(
        default=None,
        ge=0,
        description="Shard number within the job; auto-assigned when omitted.",
    )
    executor_id: str = Field(..., examples=["exec-a"])
    text: str = Field(..., description="Raw text shard; whitespace is the tokenizer.")


class ShardInfo(BaseModel):
    job_id: str
    shard_id: int
    executor_id: str
    path: str
    original_path: str
    size_bytes: int


class WordCountResponse(BaseModel):
    job_id: str
    word_counts: dict[str, int]
    shards_merged: int
    shard_detail: list[ShardInfo]


class DecommissionRequest(BaseModel):
    executor_id: str = Field(..., examples=["exec-a"])
    copy_byte_budget: int = Field(
        ...,
        ge=0,
        description="Maximum bytes allowed for copying intermediate (word-count) files.",
    )


class DecommissionResponse(BaseModel):
    executor_id: str
    peer_executor_id: str
    success: bool
    reason: Optional[str] = None
    copied_bytes: int
    copied_shard_ids: list[int]
    recomputed_shard_id: Optional[int] = None
    planned_total_bytes: int
    moved_shards: list[ShardInfo]


class ExecutorStatus(BaseModel):
    executor_id: str
    state: str
    dir: str
    device: int
    shards: list[ShardInfo]


class FilesystemInfo(BaseModel):
    originals_dir: dict
    executors: dict[str, dict]
    note: str
