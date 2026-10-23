"""Response models mirror the server's Pydantic schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ModelTokenCall(BaseModel):
    stage: str
    provider: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    estimated: bool = False


class QueryTokenUsage(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    estimated: bool = False
    query_tokens: int = 0
    session_context_tokens: int = 0
    retrieval_context_tokens: int = 0
    model_calls: list[ModelTokenCall] = Field(default_factory=list)


class RetrievalMetadata(BaseModel):
    cascade_depth_reached: str
    levels_visited: list[str]
    predicted_depth: str | None = None
    nodes_retrieved: int = 0
    total_context_tokens: int = 0
    reentries: int = 0
    latency_ms: dict[str, float] = {}
    l0_decision: str | None = None
    l0_reason: str | None = None
    retrieval_route: str | None = None
    routes_attempted: list[str] = Field(default_factory=list)
    answer_mode: str | None = None
    stop_reason: str | None = None
    source_records: list[dict[str, Any]] = Field(default_factory=list)
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    degraded_sources: list[str] = Field(default_factory=list)
    token_usage: QueryTokenUsage = Field(default_factory=QueryTokenUsage)
    quality_policy: str | None = None
    sufficiency_decisions: list[dict[str, Any]] = Field(default_factory=list)
    evidence_diagnostics: list[dict[str, Any]] = Field(default_factory=list)
    packed_evidence_ids: list[str] = Field(default_factory=list)
    declared_support: list[dict[str, Any]] = Field(default_factory=list)
    avoided_model_calls: int = 0
    provider_attempts: list[dict[str, Any]] = Field(default_factory=list)


class QueryResponse(BaseModel):
    answer: str
    session_id: str | None = None
    retrieval_metadata: RetrievalMetadata


class IngestResponse(BaseModel):
    event_id: str
    pair_id: str
    status: str


class SessionState(BaseModel):
    session_id: str
    status: str
    turn_count: int
    created_at: str
    compacted_turns: int
    key_facts: list[str]


class TenantPayload(BaseModel):
    tenant_id: str
    display_name: str
    status: str
    created_at: str
    quotas: dict[str, int]
    api_key_count: int


class HealthPayload(BaseModel):
    status: str
    components: dict[str, bool]


class ConsolidationStatus(BaseModel):
    queue_depth: int
    by_task_type: dict[str, int]
    by_status: dict[str, int]
