"""Config loader with ${VAR} interpolation matching §15 of the SDD."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator

_ENV_REF = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def _interpolate(value: Any) -> Any:
    """Recursively resolve ${ENV_VAR} references. Unset vars become None."""
    if isinstance(value, str):
        match = _ENV_REF.fullmatch(value)
        if match:
            return os.environ.get(match.group(1))
        return _ENV_REF.sub(lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


class ApiConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    api_key: str | None = None
    admin_key: str | None = None
    rate_limit_query_per_minute: int = 60
    rate_limit_ingest_per_minute: int = 600


class GatingConfig(BaseModel):
    configuration: Literal["dual", "unified"] = "dual"
    classifier_path: str = "./models/engram-gate-v1"
    embedding_model_path: str = "BAAI/bge-small-en-v1.5"
    classification_threshold: float = 0.3
    device: Literal["cpu", "cuda"] = "cpu"
    max_batch_size: int = 32
    memory_hit_threshold: float = 0.75


_OPENAI_COMPAT_PROVIDERS = (
    "openai", "openai_compat", "ollama", "groq", "gemini",
    "openrouter", "together", "deepseek",
)


class CoreModelConfig(BaseModel):
    # anthropic | local | any OpenAI-compatible provider (§engram.models.providers)
    provider: Literal[
        "local", "anthropic", "openai", "openai_compat", "ollama",
        "groq", "gemini", "openrouter", "together", "deepseek",
    ] = "anthropic"
    model_path: str = "claude-sonnet-4-6"
    api_base: str | None = None
    api_key: str | None = None
    temperature: float = 0.1
    max_tokens: int = 2048
    timeout_seconds: int = 30


class FrontierLlmConfig(BaseModel):
    provider: Literal[
        "anthropic", "openai", "openai_compat", "ollama",
        "groq", "gemini", "openrouter", "together", "deepseek",
    ] = "anthropic"
    model_path: str = "claude-sonnet-4-6"
    api_base: str | None = None
    api_key: str | None = None
    temperature: float = 0.3
    max_tokens: int = 4096


class FilesystemConfig(BaseModel):
    data_dir: str = "./data/mem"
    create_dirs: bool = True
    tree_display_max_tokens: int = 2048


class KnowledgeGraphConfig(BaseModel):
    backend: Literal["neo4j", "memory"] = "neo4j"
    uri: str = "bolt://localhost:7687"
    writer_username: str = "engram_writer"
    writer_password: str | None = None
    reader_username: str = "engram_reader"
    reader_password: str | None = None
    l2_query_timeout_seconds: int = 5
    l4_query_timeout_seconds: int = 15
    max_hops: int = 4


class EventLedgerConfig(BaseModel):
    backend: Literal["sqlite"] = "sqlite"
    path: str = "./data/event_ledger.db"
    reconciliation_interval_seconds: int = 60


class ConsolidationConfig(BaseModel):
    db_path: str = "./data/consolidation.db"
    poll_interval_seconds: int = 10
    max_concurrent_tasks: int = 4
    overview_max_tokens: int = 2048
    overview_debounce_seconds: int = 30
    manifest_update_delay_seconds: int = 5
    max_backlog: int = 10000


class SessionCacheConfig(BaseModel):
    backend: Literal["redis", "memory"] = "redis"
    redis_url: str = "redis://localhost:6379"


class RetrievalConfig(BaseModel):
    l0_skip: bool = False
    max_reentries: int = 2
    max_depth: Literal["L0", "L1", "L2", "L3", "L4"] = "L4"
    max_l1_vector_results: int = 30
    overview_budget_tokens: int = 6000
    full_doc_budget_tokens: int = 20000
    msc_compression_threshold: float = 0.8


class DecayConfig(BaseModel):
    preset: Literal["personal_conversation", "coding_agent", "knowledge_base"] = (
        "personal_conversation"
    )
    schedule: str = "0 3 * * *"
    dormant_floor: float = 0.05


class SessionConfig(BaseModel):
    timeout_minutes: int = 30
    window_threshold_ratio: float = 0.4
    max_turns_before_window: int = 50


class EngramConfig(BaseModel):
    api: ApiConfig = Field(default_factory=ApiConfig)
    gating: GatingConfig = Field(default_factory=GatingConfig)
    core_model: CoreModelConfig = Field(default_factory=CoreModelConfig)
    frontier_llm: FrontierLlmConfig = Field(default_factory=FrontierLlmConfig)
    filesystem: FilesystemConfig = Field(default_factory=FilesystemConfig)
    knowledge_graph: KnowledgeGraphConfig = Field(default_factory=KnowledgeGraphConfig)
    event_ledger: EventLedgerConfig = Field(default_factory=EventLedgerConfig)
    consolidation: ConsolidationConfig = Field(default_factory=ConsolidationConfig)
    session_cache: SessionCacheConfig = Field(default_factory=SessionCacheConfig)
    retrieval: RetrievalConfig = Field(default_factory=RetrievalConfig)
    decay: DecayConfig = Field(default_factory=DecayConfig)
    session: SessionConfig = Field(default_factory=SessionConfig)

    @field_validator("api")
    @classmethod
    def _api_key_required(cls, v: ApiConfig) -> ApiConfig:
        if not v.api_key:
            raise ValueError("api.api_key is required (set ENGRAM_API_KEY)")
        return v


_PLACEHOLDER_TOKENS = (
    "change-me",
    "CHANGE_ME",
    "REPLACE_ME",
    "your-api-key",
    "xxx",
    "changeme",
)


def _detect_placeholders(cfg: EngramConfig) -> list[str]:
    """Return human-readable paths of config fields that still hold placeholder values."""
    issues: list[str] = []
    secrets = {
        "api.api_key": cfg.api.api_key,
        "core_model.api_key": cfg.core_model.api_key,
        "frontier_llm.api_key": cfg.frontier_llm.api_key,
        "knowledge_graph.writer_password": cfg.knowledge_graph.writer_password,
        "knowledge_graph.reader_password": cfg.knowledge_graph.reader_password,
    }
    for name, value in secrets.items():
        if not value:
            continue
        s = str(value)
        low = s.lower()
        if any(tok.lower() in low for tok in _PLACEHOLDER_TOKENS):
            issues.append(name)
    return issues


def load_config(
    path: str | Path | None = None,
    *,
    enforce_no_placeholders: bool = True,
) -> EngramConfig:
    """Load config.yaml with env interpolation.

    Refuses to load when any required secret still carries a placeholder
    value (e.g. ``change-me-before-exposing-this-port``). Set
    ``enforce_no_placeholders=False`` in tests where placeholders are fine.
    """
    if path is None:
        path = os.environ.get("ENGRAM_CONFIG_PATH", "./config.yaml")
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"config not found: {path}")
    raw = yaml.safe_load(path.read_text())
    resolved = _interpolate(raw)
    cfg = EngramConfig.model_validate(resolved)
    if enforce_no_placeholders:
        placeholders = _detect_placeholders(cfg)
        if placeholders:
            raise ValueError(
                "placeholder secret values detected; set real secrets before booting: "
                + ", ".join(placeholders)
            )
    return cfg


_cached: EngramConfig | None = None


def get_config() -> EngramConfig:
    """Process-wide singleton. Tests may call `reset_config` to rebuild."""
    global _cached
    if _cached is None:
        _cached = load_config()
    return _cached


def reset_config() -> None:
    global _cached
    _cached = None
