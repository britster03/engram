"""Config loader with ${VAR} interpolation matching §15 of the SDD."""

from __future__ import annotations

import os
import re
import warnings
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, Field, field_validator, model_validator

_ENV_REF = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")
CONFIG_ENV_SETTINGS = {
    "ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS": ("session", "compaction_trigger_tokens"),
    "ENGRAM_SESSION_COMPACTION_TARGET_RATIO": ("session", "compaction_target_ratio"),
    "ENGRAM_SESSION_COMPACTION_INPUT_TOKENS": ("session", "compaction_input_tokens"),
    "ENGRAM_SESSION_COMPACTION_SUMMARY_TOKENS": ("session", "compaction_summary_tokens"),
    "ENGRAM_RETRIEVAL_QUALITY_MODE": ("retrieval", "quality_mode"),
}


def configuration_environment(path: str | Path, *, env_file: str | Path | None = None) -> dict[str, str | None]:
    """Read one dotenv file without exporting it; process values take precedence."""
    explicit = env_file if env_file is not None else os.environ.get("ENGRAM_ENV_FILE")
    if explicit is not None and not str(explicit).strip():
        raise ValueError("ENGRAM_ENV_FILE must not be empty")
    selected = Path(explicit) if explicit is not None else Path(path).resolve().parent / ".env"
    if explicit is not None and not selected.is_file():
        raise FileNotFoundError(f"environment file not found: {selected}")
    values = dotenv_values(selected, interpolate=False) if selected.is_file() else {}
    return {**values, **os.environ}


def apply_config_environment(raw: dict[str, Any], env: Mapping[str, str | None]) -> dict[str, Any]:
    """Overlay only documented nonsecret settings; useful before freezing benchmarks."""
    resolved = {**raw}
    for name, (section, key) in CONFIG_ENV_SETTINGS.items():
        if name in env:
            value = env[name]
            if value is None or not value.strip():
                raise ValueError(f"{name} must not be empty")
            resolved[section] = {**resolved.get(section, {}), key: value}
    return resolved


def _interpolate(value: Any, env: Mapping[str, str | None] | None = None) -> Any:
    """Recursively resolve ${ENV_VAR} references. Unset vars become None."""
    env = os.environ if env is None else env
    if isinstance(value, str):
        match = _ENV_REF.fullmatch(value)
        if match:
            return env.get(match.group(1))
        return _ENV_REF.sub(lambda m: env.get(m.group(1)) or "", value)
    if isinstance(value, dict):
        return {k: _interpolate(v, env) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v, env) for v in value]
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
    "openai",
    "openai_compat",
    "ollama",
    "groq",
    "gemini",
    "openrouter",
    "together",
    "deepseek",
)


class CoreModelConfig(BaseModel):
    # local | Ollama | any OpenAI-compatible provider (§engram.models.providers)
    provider: Literal[
        "local",
        "openai",
        "openai_compat",
        "openai_responses",
        "ollama",
        "groq",
        "gemini",
        "openrouter",
        "together",
        "deepseek",
    ] = "openai_responses"
    model_path: str = "muse-spark-1.3-contributor"
    api_base: str | None = None
    api_key: str | None = None
    temperature: float = 1.0
    max_tokens: int = 2048
    # Responses models count hidden reasoning against max_output_tokens. Keep
    # the effort low so structured calls retain budget for their JSON result.
    reasoning_effort: Literal["minimal", "low", "medium", "high", "xhigh"] | None = "low"
    timeout_seconds: int = 60


class FrontierLlmConfig(BaseModel):
    provider: Literal[
        "openai",
        "openai_compat",
        "openai_responses",
        "ollama",
        "groq",
        "gemini",
        "openrouter",
        "together",
        "deepseek",
    ] = "openai_responses"
    model_path: str = "muse-spark-1.3-contributor"
    api_base: str | None = None
    api_key: str | None = None
    temperature: float = 1.0
    max_tokens: int = 4096
    # Muse Spark's low reasoning effort leaves enough output budget for JSON.
    reasoning_effort: Literal["minimal", "low", "medium", "high", "xhigh"] | None = "low"


class FilesystemConfig(BaseModel):
    data_dir: str = "./data/mem"
    create_dirs: bool = True
    tree_display_max_tokens: int = 2048


class CanonicalMemoryConfig(BaseModel):
    """PostgreSQL-canonical V2 runtime controls.

    The compatibility switch exists only to let the legacy test/development
    pipeline coexist during the migration. Production configuration enables
    V2 and requires Temporal; filesystem reads and writes are then forbidden
    for memory correctness.
    """

    enabled: bool = False
    require_temporal: bool = True
    allow_filesystem_runtime: bool = True
    legacy_uri_status_code: int = Field(default=410, ge=400, le=499)
    artifact_retention_hours: int = Field(default=24, ge=1, le=720)
    automatic_user_memory_enabled: bool = False
    evidence_mode: Literal["legacy", "enforce"] = "legacy"
    extraction_policy_version: str = Field(default="ingestion-v2", min_length=1, max_length=128)
    claim_verification_max_claims: int = Field(default=200, ge=1, le=1000)
    claim_verification_batch_size: int = Field(default=8, ge=1, le=32)
    claim_verification_input_tokens: int = Field(default=24_000, ge=1000, le=200_000)
    claim_verification_output_tokens: int = Field(default=2048, ge=256, le=16_384)
    recover_transient_failures: bool = False


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
    """PostgreSQL-backed durable control plane."""

    backend: Literal["postgres"] = "postgres"
    dsn: str | None = None
    reconciliation_interval_seconds: int = 60
    # Recovery checks must be frequent, but the Neo4j-wide stale-directory
    # scan is intentionally a maintenance job and must not run every pass.
    stale_overview_scan_interval_seconds: int = Field(default=86_400, ge=60)

    @model_validator(mode="after")
    def _postgres_dsn_required(self):
        if not self.dsn:
            raise ValueError("event_ledger.dsn is required")
        return self


class TemporalConfig(BaseModel):
    """Temporal connection and worker settings.

    Workflow inputs intentionally contain identifiers only.  Conversation
    payloads remain in the control-plane database, not Temporal history.
    """

    enabled: bool = False
    address: str = "temporal:7233"
    namespace: str = "engram-prod"
    ingest_task_queue: str = "engram-ingest"
    code_ingest_task_queue: str = "engram-code-ingest"
    projection_task_queue: str = "engram-projection"
    consolidation_task_queue: str = "engram-consolidation"
    maintenance_task_queue: str = "engram-maintenance"
    worker_concurrency: int = Field(default=4, ge=1, le=64)
    maintenance_worker_concurrency: int = Field(default=1, ge=1, le=8)
    dispatch_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    history_retention_days: int = Field(default=14, ge=1, le=90)

    @field_validator(
        "ingest_task_queue",
        "code_ingest_task_queue",
        "projection_task_queue",
        "consolidation_task_queue",
        "maintenance_task_queue",
    )
    @classmethod
    def _task_queue_is_not_blank(cls, value: str) -> str:
        queue = value.strip()
        if not queue:
            raise ValueError("Temporal task queue names must not be blank")
        return queue

    @model_validator(mode="after")
    def _task_queues_are_distinct(self):
        queues = (
            self.ingest_task_queue,
            self.code_ingest_task_queue,
            self.projection_task_queue,
            self.consolidation_task_queue,
            self.maintenance_task_queue,
        )
        if len(queues) != len(set(queues)):
            raise ValueError("Temporal task queue names must be pairwise distinct")
        return self


class ConsolidationConfig(BaseModel):
    quality_mode: Literal["legacy", "enforce"] = "legacy"
    poll_interval_seconds: int = 10
    max_concurrent_tasks: int = 4
    overview_max_tokens: int = 2048
    overview_debounce_seconds: int = 30
    overview_max_children: int = Field(default=100, ge=1, le=1000)
    overview_input_tokens: int = Field(default=12000, ge=256, le=100000)
    manifest_update_delay_seconds: int = 5
    max_backlog: int = 10000


class SessionCacheConfig(BaseModel):
    backend: Literal["redis", "memory"] = "redis"
    redis_url: str = "redis://localhost:6379"


class RetrievalConfig(BaseModel):
    quality_mode: Literal["legacy", "shadow", "enforce"] = "legacy"
    query_deadline_seconds: float = Field(default=300, gt=0, le=300)
    provider_attempt_limit: int = Field(default=32, ge=1, le=64)
    sufficiency_max_assessments: int = Field(default=6, ge=0, le=6)
    sufficiency_input_tokens: int = Field(default=6000, ge=256, le=20000)
    sufficiency_output_tokens: int = Field(default=2048, ge=128, le=4096)
    conversation_candidates: int = Field(default=30, ge=1, le=30)
    l0_skip: bool = False
    session_budget_tokens: int = Field(default=6000, ge=256, le=20000)
    past_session_limit: int = Field(default=8, ge=1, le=30)
    graph_max_hops: int = Field(default=2, ge=1, le=4)
    graph_timeout_seconds: int = Field(default=3, ge=1, le=30)
    max_reentries: int = 2
    max_depth: Literal["L0", "L1", "L2", "L3", "L4"] = "L4"
    max_l1_vector_results: int = Field(default=12, ge=1, le=200)
    hybrid_enabled: bool = True
    hybrid_candidates_per_channel: int = Field(default=20, ge=1, le=100)
    hybrid_timeout_seconds: float = Field(default=3.0, gt=0, le=30)
    rrf_k: int = Field(default=60, ge=1, le=1000)
    overview_budget_tokens: int = 6000
    full_doc_budget_tokens: int = 20000
    msc_compression_threshold: float = 0.8


class DecayConfig(BaseModel):
    preset: Literal["personal_conversation", "coding_agent", "knowledge_base"] = (
        "personal_conversation"
    )
    schedule: str = "0 3 * * *"
    schedule_timezone: str = "UTC"
    dormant_floor: float = 0.05


class SessionConfig(BaseModel):
    timeout_minutes: int = 30
    compaction_trigger_tokens: int = Field(default=64000, ge=256)
    compaction_target_ratio: float = Field(default=0.5, gt=0, lt=1)
    compaction_input_tokens: int = Field(default=64000, ge=256)
    compaction_summary_tokens: int = Field(default=2000, ge=16)
    window_threshold_ratio: float = 0.4
    max_turns_before_window: int = 50

    @field_validator("compaction_trigger_tokens", "compaction_input_tokens", "compaction_summary_tokens", mode="before")
    @classmethod
    def _integer_token_budget(cls, value):
        if isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
            return int(value)
        if type(value) is not int:
            raise ValueError("compaction token budgets must be integers")
        return value

    @model_validator(mode="before")
    @classmethod
    def _deprecated_turn_settings(cls, value):
        if isinstance(value, dict) and {"window_threshold_ratio", "max_turns_before_window"} & value.keys():
            warnings.warn("session turn/character compaction triggers are deprecated and ignored; use compaction_trigger_tokens", DeprecationWarning, stacklevel=2)
        return value

    @model_validator(mode="after")
    def _compaction_headroom(self):
        if int(self.compaction_trigger_tokens * self.compaction_target_ratio) <= self.compaction_summary_tokens + 32:
            raise ValueError("compaction target must exceed summary budget plus 32 tokens of framing")
        if self.compaction_input_tokens <= self.compaction_summary_tokens + 256:
            raise ValueError("compaction input budget must exceed summary budget plus prompt overhead")
        return self


class EngramConfig(BaseModel):
    api: ApiConfig = Field(default_factory=ApiConfig)
    gating: GatingConfig = Field(default_factory=GatingConfig)
    core_model: CoreModelConfig = Field(default_factory=CoreModelConfig)
    frontier_llm: FrontierLlmConfig = Field(default_factory=FrontierLlmConfig)
    filesystem: FilesystemConfig = Field(default_factory=FilesystemConfig)
    canonical_memory: CanonicalMemoryConfig = Field(default_factory=CanonicalMemoryConfig)
    knowledge_graph: KnowledgeGraphConfig = Field(default_factory=KnowledgeGraphConfig)
    event_ledger: EventLedgerConfig = Field(default_factory=EventLedgerConfig)
    temporal: TemporalConfig = Field(default_factory=TemporalConfig)
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

    @model_validator(mode="after")
    def _canonical_runtime_is_durable(self):
        if self.canonical_memory.enabled:
            if self.canonical_memory.require_temporal and not self.temporal.enabled:
                raise ValueError("canonical_memory.enabled requires temporal.enabled")
            if self.canonical_memory.allow_filesystem_runtime:
                raise ValueError(
                    "canonical_memory.enabled requires "
                    "canonical_memory.allow_filesystem_runtime=false"
                )
        return self


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
    env_file: str | Path | None = None,
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
    env = configuration_environment(path, env_file=env_file)
    resolved = _interpolate(raw, env)
    if raw.get("_benchmark_config_frozen") is True:
        if not str(env.get("LOCOMO_PROJECT", "")).startswith("engram-lifecycle-"):
            raise ValueError("frozen benchmark configuration requires an isolated LoCoMo project")
    else:
        resolved = apply_config_environment(resolved, env)
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
