"""Process-wide wiring of Engram components.

Built lazily on first access so FastAPI can boot without hitting external
services (useful for unit tests and for /api/v1/health to report component
status without failing startup).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from engram.audit import AuditLog
from engram.cache import EmbeddingCache, OverviewCache, build_cache
from engram.config import EngramConfig, get_config
from engram.ingest.worker import IngestContext
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.models.frontier import FrontierLLMProvider
from engram.models.providers import build_providers
from engram.retrieval.orchestrator import OrchestratorContext
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.neo4j_store import Neo4jStore
from engram.storage.redis_cache import SessionCache
from engram.storage.sqlite import SqliteStore
from engram.tenancy import TenantRegistry


@dataclass
class AppState:
    cfg: EngramConfig
    sqlite: SqliteStore
    fs: FilesystemStore
    neo4j: Neo4jStore | InMemoryKnowledgeGraph
    session_cache: SessionCache
    core: CoreModelProvider
    frontier: FrontierLLMProvider
    embed: EmbeddingService
    tenant_registry: TenantRegistry
    audit: AuditLog
    embedding_cache: EmbeddingCache
    overview_cache: OverviewCache


_lock = threading.Lock()
_state: AppState | None = None


def build_state(cfg: EngramConfig | None = None) -> AppState:
    cfg = cfg or get_config()
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir, create_dirs=cfg.filesystem.create_dirs)
    if cfg.knowledge_graph.backend == "memory":
        neo4j: Neo4jStore | InMemoryKnowledgeGraph = InMemoryKnowledgeGraph()
    else:
        neo4j = Neo4jStore(cfg.knowledge_graph)
    session_cache = SessionCache(
        cfg.session_cache, default_ttl_seconds=cfg.session.timeout_minutes * 60
    )
    core, frontier = build_providers(cfg.core_model, cfg.frontier_llm)

    # Distributed caches — Redis when available, per-process fallback otherwise.
    redis_url = cfg.session_cache.redis_url if cfg.session_cache.backend == "redis" else None
    embed_cache = EmbeddingCache(build_cache(redis_url, namespace="embeddings"))
    overview_cache = OverviewCache(build_cache(redis_url, namespace="overviews"))
    embed = EmbeddingService.get(cfg.gating, cache=embed_cache)

    tenant_registry = TenantRegistry(cfg.event_ledger.path)
    tenant_registry.ensure_default(legacy_api_key=cfg.api.api_key)
    audit = AuditLog(cfg.event_ledger.path)
    return AppState(
        cfg=cfg,
        sqlite=sqlite,
        fs=fs,
        neo4j=neo4j,
        session_cache=session_cache,
        core=core,
        frontier=frontier,
        embed=embed,
        tenant_registry=tenant_registry,
        audit=audit,
        embedding_cache=embed_cache,
        overview_cache=overview_cache,
    )


def get_state() -> AppState:
    global _state
    with _lock:
        if _state is None:
            _state = build_state()
        return _state


def reset_state() -> None:
    global _state
    with _lock:
        if _state is not None:
            _state.neo4j.close()
        _state = None


def make_ingest_context(state: AppState) -> IngestContext:
    return IngestContext(
        cfg=state.cfg,
        sqlite=state.sqlite,
        fs=state.fs,
        neo4j=state.neo4j,
        core=state.core,
        embed=state.embed,
    )


def make_orchestrator_context(state: AppState) -> OrchestratorContext:
    return OrchestratorContext(
        cfg=state.cfg,
        fs=state.fs,
        neo4j=state.neo4j,
        core=state.core,
        frontier=state.frontier,
        embed=state.embed,
    )
