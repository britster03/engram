"""Graceful degradation — the orchestrator tolerates Neo4j/LLM failures."""

from __future__ import annotations

from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider
from engram.models.frontier import FrontierLLMProvider, FrontierVerdict
from engram.retrieval.orchestrator import OrchestratorContext, run_query
from engram.storage.filesystem import FilesystemStore

from engram.storage.memory_kg import InMemoryKnowledgeGraph
from .providers import DeterministicCoreProvider, DeterministicEmbeddingService, DeterministicFrontierProvider


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "anthropic", "api_key": "x"},
        "frontier_llm": {"provider": "anthropic", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "session_cache": {"backend": "memory"},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
        "retrieval": {"l0_skip": True},  # avoid needing real vectors
    })


class BrokenNeo(InMemoryKnowledgeGraph):
    def vector_search(self, *_a, **_k):  # type: ignore[override]
        raise RuntimeError("neo4j unavailable")

    def run_template(self, *_a, **_k):
        raise RuntimeError("neo4j unavailable")


class BrokenCore(CoreModelProvider):
    def complete(self, **_kwargs):
        raise CoreModelError("core model unavailable")


class BrokenFrontier(FrontierLLMProvider):
    def answer(self, **_kwargs) -> FrontierVerdict:
        raise CoreModelError("frontier unavailable")


def _ctx(cfg, *, neo, core, frontier) -> OrchestratorContext:
    fs = FilesystemStore(cfg.filesystem.data_dir)
    embed = DeterministicEmbeddingService()
    return OrchestratorContext(
        cfg=cfg, fs=fs, neo4j=neo,  # type: ignore[arg-type]
        core=core, frontier=frontier, embed=embed,  # type: ignore[arg-type]
    )


def test_query_succeeds_when_neo4j_down(cfg: EngramConfig):
    ctx = _ctx(
        cfg,
        neo=BrokenNeo(),
        core=DeterministicCoreProvider(),
        frontier=DeterministicFrontierProvider(),
    )
    result = run_query(ctx, session_id=None, query="Where does the user work?")
    # No crash; answer field populated (even if empty).
    assert result.answer is not None


def test_query_degrades_when_core_model_down(cfg: EngramConfig):
    ctx = _ctx(
        cfg,
        neo=InMemoryKnowledgeGraph(),
        core=BrokenCore(),
        frontier=DeterministicFrontierProvider(),
    )
    result = run_query(ctx, session_id=None, query="anything")
    # L1 plan fails → fallback plan; L1 vector returns [] because the FakeNeo
    # has no nodes; L2 plan fails → terminate_cascade. Either L1 or L2 is the
    # terminal depth. The point is we don't crash and the frontier still runs.
    assert result.retrieval_metadata.cascade_depth_reached in {"L0", "L1", "L2"}
    assert result.answer is not None


def test_query_returns_message_when_frontier_down(cfg: EngramConfig):
    ctx = _ctx(
        cfg,
        neo=InMemoryKnowledgeGraph(),
        core=DeterministicCoreProvider(),
        frontier=BrokenFrontier(),
    )
    result = run_query(ctx, session_id=None, query="anything")
    assert "temporarily unavailable" in (result.answer or "").lower()
