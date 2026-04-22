"""End-to-end flow using stub LLMs + in-memory Neo4j.

This validates wiring (ingest pipeline → KG index → vector search → MSC → frontier)
without requiring Docker or an API key.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.ingest.worker import IngestContext, process_event
from engram.retrieval.orchestrator import OrchestratorContext, run_query
from engram.storage.filesystem import FilesystemStore
from engram.storage.sqlite import SqliteStore
from engram.uri import pair_id as pair_id_fn

from engram.storage.memory_kg import InMemoryKnowledgeGraph
from .providers import DeterministicCoreProvider, DeterministicEmbeddingService, DeterministicFrontierProvider


@pytest.fixture
def cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> EngramConfig:
    monkeypatch.setenv("ENGRAM_API_KEY", "test-key")
    monkeypatch.setenv("NEO4J_ADMIN_PASSWORD", "x")
    # Build a config without going through load_config / env-interpolation
    cfg = EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "anthropic", "api_key": "x"},
        "frontier_llm": {"provider": "anthropic", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "consolidation": {"db_path": str(tmp_path / "cons.db")},
        "session_cache": {"backend": "memory"},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
        # Stub embeddings produce random cosine scores, so the L0 memory-hit
        # fallback would always BYPASS. Skip L0 in tests; the gate has its own
        # dedicated test.
        "retrieval": {"l0_skip": True},
    })
    return cfg


def _build_contexts(cfg: EngramConfig):
    fake_neo = InMemoryKnowledgeGraph()
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    core = DeterministicCoreProvider()
    frontier = DeterministicFrontierProvider()
    embed = DeterministicEmbeddingService()
    ingest = IngestContext(cfg=cfg, sqlite=sqlite, fs=fs, neo4j=fake_neo,  # type: ignore[arg-type]
                           core=core, embed=embed)  # type: ignore[arg-type]
    orch = OrchestratorContext(cfg=cfg, fs=fs, neo4j=fake_neo,  # type: ignore[arg-type]
                               core=core, frontier=frontier, embed=embed)  # type: ignore[arg-type]
    return ingest, orch, sqlite, fake_neo


def test_ingest_gated_skip_on_pleasantry(cfg: EngramConfig):
    ingest_ctx, _, sqlite, _ = _build_contexts(cfg)
    session_id = "sess-gate"
    pid = pair_id_fn(session_id, 0, 1)
    event_id, _ = sqlite.record_event(
        pair_id=pid,
        session_id=session_id,
        source="test",
        event_type="INGEST",
        payload={
            "turn_pair": {
                "user": {"content": "thanks", "turn_idx": 0},
                "assistant": {"content": "ok", "turn_idx": 1},
            }
        },
    )
    final = process_event(ingest_ctx, event_id)
    assert final == "GATED_SKIP"


def test_full_flow_ingest_then_query(cfg: EngramConfig):
    ingest_ctx, orch_ctx, sqlite, neo = _build_contexts(cfg)
    session_id = "sess-e2e"
    pairs = [
        ("I just accepted a job at Meta on the recommendations team.", "Congrats!"),
        ("I will be in Menlo Park by April 28th.", "Noted."),
        ("My wife's birthday is July 12th.", "Got it."),
    ]
    for idx, (user_msg, asst_msg) in enumerate(pairs):
        pid = pair_id_fn(session_id, idx * 2, idx * 2 + 1)
        event_id, _ = sqlite.record_event(
            pair_id=pid,
            session_id=session_id,
            source="test",
            event_type="INGEST",
            payload={
                "turn_pair": {
                    "user": {"content": user_msg, "turn_idx": idx * 2},
                    "assistant": {"content": asst_msg, "turn_idx": idx * 2 + 1},
                },
            },
        )
        assert process_event(ingest_ctx, event_id) == "COMPLETE"

    # KG should have episode nodes and some entity nodes.
    episode_count = sum(1 for n in neo.nodes.values() if n.get("node_type") == "DOCUMENT")
    entity_count = sum(1 for n in neo.nodes.values() if n.get("node_type") == "ENTITY")
    assert episode_count == 3
    assert entity_count >= 3

    # Query it — the stub frontier echoes retrieved sentences; we just check the pipeline runs.
    result = run_query(orch_ctx, session_id=None, query="Where does the user work?")
    md = result.retrieval_metadata.to_dict()
    # Stub LN-plan returns terminate_cascade=true so L2 is visited then the
    # cascade stops. L0 is visited first (skip=True → CONTINUE, still logged).
    assert md["cascade_depth_reached"] in {"L1", "L2", "L4"}
    assert "L1" in md["levels_visited"]
    assert md["nodes_retrieved"] >= 1
    assert result.answer  # non-empty
