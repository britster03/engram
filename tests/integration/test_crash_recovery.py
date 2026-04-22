"""Crash-recovery tests (§5.5 and §5.6).

Validate that process_event is idempotent and the reconciliation worker
requeues every stuck state described in §5.5's recovery table.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.consolidation.reconciliation import ReconciliationContext, run_once
from engram.ingest.worker import IngestContext, process_event
from engram.storage.filesystem import FilesystemStore
from engram.storage.sqlite import SqliteStore
from engram.uri import pair_id as pair_id_fn

from engram.storage.memory_kg import InMemoryKnowledgeGraph
from .providers import DeterministicCoreProvider, DeterministicEmbeddingService


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "anthropic", "api_key": "x"},
        "frontier_llm": {"provider": "anthropic", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "consolidation": {"db_path": str(tmp_path / "cons.db")},
        "session_cache": {"backend": "memory"},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
    })


def _ctx(cfg: EngramConfig) -> tuple[IngestContext, SqliteStore, InMemoryKnowledgeGraph, FilesystemStore]:
    neo = InMemoryKnowledgeGraph()
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    return (
        IngestContext(
            cfg=cfg, sqlite=sqlite, fs=fs, neo4j=neo,  # type: ignore[arg-type]
            core=DeterministicCoreProvider(),  # type: ignore[arg-type]
            embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
        ),
        sqlite, neo, fs,
    )


def _enqueue_event(sqlite: SqliteStore, session_id: str, user: str, assistant: str, idx: int) -> str:
    pid = pair_id_fn(session_id, idx * 2, idx * 2 + 1)
    event_id, _ = sqlite.record_event(
        pair_id=pid,
        session_id=session_id,
        source="test",
        event_type="INGEST",
        payload={
            "turn_pair": {
                "user": {"content": user, "turn_idx": idx * 2},
                "assistant": {"content": assistant, "turn_idx": idx * 2 + 1},
            }
        },
    )
    return event_id


def test_process_event_is_idempotent_on_replay(cfg: EngramConfig):
    ingest, sqlite, neo, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s1", "I just accepted a job at Meta.", "Congrats!", 0)
    first = process_event(ingest, eid)
    nodes_first = len(neo.nodes)
    second = process_event(ingest, eid)
    assert first == second == "COMPLETE"
    assert len(neo.nodes) == nodes_first  # no duplication


def test_reconciliation_requeues_received_stuck(cfg: EngramConfig):
    ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s2", "I live in Chicago.", "Got it.", 0)
    # Force the event's created_at into the past so the reconciliation worker
    # classifies it as stuck.
    with sqlite.transaction() as conn:
        conn.execute(
            "UPDATE events SET created_at = datetime('now', '-15 minutes') "
            "WHERE event_id = ?",
            (eid,),
        )
    driven: list[str] = []
    def _drive(event_id: str) -> None:
        driven.append(event_id)
        process_event(ingest, event_id)
    counts = run_once(ReconciliationContext(cfg=cfg, sqlite=sqlite, drive_event=_drive))
    assert counts["received_stuck"] == 1
    assert driven == [eid]
    # After recovery, status is terminal
    refreshed = sqlite.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] in {"COMPLETE", "GATED_SKIP"}


def test_reconciliation_retries_index_failed(cfg: EngramConfig):
    ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s3", "I moved to Paris.", "Noted.", 0)
    # Put fs_outbox into INDEX_FAILED with retry_count < 3.
    with sqlite.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO fs_outbox (event_id, source_uri, state, retry_count, written_at) "
            "VALUES (?, 'mem://user/episodes/x.md', 'INDEX_FAILED', 1, datetime('now'))",
            (eid,),
        )
    driven: list[str] = []
    counts = run_once(
        ReconciliationContext(
            cfg=cfg, sqlite=sqlite, drive_event=driven.append
        )
    )
    assert counts["index_failed_retried"] == 1
    assert driven == [eid]


def test_gated_store_without_extraction_requeues(cfg: EngramConfig):
    ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s4", "I own a dog named Rex.", "Cute.", 0)
    with sqlite.transaction() as conn:
        conn.execute("UPDATE events SET status = 'GATED_STORE' WHERE event_id = ?", (eid,))
    driven: list[str] = []
    counts = run_once(
        ReconciliationContext(cfg=cfg, sqlite=sqlite, drive_event=driven.append)
    )
    assert counts["gated_store_stuck"] == 1
    assert driven == [eid]
