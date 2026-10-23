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
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.uri import pair_id as pair_id_fn
from tests.postgres_support import PostgresTestStore

from .providers import DeterministicCoreProvider, DeterministicEmbeddingService


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate(
        {
            "api": {"api_key": "test-key"},
            "core_model": {"provider": "openai_responses", "api_key": "x"},
            "frontier_llm": {"provider": "openai_responses", "api_key": "x"},
            "filesystem": {"data_dir": str(tmp_path / "mem")},
            "event_ledger": {"dsn": "postgresql://test:test/test"},
            "session_cache": {"backend": "memory"},
            "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
        }
    )


def _ctx(
    cfg: EngramConfig,
) -> tuple[IngestContext, PostgresTestStore, InMemoryKnowledgeGraph, FilesystemStore]:
    neo = InMemoryKnowledgeGraph()
    store = PostgresTestStore()
    fs = FilesystemStore(cfg.filesystem.data_dir)
    return (
        IngestContext(
            cfg=cfg,
            control_plane=store,
            fs=fs,
            neo4j=neo,  # type: ignore[arg-type]
            core=DeterministicCoreProvider(),  # type: ignore[arg-type]
            embed=DeterministicEmbeddingService(),  # type: ignore[arg-type]
        ),
        store,
        neo,
        fs,
    )


def _enqueue_event(
    store: PostgresTestStore, session_id: str, user: str, assistant: str, idx: int
) -> str:
    pid = pair_id_fn(session_id, idx * 2, idx * 2 + 1)
    event_id, _ = store.record_event(
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
    ingest, store, neo, _ = _ctx(cfg)
    eid = _enqueue_event(store, "s1", "I just accepted a job at Meta.", "Congrats!", 0)
    first = process_event(ingest, eid)
    nodes_first = len(neo.nodes)
    second = process_event(ingest, eid)
    assert first == second == "COMPLETE"
    assert len(neo.nodes) == nodes_first  # no duplication


def test_reconciliation_requeues_received_stuck(cfg: EngramConfig):
    _ingest, store, _, _ = _ctx(cfg)
    eid = _enqueue_event(store, "s2", "I live in Chicago.", "Got it.", 0)
    # Force the event's created_at into the past so the reconciliation worker
    # classifies it as stuck.
    with store.transaction() as conn:
        conn.execute(
            "UPDATE events SET created_at = CURRENT_TIMESTAMP - INTERVAL '15 minutes' "
            "WHERE event_id = ?",
            (eid,),
        )
    counts = run_once(ReconciliationContext(cfg=cfg, control_plane=store))
    assert counts["received_stuck"] == 1
    refreshed = store.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"
    assert refreshed["retry_count"] == 1


def test_reconciliation_retries_index_failed(cfg: EngramConfig):
    _ingest, store, _, _ = _ctx(cfg)
    eid = _enqueue_event(store, "s3", "I moved to Paris.", "Noted.", 0)
    # Put fs_outbox into INDEX_FAILED with retry_count < 3.
    store.fs_outbox_write(eid, "mem://user/episodes/x.md")
    with store.transaction() as conn:
        conn.execute(
            "UPDATE fs_outbox "
            "SET state = 'INDEX_FAILED', retry_count = 1, written_at = CURRENT_TIMESTAMP "
            "WHERE event_id = ?",
            (eid,),
        )
    counts = run_once(ReconciliationContext(cfg=cfg, control_plane=store))
    assert counts["index_failed_retried"] == 1
    refreshed = store.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"


def test_gated_store_without_extraction_requeues(cfg: EngramConfig):
    _ingest, store, _, _ = _ctx(cfg)
    eid = _enqueue_event(store, "s4", "I own a dog named Rex.", "Cute.", 0)
    with store.transaction() as conn:
        conn.execute("UPDATE events SET status = 'GATED_STORE' WHERE event_id = ?", (eid,))
    counts = run_once(ReconciliationContext(cfg=cfg, control_plane=store))
    assert counts["gated_store_stuck"] == 1
    refreshed = store.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"


def test_reconciliation_can_skip_daily_stale_overview_scan(cfg: EngramConfig):
    """Frequent crash recovery must not invoke the daily Neo4j-wide scan."""
    _ingest, store, _neo, _ = _ctx(cfg)

    class Neo4jMustNotBeQueried:
        def run_template(self, *_args, **_kwargs):
            raise AssertionError("daily stale-overview scan should be skipped")

    counts = run_once(
        ReconciliationContext(cfg=cfg, control_plane=store, neo4j=Neo4jMustNotBeQueried()),
        scan_stale_overviews=False,
    )
    assert counts["stale_overviews_enqueued"] == 0


def test_indexed_event_resumes_only_consolidation_enqueue(cfg: EngramConfig):
    ingest, store, _neo, _fs = _ctx(cfg)
    eid = _enqueue_event(
        store,
        "s5",
        "I joined Example Corp.",
        "Congratulations!",
        0,
    )
    assert process_event(ingest, eid) == "COMPLETE"
    with store.transaction() as conn:
        conn.execute("DELETE FROM consolidation_tasks")
        conn.execute(
            "UPDATE events SET status = 'INDEXED' WHERE event_id = ?",
            (eid,),
        )

    assert process_event(ingest, eid) == "COMPLETE"
    assert store.queue_depth() > 0
