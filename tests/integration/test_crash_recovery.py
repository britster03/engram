"""Crash-recovery tests (§5.5 and §5.6).

Validate that process_event is idempotent and the reconciliation worker
requeues every stuck state described in §5.5's recovery table.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.consolidation.reconciliation import ReconciliationContext, run_once
from engram.ingest.worker import IngestContext, process_event
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore
from engram.uri import pair_id as pair_id_fn

from .providers import DeterministicCoreProvider, DeterministicEmbeddingService


class CountingCoreProvider(DeterministicCoreProvider):
    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()

    def complete(self, **kwargs):  # type: ignore[no-untyped-def,override]
        prompt = str(kwargs.get("system_prompt") or "")
        tag = prompt.split("]", 1)[0].lstrip("[") if prompt.startswith("[") else ""
        self.calls[tag] += 1
        return super().complete(**kwargs)


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "ollama_cloud", "api_key": "x"},
        "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
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


@pytest.mark.parametrize(
    "crash_stage",
    [
        "GATED",
        "EXTRACTED",
        "LINKED",
        "FILESYSTEM_COMMITTED",
        "KG_COMMITTED",
        "CONSOLIDATION_COMMITTED",
        "COMPLETE",
    ],
)
def test_crash_after_every_committed_stage_resumes_without_model_replay(
    cfg: EngramConfig,
    crash_stage: str,
):
    ingest, sqlite, neo, _fs = _ctx(cfg)
    core = CountingCoreProvider()
    ingest.core = core  # type: ignore[assignment]
    eid = _enqueue_event(
        sqlite,
        f"crash-{crash_stage}",
        "I moved to Berlin.",
        "That is a significant move.",
        0,
    )
    crashed = False

    def inject(stage: str) -> None:
        nonlocal crashed
        if stage == crash_stage and not crashed:
            crashed = True
            raise RuntimeError(f"injected after {stage}")

    ingest.stage_hook = inject
    with pytest.raises(RuntimeError, match=f"injected after {crash_stage}"):
        process_event(ingest, eid)
    calls_after_crash = core.calls.copy()

    ingest.stage_hook = None
    assert process_event(ingest, eid) == "COMPLETE"
    # Any nondeterministic stages which committed before the crash are never called again.
    if crash_stage != "GATED":
        assert core.calls["EXTRACT"] == calls_after_crash["EXTRACT"]
    assert core.calls["GATE"] == calls_after_crash["GATE"]
    if crash_stage in {
        "LINKED",
        "FILESYSTEM_COMMITTED",
        "KG_COMMITTED",
        "CONSOLIDATION_COMMITTED",
        "COMPLETE",
    }:
        assert core.calls["LINK"] == calls_after_crash["LINK"]

    artifacts = sqlite.list_ingest_artifacts(eid, tenant_id="_default")
    assert artifacts
    assert all(row["filesystem_state"] == "COMMITTED" for row in artifacts)
    assert all(row["kg_state"] == "COMMITTED" for row in artifacts)
    file_snapshot = {
        str(path.relative_to(Path(cfg.filesystem.data_dir))): path.read_text(encoding="utf-8")
        for path in Path(cfg.filesystem.data_dir).rglob("*.md")
    }
    graph_snapshot = json.dumps(
        {"nodes": neo.nodes, "edges": neo.edges}, sort_keys=True, separators=(",", ":")
    )
    call_snapshot = core.calls.copy()

    assert process_event(ingest, eid) == "COMPLETE"
    assert core.calls == call_snapshot
    assert file_snapshot == {
        str(path.relative_to(Path(cfg.filesystem.data_dir))): path.read_text(encoding="utf-8")
        for path in Path(cfg.filesystem.data_dir).rglob("*.md")
    }
    assert graph_snapshot == json.dumps(
        {"nodes": neo.nodes, "edges": neo.edges}, sort_keys=True, separators=(",", ":")
    )


def test_reconciliation_leaves_old_received_backlog_claimable(cfg: EngramConfig):
    _ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s2", "I live in Chicago.", "Got it.", 0)
    # RECEIVED is already claimable. Age alone must not manufacture a retry.
    with sqlite.transaction() as conn:
        conn.execute(
            "UPDATE events SET created_at = datetime('now', '-15 minutes') "
            "WHERE event_id = ?",
            (eid,),
        )
    counts = run_once(ReconciliationContext(cfg=cfg, sqlite=sqlite))
    assert counts["received_stuck"] == 0
    refreshed = sqlite.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"
    assert refreshed["retry_count"] == 0


def test_reconciliation_retries_index_failed(cfg: EngramConfig):
    _ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s3", "I moved to Paris.", "Noted.", 0)
    # Put fs_outbox into INDEX_FAILED with retry_count < 3.
    with sqlite.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO fs_outbox (event_id, source_uri, state, retry_count, written_at) "
            "VALUES (?, 'mem://user/episodes/x.md', 'INDEX_FAILED', 1, datetime('now'))",
            (eid,),
        )
    counts = run_once(ReconciliationContext(cfg=cfg, sqlite=sqlite))
    assert counts["index_failed_retried"] == 1
    refreshed = sqlite.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"


def test_gated_store_without_extraction_requeues(cfg: EngramConfig):
    _ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s4", "I own a dog named Rex.", "Cute.", 0)
    with sqlite.transaction() as conn:
        conn.execute(
            "UPDATE events SET status = 'GATED_STORE', "
            "processed_at = datetime('now', '-10 minutes') WHERE event_id = ?",
            (eid,),
        )
    counts = run_once(ReconciliationContext(cfg=cfg, sqlite=sqlite))
    assert counts["gated_store_stuck"] == 1
    refreshed = sqlite.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"


def test_fresh_gated_store_is_not_stolen_from_active_worker(cfg: EngramConfig):
    _ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s4-active", "I own a dog named Rex.", "Cute.", 0)
    with sqlite.transaction() as conn:
        conn.execute(
            "UPDATE events SET status = 'GATED_STORE', processed_at = datetime('now') "
            "WHERE event_id = ?",
            (eid,),
        )

    counts = run_once(ReconciliationContext(cfg=cfg, sqlite=sqlite))

    assert counts["gated_store_stuck"] == 0
    refreshed = sqlite.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "GATED_STORE"
    assert refreshed["retry_count"] == 0


def test_stale_gated_store_with_extraction_resumes_from_committed_stage(
    cfg: EngramConfig,
) -> None:
    _ingest, sqlite, _, _ = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s4-extracted", "I own a dog named Rex.", "Cute.", 0)
    sqlite.save_extraction(
        eid,
        resolved_text="User owns a dog named Rex.",
        triplets=[{
            "subject": "user",
            "relation": "owns_pet_named",
            "object": "Rex",
            "confidence": 0.95,
        }],
        l0_abstract="User owns a dog named Rex.",
    )
    with sqlite.transaction() as conn:
        conn.execute(
            "UPDATE event_stage_state SET completed_stage = 'EXTRACTED' "
            "WHERE event_id = ?",
            (eid,),
        )
        conn.execute(
            "UPDATE events SET status = 'GATED_STORE', "
            "processed_at = datetime('now', '-10 minutes') WHERE event_id = ?",
            (eid,),
        )

    counts = run_once(ReconciliationContext(cfg=cfg, sqlite=sqlite))

    assert counts["gated_store_stuck"] == 1
    refreshed = sqlite.get_event(eid)
    assert refreshed is not None
    assert refreshed["status"] == "RECEIVED"


def test_indexed_event_resumes_only_consolidation(cfg: EngramConfig):
    ingest, sqlite, _neo, _fs = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s5", "I moved to Berlin.", "Noted.", 0)
    crashed = False

    def crash_after_kg(stage: str) -> None:
        nonlocal crashed
        if stage == "KG_COMMITTED" and not crashed:
            crashed = True
            raise RuntimeError("injected after KG commit")

    ingest.stage_hook = crash_after_kg
    with pytest.raises(RuntimeError, match="injected after KG commit"):
        process_event(ingest, eid)
    state = sqlite.get_event_stage(eid, tenant_id="_default")
    assert state["completed_stage"] == "KG_COMMITTED"
    assert sqlite.queue_depth() == 0

    ingest.stage_hook = None
    assert process_event(ingest, eid) == "COMPLETE"
    event = sqlite.get_event(eid)
    assert event is not None and event["status"] == "COMPLETE"
    assert sqlite.queue_depth() > 0


def test_reconciliation_requeues_indexed_crash_window(cfg: EngramConfig):
    _ingest, sqlite, _neo, _fs = _ctx(cfg)
    eid = _enqueue_event(sqlite, "s6", "I moved to Rome.", "Noted.", 0)
    with sqlite.transaction() as conn:
        conn.execute(
            "UPDATE events SET status = 'INDEXED', processed_at = datetime('now', '-5 minutes') "
            "WHERE event_id = ?",
            (eid,),
        )
    counts = run_once(ReconciliationContext(cfg=cfg, sqlite=sqlite))
    assert counts["indexed_without_consolidation"] == 1
    event = sqlite.get_event(eid)
    assert event is not None and event["status"] == "RECEIVED"


def test_reconciliation_skips_graph_scan_without_local_tenants(cfg: EngramConfig):
    class UnexpectedGraph:
        def run_template(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("graph-only tenants must not create local refresh work")

    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)

    counts = run_once(
        ReconciliationContext(cfg=cfg, sqlite=sqlite, neo4j=UnexpectedGraph(), fs=fs)
    )

    assert counts["stale_overviews_enqueued"] == 0
    assert sqlite.queue_depth() == 0


def test_reconciliation_preserves_stale_directory_tenant_scope(cfg: EngramConfig):
    class StaleGraph:
        params: dict | None = None

        def run_template(self, cypher, params, **_kwargs):  # type: ignore[no-untyped-def]
            assert "d.tenant_id IN $tenant_ids" in cypher
            self.params = params
            return [
                {"uri": "mem://user/entities/alice", "tenant_id": "tenant-a"},
                {"uri": "mem://user/entities/bob", "tenant_id": "foreign-tenant"},
            ]

    fs = FilesystemStore(cfg.filesystem.data_dir)
    FilesystemStore(cfg.filesystem.data_dir, tenant_id="tenant-a").write_atomic(
        "mem://user/entities/alice/alice.md", "Alice."
    )
    sqlite = SqliteStore(cfg.event_ledger.path)
    graph = StaleGraph()

    counts = run_once(
        ReconciliationContext(cfg=cfg, sqlite=sqlite, neo4j=graph, fs=fs)
    )

    assert graph.params == {"tenant_ids": ["tenant-a"]}
    assert counts["stale_overviews_enqueued"] == 1
    rows = sqlite.get_conn().execute(
        "SELECT tenant_id, node_id FROM consolidation_tasks"
    ).fetchall()
    assert [(row["tenant_id"], row["node_id"]) for row in rows] == [
        ("tenant-a", "mem://user/entities/alice")
    ]
