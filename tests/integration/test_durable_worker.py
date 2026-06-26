"""Durable ingest worker — polling behaviour, idempotency, shutdown."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.ingest.durable_worker import DurableIngestContext, DurableIngestWorker
from engram.ingest.worker import IngestContext
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore
from engram.uri import pair_id as pair_id_fn

from .providers import DeterministicCoreProvider, DeterministicEmbeddingService


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


def _build_worker(cfg: EngramConfig, *, concurrency: int = 2) -> tuple[DurableIngestWorker, SqliteStore, InMemoryKnowledgeGraph]:
    sqlite = SqliteStore(cfg.event_ledger.path)
    fs = FilesystemStore(cfg.filesystem.data_dir)
    neo = InMemoryKnowledgeGraph()
    embed = DeterministicEmbeddingService()
    core = DeterministicCoreProvider()

    def _factory() -> IngestContext:
        return IngestContext(
            cfg=cfg, sqlite=sqlite, fs=fs, neo4j=neo,  # type: ignore[arg-type]
            core=core, embed=embed,  # type: ignore[arg-type]
        )

    dctx = DurableIngestContext(
        cfg=cfg, sqlite=sqlite, fs=fs, neo4j=neo,  # type: ignore[arg-type]
        core=core, embed=embed,  # type: ignore[arg-type]
        ingest_context_factory=_factory,
    )
    worker = DurableIngestWorker(
        dctx, max_concurrent=concurrency, poll_interval_seconds=0.05, batch_size=16,
    )
    return worker, sqlite, neo


def _enqueue(sqlite: SqliteStore, session_id: str, idx: int, user: str, asst: str) -> str:
    pid = pair_id_fn(session_id, idx * 2, idx * 2 + 1)
    eid, _ = sqlite.record_event(
        pair_id=pid, session_id=session_id, source="test", event_type="INGEST",
        payload={
            "turn_pair": {
                "user": {"content": user, "turn_idx": idx * 2},
                "assistant": {"content": asst, "turn_idx": idx * 2 + 1},
            }
        },
    )
    return eid


def _wait_for_terminal(sqlite: SqliteStore, event_ids: list[str], *, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        pending = 0
        for eid in event_ids:
            ev = sqlite.get_event(eid)
            if ev is None:
                continue
            if ev["status"] not in {"COMPLETE", "GATED_SKIP", "FAILED", "INDEXED"}:
                pending += 1
        if pending == 0:
            return
        time.sleep(0.05)
    raise AssertionError("events did not reach terminal state in time")


def test_worker_drains_pending_events(cfg: EngramConfig):
    worker, sqlite, _neo = _build_worker(cfg)
    event_ids = []
    for i in range(3):
        eid = _enqueue(sqlite, "s", i,
                       f"I moved to city #{i}", "Noted.")
        event_ids.append(eid)
    worker.start()
    try:
        _wait_for_terminal(sqlite, event_ids, timeout_s=5.0)
    finally:
        worker.stop(timeout_s=3.0)
    # Every event has a terminal status
    for eid in event_ids:
        ev = sqlite.get_event(eid)
        assert ev is not None
        assert ev["status"] in {"COMPLETE", "GATED_SKIP"}


def test_claim_batch_is_shared_across_worker_instances(cfg: EngramConfig):
    worker1, sqlite, _neo = _build_worker(cfg)
    eid = _enqueue(sqlite, "s", 0, "I live in Chicago.", "OK")
    worker2, _sqlite2, _neo2 = _build_worker(cfg)

    assert worker1._claim_batch() == [eid]
    assert worker2._claim_batch() == []
    ev = sqlite.get_event(eid)
    assert ev is not None
    assert ev["status"] == "PROCESSING"


def test_worker_is_idempotent_on_replay(cfg: EngramConfig):
    """Restart the worker — events already COMPLETE are left alone."""
    worker, sqlite, _neo = _build_worker(cfg)
    eid = _enqueue(sqlite, "s", 0, "I live in Chicago.", "OK")
    worker.start()
    try:
        _wait_for_terminal(sqlite, [eid], timeout_s=5.0)
    finally:
        worker.stop(timeout_s=3.0)
    # Second worker instance on the same DB
    worker2, _sqlite2, _neo2 = _build_worker(cfg)
    worker2.start()
    time.sleep(0.5)
    try:
        ev = sqlite.get_event(eid)
        assert ev is not None
        # Still terminal — the worker noticed it was done and did not re-run.
        assert ev["status"] in {"COMPLETE", "GATED_SKIP"}
    finally:
        worker2.stop(timeout_s=3.0)


def test_failed_events_are_marked(cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch):
    """Simulate a pipeline crash — the worker marks the event FAILED."""
    worker, sqlite, _neo = _build_worker(cfg)
    eid = _enqueue(sqlite, "s", 0, "I just joined Meta", "ok")

    # Monkey-patch process_event to always explode.
    from engram.ingest import durable_worker as dw

    def _blow_up(*_args, **_kwargs):
        raise RuntimeError("forced failure")

    monkeypatch.setattr(dw, "process_event", _blow_up)
    worker.start()
    try:
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            ev = sqlite.get_event(eid)
            if ev and ev["status"] == "FAILED":
                break
            time.sleep(0.05)
    finally:
        worker.stop(timeout_s=3.0)
    ev = sqlite.get_event(eid)
    assert ev is not None
    assert ev["status"] == "FAILED"
    assert "forced failure" in (ev.get("error_message") or "")
