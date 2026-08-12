"""Durable ingest worker — polling behaviour, idempotency, shutdown."""

from __future__ import annotations

import multiprocessing
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from engram.config import EngramConfig
from engram.ingest.durable_worker import DurableIngestContext, DurableIngestWorker
from engram.ingest.worker import IngestContext
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.sqlite import SqliteStore
from engram.uri import pair_id as pair_id_fn

from .providers import DeterministicCoreProvider, DeterministicEmbeddingService


def _claim_from_process(
    database_path: str,
    start: Any,
    results: Any,
) -> None:
    """Claim from a separate interpreter process for SQLite ownership tests."""
    store = SqliteStore(database_path)
    start.wait(timeout=5.0)
    results.put([row["event_id"] for row in store.claim_pending_events(limit=1)])


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


def _enqueue(
    sqlite: SqliteStore,
    session_id: str,
    idx: int,
    user: str,
    asst: str,
    *,
    force_store: bool = False,
) -> str:
    pid = pair_id_fn(session_id, idx * 2, idx * 2 + 1)
    eid, _ = sqlite.record_event(
        pair_id=pid, session_id=session_id, source="test", event_type="INGEST",
        payload={
            "force_store": force_store,
            "turn_pair": {
                "user": {"content": user, "turn_idx": idx * 2},
                "assistant": {"content": asst, "turn_idx": idx * 2 + 1},
            }
        },
    )
    return eid


def test_force_store_bypasses_semantic_write_gate(
    cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    from engram.ingest import worker as ingest_worker

    worker, sqlite, _neo = _build_worker(cfg, concurrency=1)
    eid = _enqueue(
        sqlite,
        "corpus-import",
        0,
        "A factual turn that the caller requires Engram to preserve.",
        "Acknowledged.",
        force_store=True,
    )

    def _unexpected_gate(*_args, **_kwargs):
        raise AssertionError("force_store must not call the write gate")

    monkeypatch.setattr(ingest_worker, "_call_gate", _unexpected_gate)
    worker.start()
    try:
        _wait_for_terminal(sqlite, [eid])
    finally:
        worker.stop(timeout_s=3.0)

    assert sqlite.get_event(eid)["status"] == "COMPLETE"
    stage = sqlite.get_event_stage(eid, tenant_id="_default")
    assert stage["gate_output"]["reason"] == "authenticated force_store override"


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


def test_claim_is_atomic_across_independent_processes(cfg: EngramConfig):
    """Two server processes cannot acquire the same SQLite processing lease."""
    sqlite = SqliteStore(cfg.event_ledger.path)
    eid = _enqueue(sqlite, "multiprocess", 0, "I live in Chicago.", "OK")
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_claim_from_process,
            args=(cfg.event_ledger.path, start, results),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start.set()
    for process in processes:
        process.join(timeout=10.0)
        assert process.exitcode == 0

    claims = [results.get(timeout=2.0) for _ in processes]
    assert sorted(claims, key=len) == [[], [eid]]
    event = sqlite.get_event(eid)
    assert event is not None
    assert event["status"] == "PROCESSING"


def test_claim_batch_never_exceeds_free_worker_capacity(cfg: EngramConfig):
    worker, sqlite, _neo = _build_worker(cfg, concurrency=1)
    event_ids = [
        _enqueue(sqlite, "capacity", i, f"Fact {i}", "Noted.")
        for i in range(4)
    ]

    assert worker._claim_batch() == [event_ids[0]]
    assert worker._claim_batch() == []

    statuses = [sqlite.get_event(event_id)["status"] for event_id in event_ids]
    assert statuses == ["PROCESSING", "RECEIVED", "RECEIVED", "RECEIVED"]

    # Completing a processing lease releases one slot for the next claim.
    with worker._in_flight_lock:
        worker._in_flight.discard(event_ids[0])
    sqlite.set_event_status(event_ids[0], "COMPLETE")

    assert worker._claim_batch() == [event_ids[1]]
    statuses = [sqlite.get_event(event_id)["status"] for event_id in event_ids]
    assert statuses == ["COMPLETE", "PROCESSING", "RECEIVED", "RECEIVED"]


def test_concurrent_claim_calls_share_instance_capacity(cfg: EngramConfig):
    worker, sqlite, _neo = _build_worker(cfg, concurrency=2)
    event_ids = [
        _enqueue(sqlite, "concurrent-capacity", i, f"Fact {i}", "Noted.")
        for i in range(8)
    ]

    with ThreadPoolExecutor(max_workers=8) as executor:
        batches = list(executor.map(lambda _index: worker._claim_batch(), range(8)))

    claimed = [event_id for batch in batches for event_id in batch]
    assert claimed == event_ids[:2]
    assert len(set(claimed)) == 2
    statuses = [sqlite.get_event(event_id)["status"] for event_id in event_ids]
    assert statuses == ["PROCESSING", "PROCESSING", *(["RECEIVED"] * 6)]


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


def test_shutdown_timeout_releases_inflight_claim_without_retry(
    cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    from engram.ingest import durable_worker as durable_module

    worker, sqlite, _neo = _build_worker(cfg, concurrency=1)
    eid = _enqueue(sqlite, "shutdown-release", 0, "I live in Chicago.", "OK")
    entered = threading.Event()
    unblock = threading.Event()

    def _block(*_args, **_kwargs):
        entered.set()
        unblock.wait(timeout=5.0)
        return "COMPLETE"

    monkeypatch.setattr(durable_module, "process_event", _block)
    worker.start()
    assert entered.wait(timeout=2.0)

    worker.stop(timeout_s=0.05)

    event = sqlite.get_event(eid)
    assert event is not None
    assert event["status"] == "RECEIVED"
    assert event["retry_count"] == 0
    unblock.set()
    for thread in worker._workers:
        thread.join(timeout=1.0)


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


def test_transient_provider_failure_is_persistently_deferred(
    cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    from engram.ingest import durable_worker as dw
    from engram.models.core import TransientCoreModelError

    cfg.event_ledger.transient_retry_initial_delay_seconds = 60
    worker, sqlite, _neo = _build_worker(cfg, concurrency=1)
    eid = _enqueue(sqlite, "transient", 0, "I live in Pune.", "Noted.")
    assert worker._claim_batch() == [eid]
    monkeypatch.setattr(
        dw,
        "process_event",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            TransientCoreModelError("provider rate limited")
        ),
    )

    worker._process_one(eid)

    event = sqlite.get_event(eid)
    assert event is not None
    assert event["status"] == "RECEIVED"
    assert event["retry_count"] == 1
    assert event["next_attempt_at"] is not None
    assert "rate limited" in event["error_message"]
    with worker._in_flight_lock:
        worker._in_flight.discard(eid)
    assert worker._claim_batch() == []


def test_transient_provider_failure_becomes_terminal_after_retry_budget(
    cfg: EngramConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    from engram.ingest import durable_worker as dw
    from engram.models.core import TransientCoreModelError

    cfg.event_ledger.max_transient_retries = 0
    worker, sqlite, _neo = _build_worker(cfg, concurrency=1)
    eid = _enqueue(sqlite, "transient-exhausted", 0, "I live in Pune.", "Noted.")
    assert worker._claim_batch() == [eid]
    monkeypatch.setattr(
        dw,
        "process_event",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            TransientCoreModelError("provider rate limited")
        ),
    )

    worker._process_one(eid)

    event = sqlite.get_event(eid)
    assert event is not None
    assert event["status"] == "FAILED"
    assert event["retry_count"] == 0
    assert event["next_attempt_at"] is None
