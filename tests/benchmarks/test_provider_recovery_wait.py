"""Readiness waits for native recovery but never treats failed ingestion as ready."""

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from benchmarks.lifecycle.adapter import readiness
from tests.postgres_support import PostgresTestStore


@pytest.fixture
def failed_event():
    if not os.environ.get("ENGRAM_TEST_DATABASE_URL"):
        pytest.skip("isolated PostgreSQL required")
    store = PostgresTestStore()
    from engram.storage.memory_repository import MemoryRepository

    MemoryRepository(store).ensure_schema()
    event, _ = store.record_event(
        pair_id="one",
        session_id="session",
        source="test",
        event_type="INGEST",
        payload={},
        tenant_id="tenant",
    )
    store.set_event_status(
        event, "FAILED", error_message="APITimeoutError: timeout", tenant_id="tenant"
    )
    store.mark_aggregate_dispatches_dead("INGEST", event, "failed")
    neo = MagicMock()
    neo.reader.return_value.session.return_value.__enter__.return_value.run.return_value = []
    cfg = SimpleNamespace(
        event_ledger=SimpleNamespace(dsn=store.dsn),
        retrieval=SimpleNamespace(quality_mode="enforce"),
        canonical_memory=SimpleNamespace(evidence_mode="enforce", recover_transient_failures=True),
    )
    yield store, event, SimpleNamespace(cfg=cfg, neo4j=neo)
    store.close()


def test_waiting_is_not_ready_and_only_native_health_recovery_can_advance(failed_event):
    store, event, state = failed_event
    result = readiness(state, "tenant")
    assert not result["ready"] and not result["fatal"]
    assert result["events"] == {"FAILED": 1}
    assert result["pending_provider_recovery"] == [event]
    assert result["dispatches"] == {"RECOVERY_WAITING": 1}
    assert result["historical_dispatches"] == {"DEAD": 1}
    with store.transaction() as conn:
        conn.execute(
            "UPDATE events SET processed_at=CURRENT_TIMESTAMP-INTERVAL '60 seconds' WHERE event_id=?",
            (event,),
        )
    assert (
        store.retry_recovered_provider_events(
            tenant_id="tenant", healthy_at=datetime.now(timezone.utc)
        )
        == 1
    )
    result = readiness(state, "tenant")
    assert not result["ready"] and not result["fatal"]
    assert result["pending_provider_recovery"] == []
    store.set_event_status(event, "GATED_SKIP", tenant_id="tenant")
    with store.transaction() as conn:
        conn.execute(
            "UPDATE workflow_dispatches SET status='COMPLETE' WHERE tenant_id=? AND generation=2",
            ("tenant",),
        )
    result = readiness(state, "tenant")
    assert result["ready"] and not result["fatal"]
    assert result["historical_dispatches"]["DEAD"] == 1


def test_disabled_recovery_and_permanent_errors_remain_fatal(failed_event):
    store, event, state = failed_event
    state.cfg.canonical_memory.recover_transient_failures = False
    assert readiness(state, "tenant")["fatal"]
    state.cfg.canonical_memory.recover_transient_failures = True
    store.set_event_status(
        event, "FAILED", error_message="UngroundedExtraction: unknown source", tenant_id="tenant"
    )
    result = readiness(state, "tenant")
    assert result["fatal"] and not result["ready"]
    assert result["pending_provider_recovery"] == []


def test_recovery_exhaustion_and_projection_failure_cannot_be_hidden(failed_event):
    store, event, state = failed_event
    with store.transaction() as conn:
        store._enqueue_dispatch_in_tx(conn, "PROJECTION", "bad-projection", "tenant")
        conn.execute(
            "UPDATE workflow_dispatches SET status='DEAD' WHERE workflow_type='PROJECTION'"
        )
    assert readiness(state, "tenant")["fatal"]
    with store.transaction() as conn:
        conn.execute(
            "UPDATE workflow_dispatches SET status='COMPLETE' WHERE workflow_type='PROJECTION'"
        )
        conn.execute(
            "UPDATE workflow_dispatches SET payload=?::jsonb WHERE aggregate_id=? AND workflow_type='INGEST'",
            ('{"recovery_policy":"provider-recovery-v1"}', event),
        )
    result = readiness(state, "tenant")
    assert result["fatal"] and not result["ready"]
    assert result["pending_provider_recovery"] == []
    assert readiness(state, "foreign")["pending_provider_recovery"] == []


def test_waiting_transient_failure_cannot_hide_another_permanent_failure(failed_event):
    store, first, state = failed_event
    second, _ = store.record_event(
        pair_id="two",
        session_id="session",
        source="test",
        event_type="INGEST",
        payload={},
        tenant_id="tenant",
    )
    store.set_event_status(
        second, "FAILED", error_message="UngroundedExtraction: invalid source", tenant_id="tenant"
    )
    store.mark_aggregate_dispatches_dead("INGEST", second, "failed")
    result = readiness(state, "tenant")
    assert result["events"] == {"FAILED": 2}
    assert result["pending_provider_recovery"] == [first]
    assert result["fatal"] and not result["ready"]
