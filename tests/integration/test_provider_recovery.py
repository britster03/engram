"""One atomic, tenant-scoped recovery after observed provider health returns."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from tests.postgres_support import PostgresTestStore


def fail(store, label, error, tenant="recovery-first"):
    event, _ = store.record_event(
        pair_id=label,
        session_id="recovery-session",
        source="test",
        event_type="INGEST",
        payload={"turn_pair": {"user": {"content": label}}},
        tenant_id=tenant,
    )
    store.set_event_status(event, "FAILED", error_message=error, tenant_id=tenant)
    store.mark_aggregate_dispatches_dead("INGEST", event, error)
    with store.transaction() as conn:
        conn.execute(
            "UPDATE events SET processed_at=CURRENT_TIMESTAMP - INTERVAL '60 seconds' WHERE event_id=?",
            (event,),
        )
    return event


def test_recovery_excludes_permanent_foreign_and_unhealthy_events_and_runs_once():
    store = PostgresTestStore()
    try:
        event = fail(store, "timeout", "APITimeoutError: request timed out")
        permanent = fail(store, "invalid", "UngroundedExtraction: API returned 503 in source text")
        foreign = fail(store, "other-tenant", "RateLimitError: capacity", "recovery-second")
        now = datetime.now(timezone.utc)
        assert (
            store.retry_recovered_provider_events(
                tenant_id="recovery-first", healthy_at=now - timedelta(minutes=10)
            )
            == 0
        )
        assert (
            store.retry_recovered_provider_events(
                tenant_id="recovery-first", healthy_at=now - timedelta(seconds=90)
            )
            == 0
        )
        assert (
            store.retry_recovered_provider_events(tenant_id="recovery-first", healthy_at=now) == 1
        )
        assert store.get_event(event, tenant_id="recovery-first")["retry_count"] == 1
        assert store.get_event(permanent, tenant_id="recovery-first")["status"] == "FAILED"
        assert store.get_event(foreign, tenant_id="recovery-second")["status"] == "FAILED"
        # Even another terminal failure cannot create a second auto generation.
        store.set_event_status(
            event, "FAILED", error_message="APITimeoutError: again", tenant_id="recovery-first"
        )
        store.mark_aggregate_dispatches_dead("INGEST", event, "failed again")
        with store.transaction() as conn:
            conn.execute(
                "UPDATE events SET processed_at=CURRENT_TIMESTAMP-INTERVAL '60 seconds' WHERE event_id=?",
                (event,),
            )
        assert (
            store.retry_recovered_provider_events(
                tenant_id="recovery-first", healthy_at=datetime.now(timezone.utc)
            )
            == 0
        )
        rows = (
            store.get_conn()
            .execute(
                "SELECT details FROM audit_log WHERE tenant_id=? AND target=?",
                ("recovery-first", event),
            )
            .fetchall()
        )
        assert len(rows) == 1 and "provider-recovery-v1" in rows[0]["details"]
    finally:
        store.close()


def test_concurrent_maintenance_does_not_duplicate_recovery():
    store = PostgresTestStore()
    try:
        event = fail(store, "concurrent", "CircuitOpenError: cooling down")
        healthy_at = datetime.now(timezone.utc)
        with ThreadPoolExecutor(max_workers=2) as executor:
            counts = list(
                executor.map(
                    lambda _: store.retry_recovered_provider_events(
                        tenant_id="recovery-first", healthy_at=healthy_at
                    ),
                    range(2),
                )
            )
        assert sum(counts) == 1
        rows = (
            store.get_conn()
            .execute(
                "SELECT * FROM workflow_dispatches WHERE aggregate_id=? ORDER BY generation",
                (event,),
            )
            .fetchall()
        )
        assert [row["generation"] for row in rows] == [1, 2]
        assert rows[0]["status"] == "DEAD" and rows[1]["status"] == "PENDING"
    finally:
        store.close()
