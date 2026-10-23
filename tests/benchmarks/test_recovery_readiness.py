import os

import pytest

from benchmarks.lifecycle.adapter import readiness_dispatches
from tests.postgres_support import PostgresTestStore


@pytest.mark.skipif(not os.environ.get("ENGRAM_TEST_DATABASE_URL"), reason="isolated PostgreSQL required")
def test_recovered_ingest_can_be_ready_without_hiding_projection_failures():
    store = PostgresTestStore()
    try:
        event, _ = store.record_event(pair_id="one", session_id="session", source="test",
                                     event_type="INGEST", payload={}, tenant_id="tenant")
        store.set_event_status(event, "FAILED", error_message="APITimeoutError: test", tenant_id="tenant")
        store.mark_aggregate_dispatches_dead("INGEST", event, "failed")
        store.retry_event(event, tenant_id="tenant")
        with store.transaction() as conn:
            counts = {r["status"]: r["n"] for r in readiness_dispatches(conn.raw, "tenant")}
            assert counts == {"PENDING": 1}
            conn.execute("UPDATE workflow_dispatches SET status='COMPLETE' WHERE tenant_id=? AND generation=2", ("tenant",))
            assert {r["status"]: r["n"] for r in readiness_dispatches(conn.raw, "tenant")} == {"COMPLETE": 1}
            store._enqueue_dispatch_in_tx(conn, "PROJECTION", "projection", "tenant")
            conn.execute("UPDATE workflow_dispatches SET status='DEAD' WHERE workflow_type='PROJECTION'")
            store._enqueue_dispatch_in_tx(conn, "PROJECTION", "foreign", "foreign")
            counts = {r["status"]: r["n"] for r in readiness_dispatches(conn.raw, "tenant")}
            assert counts == {"COMPLETE": 1, "DEAD": 1}
            # The failed original workflow remains available for diagnostics.
            assert conn.execute("SELECT count(*) AS n FROM workflow_dispatches WHERE tenant_id=? AND status='DEAD'", ("tenant",)).fetchone()["n"] == 2
    finally:
        store.close()
