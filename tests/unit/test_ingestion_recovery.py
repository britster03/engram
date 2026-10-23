"""Regression checks for discarded documents, retry routing and status recovery."""

import asyncio
import io
import zipfile

import pytest

from engram.api.routes.bulk_ingest import _parse_zip
from engram.storage.memory_repository import MemoryRepository
from engram.temporal.dispatcher import reconcile_started_dispatches
from tests.postgres_support import PostgresTestStore


@pytest.fixture
def store():
    instance = PostgresTestStore()
    yield instance
    instance.close()


@pytest.mark.parametrize("length,accepted", [(32000, True), (32001, False), (33021, False)])
def test_zip_document_is_complete_or_explicitly_rejected(length, accepted):
    body = io.BytesIO()
    text = "x" * (length - 10) + "FINAL FACT"
    with zipfile.ZipFile(body, "w") as archive:
        archive.writestr("facts.md", text)
    rows, rejected, _ = _parse_zip(
        body.getvalue(), job_id="upload", default_session_id=None, source="test"
    )
    if accepted:
        assert not rejected
        assert rows[0].request.turn_pair.assistant.content == text
    else:
        assert not rows
        assert "facts.md" in rejected[0].reason
        assert str(length) in rejected[0].reason
        assert "32000" in rejected[0].reason


def event_dispatch(store, kind="INGEST", *, status="STARTED"):
    event, _ = store.record_event(
        pair_id="pair",
        session_id=None,
        source="test",
        event_type=kind,
        payload={"job_id": "job", "artifact_key": "source.zip"},
        tenant_id="tenant",
    )
    aggregate = event if kind == "INGEST" else "job"
    if kind == "CODE_INGEST":
        store.enqueue_code_ingest_dispatch(
            job_id=aggregate,
            tenant_id="tenant",
            payload={"event_id": event, "artifact_key": "source.zip"},
        )
    row = store.get_dispatch_for_aggregate(kind, aggregate)
    with store.transaction() as conn:
        conn.execute(
            "UPDATE workflow_dispatches SET status = ?, workflow_run_id = 'run', updated_at = CURRENT_TIMESTAMP - INTERVAL '1 hour' WHERE dispatch_id = ?",
            (status, row["dispatch_id"]),
        )
    return event, store.get_dispatch(row["dispatch_id"])


def test_code_retry_uses_original_job_queue_and_envelope(store):
    event, dispatch = event_dispatch(store, "CODE_INGEST", status="DEAD")
    store.set_event_status(event, "FAILED", tenant_id="tenant")
    result = store.retry_event(event, tenant_id="tenant")
    assert result["status"] == "RECEIVED"
    retried = store.get_dispatch_for_aggregate("CODE_INGEST", "job")
    assert retried["generation"] == 2
    assert retried["task_queue"] == dispatch["task_queue"]
    assert retried["payload"] == dispatch["payload"]
    assert store.get_dispatch_for_aggregate("INGEST", event) is None
    assert store.retry_event(event, tenant_id="other") is None


@pytest.mark.parametrize("terminal", ["FAILED", "CANCELED", "TERMINATED", "TIMED_OUT", "COMPLETED"])
@pytest.mark.parametrize("kind", ["INGEST", "CODE_INGEST"])
def test_exact_terminal_execution_recovers_stuck_event(store, terminal, kind):
    event, row = event_dispatch(store, kind)
    assert store.reconcile_workflow_execution(
        row["dispatch_id"], run_id="run", workflow_status=terminal
    )
    assert store.get_event(event)["status"] == "FAILED"
    assert store.get_dispatch(row["dispatch_id"])["status"] == "DEAD"
    assert store.retry_event(event, tenant_id="tenant")["status"] == "RECEIVED"


def test_status_repair_is_fenced_and_preserves_committed_work(store):
    event, row = event_dispatch(store)
    assert not store.reconcile_workflow_execution(
        row["dispatch_id"], run_id="other-run", workflow_status="FAILED"
    )
    assert not store.reconcile_workflow_execution(
        row["dispatch_id"], run_id="run", workflow_status="RUNNING"
    )
    assert store.get_event(event)["status"] == "RECEIVED"
    store.set_event_status(event, "COMPLETE", tenant_id="tenant")
    assert store.reconcile_workflow_execution(
        row["dispatch_id"], run_id="run", workflow_status="FAILED"
    )
    assert store.get_event(event)["status"] == "COMPLETE"
    assert store.get_dispatch(row["dispatch_id"])["status"] == "COMPLETE"


def test_late_failure_cannot_overwrite_newer_retry(store):
    event, old = event_dispatch(store, status="DEAD")
    store.set_event_status(event, "FAILED", tenant_id="tenant")
    store.retry_event(event, tenant_id="tenant")
    assert not store.fail_ingest_execution(
        "INGEST", event, "late failure", workflow_id=old["workflow_id"], run_id="run"
    )
    assert store.get_event(event)["status"] == "RECEIVED"


def test_code_failure_records_event_and_dispatch_atomically(store):
    event, row = event_dispatch(store, "CODE_INGEST")
    assert store.fail_ingest_execution(
        "CODE_INGEST", "job", "provider outage", workflow_id=row["workflow_id"], run_id="run"
    )
    assert store.get_event(event)["status"] == "FAILED"
    assert store.get_dispatch(row["dispatch_id"])["status"] == "DEAD"


def test_visibility_outage_does_not_requeue_started_work(store):
    event, row = event_dispatch(store)

    class Client:
        def get_workflow_handle(self, workflow_id, *, run_id):
            assert workflow_id == row["workflow_id"] and run_id == "run"
            return self

        async def describe(self):
            raise ConnectionError("Temporal unavailable")

    assert asyncio.run(reconcile_started_dispatches(Client(), store)) == 0
    assert store.get_event(event)["status"] == "RECEIVED"
    assert store.get_dispatch(row["dispatch_id"])["status"] == "STARTED"


def test_verifier_artifact_publication_is_safe_under_concurrency(store):
    from concurrent.futures import ThreadPoolExecutor

    repository = MemoryRepository(store)
    repository.ensure_schema()
    event, _ = event_dispatch(store)

    def publish(index):
        return repository.record_ingest_artifact(
            event_id=event,
            artifact_type="EXTRACTION",
            artifact_key="review",
            payload={"winner": index},
            tenant_id="tenant",
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(publish, range(4)))
    assert len({r.id for r in results}) == 1
    assert len({r.payload["winner"] for r in results}) == 1
