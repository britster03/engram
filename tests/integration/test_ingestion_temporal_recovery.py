"""Opt-in real Temporal execution, worker restart and failure reconciliation."""

import asyncio
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import psycopg
import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from engram.temporal import activities
from engram.temporal.dispatcher import reconcile_started_dispatches
from engram.temporal.workflows import CanonicalIngestWorkflow
from tests.integration.test_claim_verification import TENANT, seed
from tests.integration.test_claim_verification import state as verification_state

pytestmark = pytest.mark.skipif(
    not os.environ.get("ENGRAM_TEST_TEMPORAL_ADDRESS"), reason="requires isolated Temporal"
)


@pytest.fixture
def state(monkeypatch):
    yield from verification_state.__wrapped__(monkeypatch)


def test_bookkeeping_connection_failures_survive_four_attempts_and_worker_restart(
    state, monkeypatch
):
    event, _, _ = seed(state)
    first_failure = threading.Event()
    attempts = []
    original = state.control_plane.fail_ingest_execution

    def unavailable(*args, **kwargs):
        attempt = activity.info().attempt
        attempts.append(attempt)
        if attempt <= 4:
            first_failure.set()
            raise psycopg.OperationalError("injected PostgreSQL connection outage")
        return original(*args, **kwargs)

    monkeypatch.setattr(state.control_plane, "fail_ingest_execution", unavailable)

    @activity.defn(name="engram.canonical_gate")
    def rejected_gate(event_id: str, tenant_id: str) -> dict:
        raise ApplicationError("invalid source", non_retryable=True)

    async def exercise():
        client = await Client.connect(
            os.environ["ENGRAM_TEST_TEMPORAL_ADDRESS"],
            namespace=os.environ.get("ENGRAM_TEST_TEMPORAL_NAMESPACE", "default"),
        )
        queue = "bookkeeping-" + uuid.uuid4().hex
        claimed = next(
            d for d in state.control_plane.claim_dispatches() if d["aggregate_id"] == event
        )
        with ThreadPoolExecutor(max_workers=2) as executor:

            def worker():
                return Worker(
                    client,
                    task_queue=queue,
                    workflows=[CanonicalIngestWorkflow],
                    activities=[rejected_gate, activities.mark_canonical_ingest_failed_activity],
                    activity_executor=executor,
                    graceful_shutdown_timeout=timedelta(seconds=2),
                )

            async with worker():
                handle = await client.start_workflow(
                    CanonicalIngestWorkflow.run,
                    args=[event, TENANT],
                    id=claimed["workflow_id"],
                    task_queue=queue,
                )
                state.control_plane.mark_dispatch_started(
                    claimed["dispatch_id"],
                    handle.first_execution_run_id,
                    claim_token=claimed["claim_token"],
                )
                assert await asyncio.to_thread(first_failure.wait, 15)
            assert (await handle.describe()).status.name == "RUNNING"
            assert state.control_plane.get_dispatch(claimed["dispatch_id"])["status"] == "STARTED"
            async with worker():
                with pytest.raises(WorkflowFailureError):
                    await asyncio.wait_for(handle.result(), timeout=75)
            assert (await handle.describe()).status.name == "FAILED"
            assert state.control_plane.get_event(event)["status"] == "FAILED"
            assert state.control_plane.get_dispatch(claimed["dispatch_id"])["status"] == "DEAD"

    asyncio.run(exercise())
    assert attempts == [1, 2, 3, 4, 5]


def test_terminated_workflow_is_reconciled_without_any_bookkeeping_activity(state):
    event, _, _ = seed(state)

    async def exercise():
        client = await Client.connect(
            os.environ["ENGRAM_TEST_TEMPORAL_ADDRESS"],
            namespace=os.environ.get("ENGRAM_TEST_TEMPORAL_NAMESPACE", "default"),
        )
        claimed = next(
            d for d in state.control_plane.claim_dispatches() if d["aggregate_id"] == event
        )
        handle = await client.start_workflow(
            CanonicalIngestWorkflow.run,
            args=[event, TENANT],
            id=claimed["workflow_id"],
            task_queue="unpolled-" + uuid.uuid4().hex,
        )
        state.control_plane.mark_dispatch_started(
            claimed["dispatch_id"],
            handle.first_execution_run_id,
            claim_token=claimed["claim_token"],
        )
        await handle.terminate(reason="Injected termination before worker activity")
        with state.control_plane.transaction() as conn:
            conn.execute(
                "UPDATE workflow_dispatches SET updated_at = CURRENT_TIMESTAMP - INTERVAL '1 hour' WHERE dispatch_id = ?",
                (claimed["dispatch_id"],),
            )
        assert await reconcile_started_dispatches(client, state.control_plane) == 1
        assert state.control_plane.get_event(event)["status"] == "FAILED"
        assert state.control_plane.get_dispatch(claimed["dispatch_id"])["status"] == "DEAD"

    asyncio.run(exercise())
