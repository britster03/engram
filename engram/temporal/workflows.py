"""Deterministic Temporal workflows.

Only opaque control-plane identifiers are accepted as workflow input.  This
keeps conversation text and credentials out of Temporal event history.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
)

_MAINTENANCE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=5,
)

_PROJECTION_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
)

_FAILURE_BOOKKEEPING_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    # Keep a terminal workflow open until PostgreSQL can record its terminal
    # state. This prevents an outbox row being stranded in STARTED during a
    # control-plane outage.
    maximum_attempts=0,
)


def _terminal_retry() -> RetryPolicy:
    # Preserve replay of workflows that already scheduled the old activity.
    return (_FAILURE_BOOKKEEPING_RETRY if workflow.patched("durable-terminal-bookkeeping-v1")
            else RetryPolicy(maximum_attempts=3))


def ingestion_failure_summary(error: BaseException | None) -> str:
    """Retain a typed transient provider cause through SDK/Temporal wrappers.

    This is deterministic failure bookkeeping, not a retry decision. Permanent
    non-retryable failures remain terminal. Recovery still requires observed
    provider health, tenant scope, and the durable one-generation limit.
    """
    transient = {
        "APITimeoutError",
        "APIConnectionError",
        "RateLimitError",
        "InternalServerError",
        "CircuitOpenError",
    }
    current = error
    for _ in range(16):
        if current is None or getattr(current, "non_retryable", False):
            break
        name = getattr(current, "type", None) or type(current).__name__
        if name in transient:
            message = str(current)
            return (message if message.startswith(name + ":") else name + ": " + message)[:500]
        current = getattr(current, "cause", None) or getattr(current, "__cause__", None)
    return str(error)[:500]


@workflow.defn(name="engram.ingest")
class IngestWorkflow:
    @workflow.run
    async def run(self, event_id: str) -> str:
        try:
            return await workflow.execute_activity(
                "engram.process_ingest",
                event_id,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=_RETRY,
            )
        except ActivityError as err:
            # A terminal status belongs in the authoritative control plane,
            # never just in Temporal visibility history.
            await workflow.execute_activity(
                "engram.mark_event_failed",
                args=[
                    event_id,
                    ingestion_failure_summary(err.cause)
                    if workflow.patched("provider-ingestion-failure-cause-v1")
                    else str(err.cause)[:500],
                ],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=_terminal_retry(),
            )
            raise ApplicationError("ingest exhausted retries", type="IngestFailed") from err


@workflow.defn(name="engram.canonical_ingest")
class CanonicalIngestWorkflow:
    """Bounded V2 conversation pipeline for PostgreSQL-canonical memory.

    Only the event ID and tenant routing key cross the workflow boundary. Gate
    and extraction outputs are persisted as PostgreSQL artifacts by their
    Activities; the final Activity performs the canonical mutation and event
    completion in one outer transaction. ``IngestWorkflow`` remains the legacy
    compatibility workflow while the dispatcher selects this class for
    canonical mode.
    """

    @workflow.run
    async def run(self, event_id: str, tenant_id: str) -> str:
        try:
            gate = await workflow.execute_activity(
                "engram.canonical_gate",
                args=[event_id, tenant_id],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=_RETRY,
            )
            if isinstance(gate, dict) and bool(gate.get("store")):
                await workflow.execute_activity(
                    "engram.canonical_extract",
                    args=[event_id, tenant_id],
                    start_to_close_timeout=timedelta(minutes=10),
                    retry_policy=_RETRY,
                )
            return await workflow.execute_activity(
                "engram.commit_canonical",
                args=[event_id, tenant_id],
                start_to_close_timeout=timedelta(minutes=15),
                retry_policy=_RETRY,
            )
        except ActivityError as err:
            await workflow.execute_activity(
                "engram.mark_canonical_ingest_failed",
                args=[
                    event_id,
                    tenant_id,
                    ingestion_failure_summary(err.cause)
                    if workflow.patched("provider-ingestion-failure-cause-v1")
                    else str(err.cause)[:500],
                ],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=_terminal_retry(),
            )
            raise ApplicationError(
                "canonical ingest exhausted retries", type="CanonicalIngestFailed"
            ) from err


@workflow.defn(name="engram.code_ingest")
class CodeIngestWorkflow:
    """Durable scaffold for code-archive ingestion on its own task queue."""

    @workflow.run
    async def run(self, job_id: str) -> str:
        try:
            return await workflow.execute_activity(
                "engram.process_code_ingest",
                job_id,
                start_to_close_timeout=timedelta(minutes=30),
                retry_policy=_RETRY,
            )
        except ActivityError as err:
            await workflow.execute_activity(
                "engram.mark_code_ingest_failed",
                args=[job_id, str(err.cause)[:500]],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=_terminal_retry(),
            )
            raise ApplicationError(
                "code ingest exhausted retries", type="CodeIngestFailed"
            ) from err


@workflow.defn(name="engram.consolidation")
class ConsolidationWorkflow:
    @workflow.run
    async def run(self, task_id: str) -> str:
        try:
            return await workflow.execute_activity(
                "engram.process_consolidation",
                task_id,
                start_to_close_timeout=timedelta(minutes=15),
                retry_policy=_RETRY,
            )
        except ActivityError as err:
            await workflow.execute_activity(
                "engram.mark_task_failed",
                args=[task_id, str(err.cause)[:500]],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=_terminal_retry(),
            )
            raise ApplicationError(
                "consolidation exhausted retries", type="ConsolidationFailed"
            ) from err


@workflow.defn(name="engram.projection")
class ProjectionWorkflow:
    """Project one committed canonical mutation into Neo4j.

    The workflow receives only a ``workflow_dispatches.dispatch_id``.  The
    Activity loads the canonical mutation ID and stable projection identifiers
    from PostgreSQL, keeping mutable payloads and credentials out of Temporal
    history while preserving the transactional outbox handoff.
    """

    @workflow.run
    async def run(self, dispatch_id: str) -> str:
        try:
            return await workflow.execute_activity(
                "engram.project_neo4j",
                dispatch_id,
                start_to_close_timeout=timedelta(minutes=15),
                retry_policy=_PROJECTION_RETRY,
            )
        except ActivityError as err:
            await workflow.execute_activity(
                "engram.mark_projection_failed",
                args=[dispatch_id, str(err.cause)[:500]],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=_FAILURE_BOOKKEEPING_RETRY,
            )
            raise ApplicationError("projection exhausted retries", type="ProjectionFailed") from err


@workflow.defn(name="engram.reconciliation")
class ReconciliationWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        return await workflow.execute_activity(
            "engram.run_reconciliation",
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_MAINTENANCE_RETRY,
        )


@workflow.defn(name="engram.stale_overview_scan")
class StaleOverviewWorkflow:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            "engram.scan_stale_overviews",
            start_to_close_timeout=timedelta(minutes=30),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=_MAINTENANCE_RETRY,
        )


@workflow.defn(name="engram.decay")
class DecayWorkflow:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            "engram.run_decay",
            start_to_close_timeout=timedelta(hours=6),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=_MAINTENANCE_RETRY,
        )
