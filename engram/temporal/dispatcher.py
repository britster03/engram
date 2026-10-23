"""Transactional-outbox dispatcher for starting Temporal workflows."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta
from typing import Any

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy

from engram.config import EngramConfig, get_config
from engram.storage.postgres import PostgresStore
from engram.temporal.workflows import (
    CanonicalIngestWorkflow,
    CodeIngestWorkflow,
    ConsolidationWorkflow,
    DecayWorkflow,
    IngestWorkflow,
    ProjectionWorkflow,
    ReconciliationWorkflow,
    StaleOverviewWorkflow,
)

log = logging.getLogger(__name__)


def build_maintenance_schedules(cfg: EngramConfig) -> dict[str, Schedule]:
    """Build the desired server-side schedules from application config."""
    queue = cfg.temporal.maintenance_task_queue
    no_overlap = SchedulePolicy(
        overlap=ScheduleOverlapPolicy.SKIP,
        catchup_window=timedelta(minutes=10),
    )
    schedules: dict[str, Schedule] = {}
    if cfg.event_ledger.reconciliation_interval_seconds > 0:
        schedules["engram-reconciliation"] = Schedule(
            action=ScheduleActionStartWorkflow(
                ReconciliationWorkflow.run,
                id="engram:maintenance:reconciliation",
                task_queue=queue,
                execution_timeout=timedelta(minutes=15),
            ),
            spec=ScheduleSpec(
                intervals=[
                    ScheduleIntervalSpec(
                        every=timedelta(seconds=cfg.event_ledger.reconciliation_interval_seconds)
                    )
                ]
            ),
            policy=no_overlap,
        )
    schedules["engram-stale-overview-scan"] = Schedule(
        action=ScheduleActionStartWorkflow(
            StaleOverviewWorkflow.run,
            id="engram:maintenance:stale-overview-scan",
            task_queue=queue,
            execution_timeout=timedelta(minutes=45),
        ),
        spec=ScheduleSpec(
            intervals=[
                ScheduleIntervalSpec(
                    every=timedelta(seconds=cfg.event_ledger.stale_overview_scan_interval_seconds)
                )
            ]
        ),
        policy=no_overlap,
    )
    schedules["engram-decay"] = Schedule(
        action=ScheduleActionStartWorkflow(
            DecayWorkflow.run,
            id="engram:maintenance:decay",
            task_queue=queue,
            execution_timeout=timedelta(hours=7),
        ),
        spec=ScheduleSpec(
            cron_expressions=[cfg.decay.schedule],
            time_zone_name=cfg.decay.schedule_timezone,
        ),
        policy=SchedulePolicy(
            overlap=ScheduleOverlapPolicy.SKIP,
            catchup_window=timedelta(hours=24),
        ),
    )
    return schedules


async def ensure_maintenance_schedules(client: Client, cfg: EngramConfig) -> None:
    """Create schedules once and update their definitions after config changes."""
    for schedule_id, schedule in build_maintenance_schedules(cfg).items():
        try:
            await client.create_schedule(schedule_id, schedule)
            log.info("created Temporal schedule", extra={"schedule_id": schedule_id})
        except ScheduleAlreadyRunningError:
            handle = client.get_schedule_handle(schedule_id)

            def apply_schedule(
                _input: ScheduleUpdateInput,
                desired: Schedule = schedule,
            ) -> ScheduleUpdate:
                return ScheduleUpdate(schedule=desired)

            await handle.update(apply_schedule)
            log.info("updated Temporal schedule", extra={"schedule_id": schedule_id})


_WORKFLOWS: dict[str, Any] = {
    "INGEST": IngestWorkflow,
    "CODE_INGEST": CodeIngestWorkflow,
    "PROJECTION": ProjectionWorkflow,
    "CONSOLIDATION": ConsolidationWorkflow,
}


async def reconcile_started_dispatches(client: Client, store: PostgresStore) -> int:
    """Check exact run IDs; visibility outages never trigger duplicate workflows."""
    semaphore = asyncio.Semaphore(8)

    async def inspect(dispatch: dict[str, Any]) -> bool:
        async with semaphore:
            run_id = dispatch.get("workflow_run_id")
            if not run_id:
                store.touch_dispatch_inspected(dispatch["dispatch_id"], run_id)
                log.warning("started dispatch has no Temporal run ID", extra={"dispatch_id": dispatch["dispatch_id"]})
                return False
            try:
                description = await asyncio.wait_for(
                    client.get_workflow_handle(dispatch["workflow_id"], run_id=run_id).describe(),
                    timeout=5,
                )
                return store.reconcile_workflow_execution(
                    dispatch["dispatch_id"], run_id=run_id,
                    workflow_status=description.status.name,
                )
            except Exception:
                log.warning("unable to verify Temporal execution", extra={"dispatch_id": dispatch["dispatch_id"]}, exc_info=True)
                store.touch_dispatch_inspected(dispatch["dispatch_id"], run_id)
                return False

    return sum(await asyncio.gather(*(inspect(row) for row in store.started_dispatches())))


async def run() -> None:
    cfg = get_config()
    if not cfg.temporal.enabled:
        raise RuntimeError("Temporal dispatcher requires temporal.enabled")
    dsn = os.environ.get("ENGRAM_DATABASE_URL") or cfg.event_ledger.dsn
    if not dsn:
        raise RuntimeError("Temporal dispatcher requires ENGRAM_DATABASE_URL or event_ledger.dsn")
    store = PostgresStore(
        dsn,
        initialize_schema=False,
        ingest_task_queue=cfg.temporal.ingest_task_queue,
        code_ingest_task_queue=cfg.temporal.code_ingest_task_queue,
        projection_task_queue=cfg.temporal.projection_task_queue,
        consolidation_task_queue=cfg.temporal.consolidation_task_queue,
    )
    try:
        client = await Client.connect(
            cfg.temporal.address,
            namespace=cfg.temporal.namespace,
        )
        await ensure_maintenance_schedules(client, cfg)
        next_reconciliation = 0.0
        while True:
            if asyncio.get_running_loop().time() >= next_reconciliation:
                try:
                    repaired = await reconcile_started_dispatches(client, store)
                    if repaired:
                        log.info("reconciled terminal Temporal executions", extra={"count": repaired})
                except Exception:
                    log.exception("Temporal status reconciliation failed")
                next_reconciliation = asyncio.get_running_loop().time() + 30
            try:
                dispatches = store.claim_dispatches()
            except Exception:
                log.exception("failed to claim Temporal dispatches")
                await asyncio.sleep(cfg.temporal.dispatch_interval_seconds)
                continue
            for dispatch in dispatches:
                workflow = _WORKFLOWS.get(str(dispatch["workflow_type"]))
                if workflow is IngestWorkflow and cfg.canonical_memory.enabled:
                    # Keep the durable outbox workflow type as INGEST for
                    # compatibility, while canonical deployments route the
                    # event to the bounded PostgreSQL-only workflow.
                    workflow = CanonicalIngestWorkflow
                if workflow is None:
                    error = f"unsupported workflow type: {dispatch['workflow_type']}"
                    log.error(error, extra={"dispatch_id": dispatch["dispatch_id"]})
                    store.mark_dispatch_failed(
                        dispatch["dispatch_id"],
                        error,
                        claim_token=str(dispatch["claim_token"]),
                    )
                    continue
                try:
                    workflow_args = (
                        [dispatch["aggregate_id"], dispatch["tenant_id"]]
                        if workflow is CanonicalIngestWorkflow
                        else [
                            dispatch["dispatch_id"]
                            if str(dispatch["workflow_type"]) == "PROJECTION"
                            else dispatch["aggregate_id"]
                        ]
                    )
                    handle: Any = await client.start_workflow(
                        workflow,
                        args=workflow_args,
                        id=dispatch["workflow_id"],
                        task_queue=dispatch["task_queue"],
                        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                    )
                    store.mark_dispatch_started(
                        dispatch["dispatch_id"],
                        handle.first_execution_run_id,
                        claim_token=str(dispatch["claim_token"]),
                    )
                except Exception as err:
                    log.exception(
                        "temporal dispatch failed",
                        extra={"dispatch_id": dispatch["dispatch_id"]},
                    )
                    store.release_dispatch(
                        dispatch["dispatch_id"],
                        str(err),
                        claim_token=str(dispatch["claim_token"]),
                    )
            await asyncio.sleep(cfg.temporal.dispatch_interval_seconds)
    finally:
        store.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
