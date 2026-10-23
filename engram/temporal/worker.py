"""Dedicated Temporal worker process; never run this inside the API process."""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, ExitStack

from temporalio.client import Client
from temporalio.worker import Worker

from engram.config import get_config
from engram.temporal.activities import (
    canonical_extract_activity,
    canonical_gate_activity,
    commit_canonical_activity,
    mark_canonical_ingest_failed_activity,
    mark_code_ingest_failed_activity,
    mark_event_failed_activity,
    mark_projection_failed_activity,
    mark_task_failed_activity,
    process_code_ingest_activity,
    process_consolidation_activity,
    process_ingest_activity,
    project_neo4j_activity,
    run_decay_activity,
    run_reconciliation_activity,
    scan_stale_overviews_activity,
)
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

_QUEUE_CHOICES = (
    "all",
    "ingest",
    "code-ingest",
    "consolidation",
    "projection",
    "maintenance",
)


def parse_queue(argv: list[str] | None = None) -> str:
    """Parse the Temporal queue selection.

    Production uses ``all`` so projection cannot be omitted accidentally.
    Individual selections remain useful for focused tests and diagnostics.
    """
    parser = argparse.ArgumentParser(description="Run Engram Temporal workers")
    parser.add_argument(
        "--queue",
        choices=_QUEUE_CHOICES,
        default="all",
        help="queue worker to run (default: all; useful for local development)",
    )
    return str(parser.parse_args(argv).queue)


async def run(queue: str = "all") -> None:
    if queue not in _QUEUE_CHOICES:
        raise ValueError(f"unsupported Temporal queue: {queue}")
    cfg = get_config()
    if not cfg.temporal.enabled:
        raise RuntimeError("temporal.enabled must be true to run a Temporal worker")
    client = await Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace)
    worker_concurrency = cfg.temporal.worker_concurrency
    maintenance_concurrency = cfg.temporal.maintenance_worker_concurrency
    selected = set(_QUEUE_CHOICES[1:]) if queue == "all" else {queue}
    with ExitStack() as executors:
        async with AsyncExitStack() as workers:
            if "ingest" in selected:
                ingest_executor = executors.enter_context(
                    ThreadPoolExecutor(max_workers=worker_concurrency)
                )
                await workers.enter_async_context(
                    Worker(
                        client,
                        task_queue=cfg.temporal.ingest_task_queue,
                        workflows=[IngestWorkflow, CanonicalIngestWorkflow],
                        activities=[
                            process_ingest_activity,
                            mark_event_failed_activity,
                            canonical_gate_activity,
                            canonical_extract_activity,
                            commit_canonical_activity,
                            mark_canonical_ingest_failed_activity,
                        ],
                        activity_executor=ingest_executor,
                        max_concurrent_activities=worker_concurrency,
                    )
                )
            if "consolidation" in selected:
                consolidation_executor = executors.enter_context(
                    ThreadPoolExecutor(max_workers=worker_concurrency)
                )
                await workers.enter_async_context(
                    Worker(
                        client,
                        task_queue=cfg.temporal.consolidation_task_queue,
                        workflows=[ConsolidationWorkflow],
                        activities=[process_consolidation_activity, mark_task_failed_activity],
                        activity_executor=consolidation_executor,
                        max_concurrent_activities=worker_concurrency,
                    )
                )
            if "code-ingest" in selected:
                code_ingest_executor = executors.enter_context(
                    ThreadPoolExecutor(max_workers=worker_concurrency)
                )
                await workers.enter_async_context(
                    Worker(
                        client,
                        task_queue=cfg.temporal.code_ingest_task_queue,
                        workflows=[CodeIngestWorkflow],
                        activities=[
                            process_code_ingest_activity,
                            mark_code_ingest_failed_activity,
                        ],
                        activity_executor=code_ingest_executor,
                        max_concurrent_activities=worker_concurrency,
                    )
                )
            if "projection" in selected:
                projection_executor = executors.enter_context(
                    ThreadPoolExecutor(max_workers=worker_concurrency)
                )
                await workers.enter_async_context(
                    Worker(
                        client,
                        task_queue=cfg.temporal.projection_task_queue,
                        workflows=[ProjectionWorkflow],
                        activities=[project_neo4j_activity, mark_projection_failed_activity],
                        activity_executor=projection_executor,
                        max_concurrent_activities=worker_concurrency,
                    )
                )
            if "maintenance" in selected:
                maintenance_executor = executors.enter_context(
                    ThreadPoolExecutor(max_workers=maintenance_concurrency)
                )
                await workers.enter_async_context(
                    Worker(
                        client,
                        task_queue=cfg.temporal.maintenance_task_queue,
                        workflows=[
                            ReconciliationWorkflow,
                            StaleOverviewWorkflow,
                            DecayWorkflow,
                        ],
                        activities=[
                            run_reconciliation_activity,
                            scan_stale_overviews_activity,
                            run_decay_activity,
                        ],
                        activity_executor=maintenance_executor,
                        max_concurrent_activities=maintenance_concurrency,
                    )
                )
            await asyncio.Future()


def main() -> None:
    asyncio.run(run(parse_queue()))


if __name__ == "__main__":
    main()
