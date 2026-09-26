"""Reconciliation worker (§5.5, §5.6).

Scans for stuck states in the Event Ledger and outbox tables, retrying with
exponential backoff. Runs on a schedule (default every 60 seconds) and at
process startup.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from engram.config import EngramConfig
from engram.storage.postgres import PostgresStore

log = logging.getLogger(__name__)


@dataclass
class ReconciliationContext:
    cfg: EngramConfig
    control_plane: PostgresStore
    neo4j: Any | None = None  # optional for directory staleness check


def run_once(
    ctx: ReconciliationContext,
    *,
    scan_stale_overviews: bool = True,
) -> dict[str, int]:
    """Single reconciliation pass. Returns counts of what was requeued."""
    counts = {
        "received_stuck": 0,
        "processing_stuck": 0,
        "gated_store_stuck": 0,
        "written_stuck": 0,
        "index_failed_retried": 0,
        "indexed_incomplete": 0,
        "stale_overviews_enqueued": 0,
    }
    conn = ctx.control_plane.get_conn()

    received_age = "created_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes'"
    processing_age = "coalesce(processed_at, created_at) < CURRENT_TIMESTAMP - INTERVAL '5 minutes'"
    written_age = "written_at < CURRENT_TIMESTAMP - INTERVAL '2 minutes'"
    indexed_age = "processed_at < CURRENT_TIMESTAMP - INTERVAL '2 minutes'"

    # 1. events.status = RECEIVED and > 5 minutes old → requeue
    stuck_received = conn.execute(
        f"SELECT event_id FROM events WHERE status = 'RECEIVED' AND {received_age}"
    ).fetchall()
    for row in stuck_received:
        _requeue_event(ctx.control_plane, row["event_id"])
        counts["received_stuck"] += 1

    # 1b. events.status = PROCESSING and claim age > 5 minutes → replay.
    # The durable worker uses PROCESSING as a shared claim state. If its
    # process dies after claiming but before completion, reconciliation can
    # safely re-drive the idempotent pipeline.
    stuck_processing = conn.execute(
        f"SELECT event_id FROM events WHERE status = 'PROCESSING' AND {processing_age}"
    ).fetchall()
    for row in stuck_processing:
        _requeue_event(ctx.control_plane, row["event_id"])
        counts["processing_stuck"] += 1

    # 2. events.status = GATED_STORE with no extraction → requeue
    stuck_gated = conn.execute(
        "SELECT e.event_id FROM events e "
        "LEFT JOIN extractions x ON x.event_id = e.event_id "
        "WHERE e.status = 'GATED_STORE' AND x.event_id IS NULL"
    ).fetchall()
    for row in stuck_gated:
        _requeue_event(ctx.control_plane, row["event_id"])
        counts["gated_store_stuck"] += 1

    # 3. fs_outbox.state = WRITTEN for > 2 minutes → replay KG index step
    stuck_written = conn.execute(
        f"SELECT event_id FROM fs_outbox WHERE state = 'WRITTEN' AND {written_age}"
    ).fetchall()
    for row in stuck_written:
        _requeue_event(ctx.control_plane, row["event_id"])
        counts["written_stuck"] += 1

    # 4. fs_outbox.state = INDEX_FAILED with retry_count < 3 → replay with backoff
    failed = conn.execute(
        "SELECT event_id, retry_count FROM fs_outbox "
        "WHERE state = 'INDEX_FAILED' AND retry_count < 3"
    ).fetchall()
    for row in failed:
        _requeue_event(ctx.control_plane, row["event_id"])
        counts["index_failed_retried"] += 1

    # 4b. Indexing committed, but the process died before it enqueued the
    # derived-view tasks and marked the event COMPLETE. Replaying an INDEXED
    # event executes only step 7; it does not repeat model or filesystem work.
    indexed = conn.execute(
        f"SELECT event_id FROM events WHERE status = 'INDEXED' AND {indexed_age}"
    ).fetchall()
    for row in indexed:
        _requeue_event(ctx.control_plane, row["event_id"])
        counts["indexed_incomplete"] += 1

    # 5. §7.4 daily scan: enqueue CONSOLIDATE_OVERVIEW for directories whose
    # child was modified after the overview was regenerated. This is kept
    # separate from the frequent recovery pass below; otherwise every
    # completed task becomes eligible to be enqueued again a minute later.
    if scan_stale_overviews:
        counts["stale_overviews_enqueued"] = run_stale_overview_scan(ctx)

    return counts


def _requeue_event(control_plane: PostgresStore, event_id: str) -> None:
    """Return a stuck event to the durable worker's claimable state."""
    control_plane.requeue_event(event_id)


def run_stale_overview_scan(
    ctx: ReconciliationContext,
    *,
    strict: bool = False,
) -> int:
    """Queue stale directory summaries without replaying stuck ingest work."""
    if ctx.neo4j is None:
        return 0
    enqueued = 0
    try:
        stale = ctx.neo4j.run_template(
            "MATCH (d:Node)-[:CONTAINS]->(c:Node) "
            "WHERE d.node_type = 'DIRECTORY' "
            "AND (d.overview_generated_at IS NULL OR "
            "     c.created_at > d.overview_generated_at OR "
            "     coalesce(c.superseded_at, '') > "
            "         coalesce(d.overview_generated_at, '')) "
            "RETURN DISTINCT d.source_uri AS uri, "
            "coalesce(d.tenant_id, c.tenant_id, '_default') AS tenant_id "
            "LIMIT 200",
            {},
            timeout_s=10,
        )
        for row in stale:
            uri = row.get("uri")
            tenant_id = str(row.get("tenant_id") or "_default")
            if uri and ctx.control_plane.enqueue_task(
                node_id=str(uri),
                task_type="CONSOLIDATE_OVERVIEW",
                priority=6,
                tenant_id=tenant_id,
            ):
                enqueued += 1
    except Exception:
        if strict:
            raise
        log.debug("stale-directory scan failed", exc_info=True)
    return enqueued


def run_forever(ctx: ReconciliationContext, stop: threading.Event) -> None:
    interval = max(10, ctx.cfg.event_ledger.reconciliation_interval_seconds)
    stale_scan_interval = max(
        interval,
        ctx.cfg.event_ledger.stale_overview_scan_interval_seconds,
    )
    # Recovery runs immediately, but do not turn a process restart into a
    # full maintenance scan. The first stale-directory scan is due after its
    # configured daily cadence.
    next_stale_scan = time.monotonic() + stale_scan_interval

    def reconcile() -> None:
        nonlocal next_stale_scan
        now = time.monotonic()
        scan_stale_overviews = now >= next_stale_scan
        run_once(ctx, scan_stale_overviews=scan_stale_overviews)
        if scan_stale_overviews:
            next_stale_scan = now + stale_scan_interval

    try:
        reconcile()
    except Exception:
        log.exception("initial reconciliation pass failed")
    while not stop.is_set():
        stop.wait(interval)
        if stop.is_set():
            break
        try:
            reconcile()
        except Exception:
            log.exception("reconciliation pass failed")


def start_background(
    ctx: ReconciliationContext,
    *,
    redis_url: str | None = None,
) -> tuple[threading.Thread, threading.Event]:
    """Start the reconciliation worker, optionally under a Redis lease."""
    from engram.coordination import build_lease, run_as_leader

    stop = threading.Event()
    if redis_url:
        lease = build_lease(redis_url, "reconciliation-worker", ttl_seconds=30.0)
        thread = threading.Thread(
            target=run_as_leader,
            args=(lease, stop, lambda inner_stop: run_forever(ctx, inner_stop)),
            name="engram-reconciliation-leader",
            daemon=True,
        )
    else:
        thread = threading.Thread(
            target=run_forever,
            args=(ctx, stop),
            name="engram-reconciliation",
            daemon=True,
        )
    thread.start()
    return thread, stop
