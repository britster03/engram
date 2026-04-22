"""Reconciliation worker (§5.5, §5.6).

Scans for stuck states in the Event Ledger and outbox tables, retrying with
exponential backoff. Runs on a schedule (default every 60 seconds) and at
process startup.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from engram.config import EngramConfig
from engram.storage.sqlite import SqliteStore

log = logging.getLogger(__name__)


@dataclass
class ReconciliationContext:
    cfg: EngramConfig
    sqlite: SqliteStore
    drive_event: callable  # (event_id) -> None, injected by caller
    neo4j: "object | None" = None  # optional for directory staleness check


def run_once(ctx: ReconciliationContext) -> dict[str, int]:
    """Single reconciliation pass. Returns counts of what was requeued."""
    counts = {
        "received_stuck": 0,
        "gated_store_stuck": 0,
        "written_stuck": 0,
        "index_failed_retried": 0,
        "stale_overviews_enqueued": 0,
    }
    conn = ctx.sqlite.get_conn()

    # 1. events.status = RECEIVED and > 5 minutes old → requeue
    stuck_received = conn.execute(
        "SELECT event_id FROM events WHERE status = 'RECEIVED' "
        "AND julianday('now') - julianday(created_at) > 5.0/1440"
    ).fetchall()
    for row in stuck_received:
        ctx.drive_event(row["event_id"])
        counts["received_stuck"] += 1

    # 2. events.status = GATED_STORE with no extraction → requeue
    stuck_gated = conn.execute(
        "SELECT e.event_id FROM events e "
        "LEFT JOIN extractions x ON x.event_id = e.event_id "
        "WHERE e.status = 'GATED_STORE' AND x.event_id IS NULL"
    ).fetchall()
    for row in stuck_gated:
        ctx.drive_event(row["event_id"])
        counts["gated_store_stuck"] += 1

    # 3. fs_outbox.state = WRITTEN for > 2 minutes → replay KG index step
    stuck_written = conn.execute(
        "SELECT event_id FROM fs_outbox WHERE state = 'WRITTEN' "
        "AND julianday('now') - julianday(written_at) > 2.0/1440"
    ).fetchall()
    for row in stuck_written:
        ctx.drive_event(row["event_id"])
        counts["written_stuck"] += 1

    # 4. fs_outbox.state = INDEX_FAILED with retry_count < 3 → replay with backoff
    failed = conn.execute(
        "SELECT event_id, retry_count FROM fs_outbox "
        "WHERE state = 'INDEX_FAILED' AND retry_count < 3"
    ).fetchall()
    for row in failed:
        ctx.drive_event(row["event_id"])
        counts["index_failed_retried"] += 1

    # 5. §7.4 daily scan: enqueue CONSOLIDATE_OVERVIEW for directories whose
    # child was modified after the overview was regenerated.
    if ctx.neo4j is not None:
        try:
            stale = ctx.neo4j.run_template(
                "MATCH (d:Node)-[:CONTAINS]->(c:Node) "
                "WHERE d.node_type = 'DIRECTORY' "
                "AND (d.overview_generated_at IS NULL OR "
                "     c.created_at > d.overview_generated_at OR "
                "     coalesce(c.superseded_at, '') > coalesce(d.overview_generated_at, '')) "
                "RETURN DISTINCT d.source_uri AS uri LIMIT 200",
                {},
                timeout_s=10,
            )
            for row in stale:
                uri = row.get("uri")
                if uri and ctx.sqlite.enqueue_task(
                    node_id=str(uri), task_type="CONSOLIDATE_OVERVIEW", priority=6
                ):
                    counts["stale_overviews_enqueued"] += 1
        except Exception:
            log.debug("stale-directory scan failed", exc_info=True)

    return counts


def run_forever(ctx: ReconciliationContext, stop: threading.Event) -> None:
    interval = max(10, ctx.cfg.event_ledger.reconciliation_interval_seconds)
    # Run immediately on startup
    try:
        run_once(ctx)
    except Exception:
        log.exception("initial reconciliation pass failed")
    while not stop.is_set():
        stop.wait(interval)
        if stop.is_set():
            break
        try:
            run_once(ctx)
        except Exception:
            log.exception("reconciliation pass failed")


def start_background(
    ctx: ReconciliationContext, *, redis_url: str | None = None,
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
            target=run_forever, args=(ctx, stop),
            name="engram-reconciliation", daemon=True,
        )
    thread.start()
    return thread, stop
