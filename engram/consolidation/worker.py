"""Consolidation worker — polls the SQLite queue and dispatches tasks (§7.2).

Runs as a daemon thread owned by the FastAPI lifespan (or the CLI when
invoked from `engram` operations). Uses the unique `idx_tasks_pending_unique`
index to debounce (§7.5): duplicate enqueues for the same (node_id, task_type)
while one is PENDING or PROCESSING are silently coalesced.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any

from engram.config import EngramConfig
from engram.consolidation import tasks as handlers
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore
from engram.storage.sqlite import SqliteStore
from engram.tenancy import Tenant, TenantQuotas, get_current_tenant, set_current_tenant

log = logging.getLogger(__name__)


@dataclass
class ConsolidationContext:
    cfg: EngramConfig
    sqlite: SqliteStore
    fs: FilesystemStore
    neo4j: Any
    core: CoreModelProvider
    embed: EmbeddingService
    overview_cache: object | None = None


def _next_task(sqlite: SqliteStore) -> dict | None:
    conn = sqlite.get_conn()
    with sqlite.transaction():
        row = conn.execute(
            "SELECT * FROM consolidation_tasks "
            "WHERE status = 'PENDING' "
            "ORDER BY priority ASC, scheduled_at ASC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE consolidation_tasks SET status = 'PROCESSING', started_at = datetime('now') "
            "WHERE task_id = ?",
            (row["task_id"],),
        )
    return dict(row)


def _complete_task(sqlite: SqliteStore, task_id: str, status: str, err: str | None = None) -> None:
    with sqlite.transaction() as conn:
        conn.execute(
            "UPDATE consolidation_tasks SET status = ?, completed_at = datetime('now'), "
            "error_message = ? WHERE task_id = ?",
            (status, err, task_id),
        )


def process_one(ctx: ConsolidationContext) -> bool:
    """Return True if a task was processed; False if the queue was empty."""
    task = _next_task(ctx.sqlite)
    if task is None:
        return False
    previous_tenant = get_current_tenant()
    task_tenant = str(task.get("tenant_id") or "_default")
    set_current_tenant(
        Tenant(
            tenant_id=task_tenant,
            display_name=task_tenant,
            quotas=TenantQuotas(),
        )
    )
    try:
        _dispatch(ctx, task)
        _complete_task(ctx.sqlite, task["task_id"], "COMPLETE")
    except Exception as err:
        log.exception("consolidation task %s failed", task["task_id"])
        _complete_task(ctx.sqlite, task["task_id"], "FAILED", str(err))
    finally:
        set_current_tenant(previous_tenant)
    return True


def _dispatch(ctx: ConsolidationContext, task: dict) -> None:
    t = task["task_type"]
    node_id = task["node_id"]
    if t == "REGENERATE_MANIFEST":
        handlers.handle_regenerate_manifest(
            node_id=node_id, fs=ctx.fs, cfg=ctx.cfg.consolidation
        )
    elif t == "CONSOLIDATE_OVERVIEW":
        handlers.handle_consolidate_overview(
            node_id=node_id,
            fs=ctx.fs,
            neo4j=ctx.neo4j,
            core=ctx.core,
            cfg=ctx.cfg.consolidation,
            overview_cache=ctx.overview_cache,
        )
    elif t == "PROPAGATE_OVERVIEW":
        handlers.handle_propagate_overview(
            node_id=node_id,
            sqlite=ctx.sqlite,
            cfg=ctx.cfg.consolidation,
            tenant_id=task["tenant_id"],
        )
    elif t == "ATOMIZE":
        handlers.handle_atomize(
            node_id=node_id, sqlite=ctx.sqlite, cfg=ctx.cfg.consolidation
        )
    elif t == "NORMALIZE":
        handlers.handle_normalize(
            node_id=node_id, sqlite=ctx.sqlite, neo4j=ctx.neo4j,
            embed=ctx.embed, cfg=ctx.cfg.consolidation,
        )
    elif t == "TEMPORALIZE":
        handlers.handle_temporalize(
            node_id=node_id, fs=ctx.fs, cfg=ctx.cfg.consolidation
        )
    elif t == "INTEGRATE":
        handlers.handle_integrate(
            node_id=node_id, sqlite=ctx.sqlite, cfg=ctx.cfg.consolidation
        )
    elif t == "UNMERGE":
        handlers.handle_unmerge(
            node_id=node_id, fs=ctx.fs, neo4j=ctx.neo4j, sqlite=ctx.sqlite,
            core=ctx.core, embed=ctx.embed, cfg=ctx.cfg.consolidation,
            tenant_id=task["tenant_id"],
        )
    else:
        log.warning("unknown consolidation task type %r; marking complete", t)


def run_forever(ctx: ConsolidationContext, stop: threading.Event) -> None:
    """Block until `stop` is set, processing tasks as they arrive."""
    interval = max(1, ctx.cfg.consolidation.poll_interval_seconds)
    while not stop.is_set():
        did_work = False
        for _ in range(ctx.cfg.consolidation.max_concurrent_tasks):
            if process_one(ctx):
                did_work = True
            else:
                break
        if not did_work:
            stop.wait(interval)


def start_background(
    ctx: ConsolidationContext, *, redis_url: str | None = None,
) -> tuple[threading.Thread, threading.Event]:
    """Start the consolidation worker, optionally under a Redis-backed lease.

    When `redis_url` is set, only one replica holds the lease at a time;
    other replicas stay in standby. When `redis_url` is None we run
    unconditionally (single-replica deployments).
    """
    from engram.coordination import build_lease, run_as_leader

    stop = threading.Event()
    if redis_url:
        lease = build_lease(redis_url, "consolidation-worker", ttl_seconds=30.0)
        thread = threading.Thread(
            target=run_as_leader,
            args=(lease, stop, lambda inner_stop: run_forever(ctx, inner_stop)),
            name="engram-consolidation-leader",
            daemon=True,
        )
    else:
        thread = threading.Thread(
            target=run_forever, args=(ctx, stop),
            name="engram-consolidation", daemon=True,
        )
    thread.start()
    return thread, stop
