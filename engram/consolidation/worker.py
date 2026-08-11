"""Consolidation worker — polls the generation-aware SQLite queue (§7.2)."""

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
            "AND datetime(COALESCE(not_before, scheduled_at)) <= datetime('now') "
            "ORDER BY priority ASC, scheduled_at ASC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE consolidation_tasks SET status = 'PROCESSING', started_at = datetime('now') "
            ", claimed_generation = generation WHERE task_id = ?",
            (row["task_id"],),
        )
    return dict(row)


def _complete_task(sqlite: SqliteStore, task_id: str, status: str, err: str | None = None) -> None:
    with sqlite.transaction() as conn:
        current = conn.execute(
            "SELECT task_type, generation, claimed_generation FROM consolidation_tasks "
            "WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if (
            current is not None
            and status == "COMPLETE"
            and current["task_type"] == "REFRESH_DIRECTORY"
            and int(current["generation"] or 0) > int(current["claimed_generation"] or 0)
        ):
            conn.execute(
                "UPDATE consolidation_tasks SET status = 'PENDING', started_at = NULL, "
                "completed_at = NULL, error_message = NULL WHERE task_id = ?",
                (task_id,),
            )
            return
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
    elif t == "REFRESH_DIRECTORY":
        handlers.handle_refresh_directory(
            node_id=node_id,
            sqlite=ctx.sqlite,
            fs=ctx.fs,
            neo4j=ctx.neo4j,
            core=ctx.core,
            cfg=ctx.cfg.consolidation,
            tenant_id=task["tenant_id"],
            overview_cache=ctx.overview_cache,
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
