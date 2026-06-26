"""Durable ingest worker — replaces FastAPI `BackgroundTasks`.

Why this exists:
  FastAPI's BackgroundTasks run inside the worker process after the response
  is sent. If that process crashes or is restarted between the HTTP 202 and
  the task firing, the event stays in RECEIVED and nothing processes it for
  up to five minutes (until the reconciliation worker notices).

This worker polls the SQLite event ledger directly. The API endpoint's
responsibility shrinks to exactly one thing: commit the event row. The
worker owns everything after that, with bounded concurrency and graceful
shutdown.

Failure model:
  - Transient LLM / Neo4j failure → retry in the next poll cycle (the
    @resilient decorators bubble up exceptions; we catch them here, mark
    the event FAILED, and rely on the reconciliation worker to requeue).
  - Permanent failure (e.g. schema violation) → event marked FAILED with a
    preserved error_message; operators retry manually via
    POST /api/v1/events/{id}/retry.
  - Shutdown → the stop Event gates the poll loop; in-flight events are
    allowed to finish (join timeout), or the OS SIGKILL eventually fires
    and the reconciliation worker picks them up next boot.

Concurrency:
  A pool of N worker threads (default `max_concurrent` from config) pulls
  events from a single queue. Each thread builds its own IngestContext so
  SQLite / Neo4j driver connections are per-thread.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from engram import metrics as metrics_mod
from engram.config import EngramConfig
from engram.ingest.worker import IngestContext, process_event
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore
from engram.storage.sqlite import SqliteStore

log = logging.getLogger(__name__)


@dataclass
class DurableIngestContext:
    cfg: EngramConfig
    sqlite: SqliteStore
    fs: FilesystemStore
    neo4j: Any
    core: CoreModelProvider
    embed: EmbeddingService
    ingest_context_factory: Callable[[], IngestContext]


class DurableIngestWorker:
    """Polls the event ledger for RECEIVED events and processes them."""

    def __init__(
        self,
        ctx: DurableIngestContext,
        *,
        max_concurrent: int = 4,
        poll_interval_seconds: float = 1.0,
        batch_size: int = 32,
    ) -> None:
        self.ctx = ctx
        self.max_concurrent = max_concurrent
        self.poll_interval = poll_interval_seconds
        self.batch_size = batch_size
        self._stop = threading.Event()
        self._poll_thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._queue: queue.Queue[str] = queue.Queue(maxsize=max_concurrent * 4)
        self._in_flight: set[str] = set()
        self._in_flight_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._poll_thread is not None:
            raise RuntimeError("already started")
        for i in range(self.max_concurrent):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"engram-ingest-{i}",
                daemon=True,
            )
            t.start()
            self._workers.append(t)
        self._poll_thread = threading.Thread(
            target=self._poll_loop, name="engram-ingest-poll", daemon=True,
        )
        self._poll_thread.start()
        log.info("durable ingest worker started (concurrency=%d)", self.max_concurrent)

    def stop(self, *, timeout_s: float = 10.0) -> None:
        self._stop.set()
        if self._poll_thread is not None:
            self._poll_thread.join(timeout=timeout_s)
        # Drain remaining items with sentinels so workers exit cleanly.
        for _ in self._workers:
            with suppress(queue.Full):
                self._queue.put_nowait("__STOP__")
        for t in self._workers:
            t.join(timeout=timeout_s)
        log.info("durable ingest worker stopped")

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                claimed = self._claim_batch()
                for event_id in claimed:
                    # Block if the queue is full — we want backpressure, not drops.
                    while not self._stop.is_set():
                        try:
                            self._queue.put(event_id, timeout=0.5)
                            break
                        except queue.Full:
                            continue
            except Exception:
                log.exception("poll loop error")
            self._stop.wait(self.poll_interval)

    def _claim_batch(self) -> list[str]:
        """Claim up to `batch_size` events in status=RECEIVED.

        Claiming is an SQLite transaction that flips RECEIVED → PROCESSING.
        That state transition is shared across API workers and replicas,
        unlike the process-local `_in_flight` set used only for queue hygiene.
        """
        rows = self.ctx.sqlite.claim_pending_events(limit=self.batch_size)
        claimed: list[str] = []
        with self._in_flight_lock:
            for r in rows:
                eid = r["event_id"]
                if eid in self._in_flight:
                    continue
                self._in_flight.add(eid)
                claimed.append(eid)
        return claimed

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    def _worker_loop(self) -> None:
        while True:
            event_id = self._queue.get()
            if event_id == "__STOP__":
                return
            try:
                self._process_one(event_id)
            finally:
                with self._in_flight_lock:
                    self._in_flight.discard(event_id)

    def _process_one(self, event_id: str) -> None:
        started = time.perf_counter()
        try:
            ctx = self.ctx.ingest_context_factory()
            final = process_event(ctx, event_id)
            metrics_mod.ingest_events_total.labels(final_status=final).inc()
        except Exception as err:
            log.exception("durable ingest failed for %s", event_id)
            try:
                self.ctx.sqlite.set_event_status(
                    event_id, "FAILED", error_message=str(err)[:500]
                )
                metrics_mod.ingest_events_total.labels(final_status="FAILED").inc()
            except Exception:
                log.exception("failed to mark event FAILED")
        finally:
            metrics_mod.ingest_stage.labels(stage="total").observe(
                time.perf_counter() - started
            )


def start_background(
    cfg: EngramConfig,
    sqlite: SqliteStore,
    fs: FilesystemStore,
    neo4j: Any,
    core: CoreModelProvider,
    embed: EmbeddingService,
    *,
    max_concurrent: int = 4,
    poll_interval_seconds: float = 1.0,
) -> DurableIngestWorker:
    def _factory() -> IngestContext:
        return IngestContext(
            cfg=cfg, sqlite=sqlite, fs=fs, neo4j=neo4j, core=core, embed=embed,
        )

    ctx = DurableIngestContext(
        cfg=cfg, sqlite=sqlite, fs=fs, neo4j=neo4j, core=core, embed=embed,
        ingest_context_factory=_factory,
    )
    worker = DurableIngestWorker(
        ctx,
        max_concurrent=max_concurrent,
        poll_interval_seconds=poll_interval_seconds,
    )
    worker.start()
    return worker
