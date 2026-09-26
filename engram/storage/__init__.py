"""Storage backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from engram.config import EngramConfig
    from engram.storage.postgres import PostgresStore


class ControlPlaneStore(Protocol):
    """Durable PostgreSQL event/control-plane operations."""

    def get_conn(self): ...
    def transaction(self): ...
    def record_event(self, **kwargs): ...
    def get_event(self, event_id: str, **kwargs): ...
    def set_event_status(self, event_id: str, status: str, *args, **kwargs): ...
    def requeue_event(self, event_id: str, **kwargs): ...
    def save_linked_entities(self, rows): ...
    def enqueue_task(self, **kwargs): ...
    def queue_depth(self, **kwargs): ...
    def get_dispatch(self, dispatch_id: str, **kwargs): ...
    def complete_dispatch(self, dispatch_id: str, **kwargs): ...
    def mark_dispatch_dead(self, dispatch_id: str, error: str, **kwargs): ...


def build_control_plane_store(cfg: EngramConfig) -> PostgresStore:
    """Build the PostgreSQL control plane."""
    from engram.storage.postgres import PostgresStore

    return PostgresStore(
        cfg.event_ledger.dsn or "",
        initialize_schema=False,
        ingest_task_queue=cfg.temporal.ingest_task_queue,
        code_ingest_task_queue=cfg.temporal.code_ingest_task_queue,
        projection_task_queue=cfg.temporal.projection_task_queue,
        consolidation_task_queue=cfg.temporal.consolidation_task_queue,
    )
