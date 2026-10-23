"""Bounded, resumable lexical reindex handoff through the existing outbox."""

from __future__ import annotations

from typing import Any


def backfill_search(
    repository: Any, *, tenant_id: str, cursor: str | None = None, limit: int = 100
) -> dict[str, Any]:
    page = repository.list_memories(tenant_id=tenant_id, limit=limit, cursor=cursor)
    for node in page.nodes:
        repository.enqueue_projection(
            aggregate_type="MEMORY",
            aggregate_id=node.id,
            operation="REINDEX_BM25_V1",
            revision=node.revision,
            tenant_id=tenant_id,
            payload={
                "memory_id": str(node.id),
                "aggregate_type": "MEMORY",
                "operation": "UPSERT_NODE",
            },
        )
    return {"submitted": len(page.nodes), "next_cursor": page.next_cursor}
