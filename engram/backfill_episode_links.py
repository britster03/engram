"""Repair missing episode-to-entity hierarchy edges in canonical PostgreSQL.

Conversation ingest now writes these edges as part of the canonical commit.
This one-shot utility repairs episodes created before that behavior was added;
the projection outbox is used so Neo4j catches up through the normal dispatcher.
"""

from __future__ import annotations

import argparse
import os
import uuid
from collections import defaultdict
from typing import Any

from engram.storage.memory_repository import MemoryRepository
from engram.storage.postgres import PostgresStore
from engram.tenancy import DEFAULT_TENANT_ID


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tenant-id",
        default=os.environ.get("ENGRAM_TENANT_ID", DEFAULT_TENANT_ID),
        help="Tenant slice to repair (default: ENGRAM_TENANT_ID or _default)",
    )
    parser.add_argument(
        "--session-id",
        help="Only repair episodes for this conversation session",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report missing links without writing canonical rows",
    )
    return parser.parse_args()


def _episode_entities(conn: Any, tenant_id: str, session_id: str | None) -> dict[str, set[str]]:
    clauses = [
        "e.tenant_id = ?",
        "e.memory_type = 'EPISODE'",
        "e.status <> 'DELETED'",
        "n.tenant_id = e.tenant_id",
        "n.memory_type = 'ENTITY'",
        "n.status <> 'DELETED'",
        "ev.tenant_id = e.tenant_id",
        "ev.source_event_id = e.origin_event_id",
        "ev.memory_id = n.id",
    ]
    params: list[Any] = [tenant_id]
    if session_id:
        clauses.append("e.metadata->>'session_id' = ?")
        params.append(session_id)
    rows = conn.execute(
        "SELECT DISTINCT e.id AS episode_id, n.id AS entity_id "
        "FROM memory_nodes e "
        "JOIN memory_evidence ev ON ev.tenant_id = e.tenant_id "
        "JOIN memory_nodes n ON n.tenant_id = ev.tenant_id AND n.id = ev.memory_id "
        "WHERE "
        + " AND ".join(clauses),
        tuple(params),
    ).fetchall()
    grouped: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        grouped[str(row["episode_id"])].add(str(row["entity_id"]))
    return grouped


def _as_uuid(value: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"canonical memory id is not a UUID: {value}") from exc


def main() -> int:
    args = _parse_args()
    dsn = os.environ.get("ENGRAM_DATABASE_URL")
    if not dsn:
        raise SystemExit("ENGRAM_DATABASE_URL is required")

    store = PostgresStore(dsn, initialize_schema=False)
    repository = MemoryRepository(store, default_tenant_id=args.tenant_id)
    try:
        conn = repository._tenant_connection(args.tenant_id)
        grouped = _episode_entities(conn, args.tenant_id, args.session_id)
        missing: list[tuple[str, str]] = []
        for episode_id, entity_ids in grouped.items():
            episode_uuid = _as_uuid(episode_id)
            existing = {
                str(node.id)
                for node in repository.get_children(episode_uuid, tenant_id=args.tenant_id)
            }
            missing.extend(
                (episode_id, entity_id)
                for entity_id in sorted(entity_ids)
                if entity_id not in existing
            )

        print(f"episodes_with_entities={len(grouped)} missing_links={len(missing)}")
        if args.dry_run:
            return 0

        created = 0
        for episode_id, entity_id in missing:
            repository.add_hierarchy(
                parent_id=_as_uuid(episode_id),
                child_id=_as_uuid(entity_id),
                metadata={
                    "source": "canonical-conversation-backfill",
                    "relation": "MENTIONS",
                },
                tenant_id=args.tenant_id,
                emit_projection=True,
            )
            created += 1
        print(f"created_links={created}")
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
