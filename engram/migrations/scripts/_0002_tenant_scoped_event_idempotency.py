"""Tenant-scope event idempotency and durable ingest claims.

Older ledgers made events.pair_id globally unique, which caused two tenants
using the same session/turn indexes to dedupe into each other. Rebuild the
events table without the inline UNIQUE constraint and add a tenant-scoped
unique index instead.
"""

from __future__ import annotations

from engram.migrations.runner import MigrationContext, ensure_meta, set_meta

SCHEMA_VERSION = 2


def upgrade(ctx: MigrationContext) -> None:
    ensure_meta(ctx.sqlite)
    conn = ctx.sqlite.get_conn()
    if _needs_events_rebuild(conn):
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                """CREATE TABLE events_new (
                    event_id      TEXT PRIMARY KEY,
                    pair_id       TEXT NOT NULL,
                    tenant_id     TEXT NOT NULL DEFAULT '_default',
                    session_id    TEXT,
                    source        TEXT NOT NULL,
                    event_type    TEXT NOT NULL,
                    payload       TEXT NOT NULL,
                    status        TEXT NOT NULL DEFAULT 'RECEIVED',
                    retry_count   INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT,
                    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                    processed_at  TEXT
                )"""
            )
            conn.execute(
                """INSERT INTO events_new (
                    event_id, pair_id, tenant_id, session_id, source, event_type,
                    payload, status, retry_count, error_message, created_at, processed_at
                )
                SELECT
                    event_id, pair_id, COALESCE(tenant_id, '_default'), session_id,
                    source, event_type, payload, status, retry_count, error_message,
                    created_at, processed_at
                FROM events"""
            )
            conn.execute("DROP TABLE events")
            conn.execute("ALTER TABLE events_new RENAME TO events")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_events_status   ON events(status, created_at);
        CREATE INDEX IF NOT EXISTS idx_events_session  ON events(session_id);
        CREATE INDEX IF NOT EXISTS idx_events_tenant   ON events(tenant_id, status, created_at);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_events_tenant_pair ON events(tenant_id, pair_id);
        """
    )
    set_meta(ctx.sqlite, "schema_version", str(SCHEMA_VERSION))


def _needs_events_rebuild(conn) -> bool:
    indexes = conn.execute("PRAGMA index_list(events)").fetchall()
    names = {row["name"] for row in indexes}
    if "idx_events_tenant_pair" not in names:
        return True
    return any(row["unique"] and row["name"] != "idx_events_tenant_pair" for row in indexes)
