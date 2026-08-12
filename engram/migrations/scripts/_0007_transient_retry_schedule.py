"""Add a persisted not-before lease for transient ingest retries."""

from __future__ import annotations

from engram.migrations.runner import MigrationContext, ensure_meta, set_meta

SCHEMA_VERSION = 7


def upgrade(ctx: MigrationContext) -> None:
    ensure_meta(ctx.sqlite)
    conn = ctx.sqlite.get_conn()
    existing = {str(row["name"]) for row in conn.execute("PRAGMA table_info(events)")}
    if "next_attempt_at" not in existing:
        conn.execute("ALTER TABLE events ADD COLUMN next_attempt_at TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_claimable "
        "ON events(status, next_attempt_at, created_at)"
    )
    set_meta(ctx.sqlite, "schema_version", str(SCHEMA_VERSION))
