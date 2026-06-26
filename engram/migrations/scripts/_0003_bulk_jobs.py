"""Bulk ingest job status table."""

from __future__ import annotations

from engram.migrations.runner import MigrationContext, ensure_meta, set_meta

SCHEMA_VERSION = 3


def upgrade(ctx: MigrationContext) -> None:
    ensure_meta(ctx.sqlite)
    ctx.sqlite.get_conn().executescript(
        """
        CREATE TABLE IF NOT EXISTS bulk_jobs (
            job_id          TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL DEFAULT '_default',
            source          TEXT NOT NULL,
            filename        TEXT,
            dry_run         INTEGER NOT NULL DEFAULT 0,
            status          TEXT NOT NULL,
            total_count     INTEGER NOT NULL DEFAULT 0,
            accepted_count  INTEGER NOT NULL DEFAULT 0,
            rejected_count  INTEGER NOT NULL DEFAULT 0,
            rejected_rows   TEXT NOT NULL DEFAULT '[]',
            event_ids       TEXT NOT NULL DEFAULT '[]',
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            completed_at    TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bulk_jobs_tenant_created
            ON bulk_jobs(tenant_id, created_at);
        """
    )
    set_meta(ctx.sqlite, "schema_version", str(SCHEMA_VERSION))
