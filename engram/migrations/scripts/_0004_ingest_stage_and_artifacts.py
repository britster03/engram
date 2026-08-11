"""Persist ingest stages and artifact-level filesystem/KG readiness."""

from __future__ import annotations

from engram.migrations.runner import MigrationContext, ensure_meta, set_meta

SCHEMA_VERSION = 4


def upgrade(ctx: MigrationContext) -> None:
    ensure_meta(ctx.sqlite)
    ctx.sqlite.get_conn().executescript(
        """
        CREATE TABLE IF NOT EXISTS event_stage_state (
            event_id        TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL DEFAULT '_default',
            completed_stage TEXT NOT NULL DEFAULT 'RECEIVED',
            gate_output     TEXT,
            link_output     TEXT,
            updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_event_stage_tenant
            ON event_stage_state(tenant_id, completed_stage, updated_at);

        CREATE TABLE IF NOT EXISTS ingest_artifacts (
            event_id         TEXT NOT NULL,
            tenant_id        TEXT NOT NULL DEFAULT '_default',
            artifact_type    TEXT NOT NULL,
            source_uri       TEXT NOT NULL,
            artifact_id      TEXT NOT NULL,
            content_hash     TEXT NOT NULL,
            required         INTEGER NOT NULL DEFAULT 1,
            filesystem_state TEXT NOT NULL DEFAULT 'PENDING',
            kg_state         TEXT NOT NULL DEFAULT 'PENDING',
            attempt_count    INTEGER NOT NULL DEFAULT 0,
            error_message    TEXT,
            created_at       TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at       TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (event_id, source_uri),
            FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_ingest_artifacts_event
            ON ingest_artifacts(
                tenant_id, event_id, required, filesystem_state, kg_state
            );
        CREATE INDEX IF NOT EXISTS idx_ingest_artifacts_state
            ON ingest_artifacts(tenant_id, filesystem_state, kg_state, updated_at);
        """
    )
    set_meta(ctx.sqlite, "schema_version", str(SCHEMA_VERSION))
