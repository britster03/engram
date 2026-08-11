"""Add source and extractor provenance to artifact-level outbox rows."""

from __future__ import annotations

from engram.migrations.runner import MigrationContext, ensure_meta, set_meta

SCHEMA_VERSION = 6


def upgrade(ctx: MigrationContext) -> None:
    ensure_meta(ctx.sqlite)
    conn = ctx.sqlite.get_conn()
    existing = {str(row["name"]) for row in conn.execute("PRAGMA table_info(ingest_artifacts)")}
    additions = {
        "source_session_id": "TEXT",
        "source_turn_ids": "TEXT NOT NULL DEFAULT '[]'",
        "confidence": "REAL",
        "extractor_version": "TEXT",
    }
    for column, declaration in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE ingest_artifacts ADD COLUMN {column} {declaration}")
    set_meta(ctx.sqlite, "schema_version", str(SCHEMA_VERSION))
