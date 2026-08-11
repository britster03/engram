"""Add generation-aware coalescing fields to the consolidation queue."""

from __future__ import annotations

from engram.migrations.runner import MigrationContext, ensure_meta, set_meta

SCHEMA_VERSION = 5


def upgrade(ctx: MigrationContext) -> None:
    ensure_meta(ctx.sqlite)
    conn = ctx.sqlite.get_conn()
    existing = {str(row["name"]) for row in conn.execute("PRAGMA table_info(consolidation_tasks)")}
    additions = {
        "not_before": "TEXT",
        "generation": "INTEGER NOT NULL DEFAULT 1",
        "claimed_generation": "INTEGER",
        "child_signature": "TEXT",
    }
    for column, declaration in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE consolidation_tasks ADD COLUMN {column} {declaration}")
    set_meta(ctx.sqlite, "schema_version", str(SCHEMA_VERSION))
