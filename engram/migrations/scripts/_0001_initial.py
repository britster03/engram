"""Baseline migration — establishes schema_version = 1 on a fresh deployment.

No structural changes; the SQLite and filesystem schemas are already created
by their respective stores on first open. This migration exists so that
future scripts can rely on the meta.schema_version row being present.
"""

from __future__ import annotations

from engram.migrations.runner import MigrationContext, ensure_meta, set_meta

SCHEMA_VERSION = 1


def upgrade(ctx: MigrationContext) -> None:
    ensure_meta(ctx.sqlite)
    set_meta(ctx.sqlite, "schema_version", str(SCHEMA_VERSION))
