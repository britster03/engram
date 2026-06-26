"""Schema migration runner (§13.5).

Migrations are numbered Python modules under `engram/migrations/scripts/`,
each exposing:
  SCHEMA_VERSION: int  # the version this migration brings the repo up to
  def upgrade(ctx: MigrationContext) -> None: ...

The runner:
  1. Pauses ingest by setting a maintenance flag in SQLite.
  2. Drains the ingest and consolidation workers (best-effort — caller is
     expected to stop the worker threads before invoking).
  3. Runs each pending migration in order against the filesystem.
  4. Increments `schema_version` on touched nodes and bumps a global marker
     in SQLite's `meta` table.
  5. Optionally runs rebuild-kg for affected nodes to re-derive Neo4j.
  6. Clears the maintenance flag.

This is a one-way tool: rollback is "restore the data_dir and event_ledger
snapshot." The SDD is explicit that this is the recovery model.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from collections.abc import Callable
from dataclasses import dataclass

from engram.config import EngramConfig
from engram.storage.filesystem import FilesystemStore
from engram.storage.sqlite import SqliteStore

log = logging.getLogger(__name__)


_META_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

_SCHEMA_KEY = "schema_version"
_MAINTENANCE_KEY = "maintenance_mode"


@dataclass
class MigrationContext:
    cfg: EngramConfig
    sqlite: SqliteStore
    fs: FilesystemStore


def ensure_meta(sqlite: SqliteStore) -> None:
    # executescript commits implicitly, so we cannot wrap it in BEGIN IMMEDIATE.
    sqlite.get_conn().executescript(_META_TABLE_SQL)


def get_meta(sqlite: SqliteStore, key: str, default: str = "") -> str:
    ensure_meta(sqlite)
    row = sqlite.get_conn().execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_meta(sqlite: SqliteStore, key: str, value: str) -> None:
    ensure_meta(sqlite)
    with sqlite.transaction() as conn:
        conn.execute(
            "INSERT INTO meta (key, value, updated_at) VALUES (?, ?, datetime('now')) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
            (key, value),
        )


def current_schema_version(sqlite: SqliteStore) -> int:
    return int(get_meta(sqlite, _SCHEMA_KEY, "1"))


def is_in_maintenance(sqlite: SqliteStore) -> bool:
    return get_meta(sqlite, _MAINTENANCE_KEY, "0") == "1"


def pending_migrations() -> list[tuple[int, Callable[[MigrationContext], None]]]:
    """Load every migration script as (version, upgrade_callable)."""
    from engram.migrations import scripts as scripts_pkg
    entries: list[tuple[int, Callable[[MigrationContext], None]]] = []
    for _, mod_name, _ in pkgutil.iter_modules(scripts_pkg.__path__):
        mod = importlib.import_module(f"engram.migrations.scripts.{mod_name}")
        version = int(mod.SCHEMA_VERSION)
        upgrade = mod.upgrade
        entries.append((version, upgrade))
    entries.sort(key=lambda p: p[0])
    return entries


def run_pending(cfg: EngramConfig) -> dict[str, int]:
    """Run every migration whose SCHEMA_VERSION is strictly greater than the
    recorded schema_version. Returns a summary."""
    sqlite = SqliteStore(cfg.event_ledger.path)
    ensure_meta(sqlite)
    current = current_schema_version(sqlite)
    migrations = pending_migrations()
    applied: list[int] = []
    ctx = MigrationContext(
        cfg=cfg,
        sqlite=sqlite,
        fs=FilesystemStore(cfg.filesystem.data_dir, create_dirs=True),
    )
    set_meta(sqlite, _MAINTENANCE_KEY, "1")
    try:
        for version, upgrade in migrations:
            if version <= current:
                continue
            log.info("applying migration %d", version)
            upgrade(ctx)
            set_meta(sqlite, _SCHEMA_KEY, str(version))
            applied.append(version)
            current = version
    finally:
        set_meta(sqlite, _MAINTENANCE_KEY, "0")
    return {"applied": len(applied), "current_version": current}
