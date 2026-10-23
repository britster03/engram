"""Fresh and N-1 upgrades against disposable PostgreSQL schemas."""

import os
import uuid
from urllib.parse import quote

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg import sql


@pytest.mark.parametrize("starting_revision", [None, "0005_ordered_retrieval", "0008_overview_policy"])
def test_plan_migrations(starting_revision, monkeypatch):
    base = os.environ.get("ENGRAM_TEST_DATABASE_URL")
    if not base:
        pytest.skip("requires isolated PostgreSQL")
    schema = "migration_" + uuid.uuid4().hex
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    dsn = base + ("&" if "?" in base else "?") + "options=" + quote("-c search_path=" + schema)
    monkeypatch.setenv("ENGRAM_DATABASE_URL", dsn)
    try:
        cfg = Config("alembic.ini")
        if starting_revision:
            command.upgrade(cfg, starting_revision)
        if starting_revision == "0005_ordered_retrieval":
            # Historical 0001 imports current bootstrap DDL. Remove newer
            # additions to reproduce a database genuinely lacking them.
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("DROP TABLE IF EXISTS chat_runs")
                conn.execute(
                    "ALTER TABLE consolidation_tasks DROP COLUMN IF EXISTS requested_generation, "
                    "DROP COLUMN IF EXISTS processing_generation"
                )
        command.upgrade(cfg, "head")
        with psycopg.connect(dsn) as conn:
            assert (
                conn.execute("SELECT version_num FROM alembic_version").fetchone()[0]
                == "0009_ingestion_policy"
            )
            assert conn.execute("SELECT count(*) FROM chat_runs").fetchone()[0] == 0
            assert conn.execute("SELECT count(*) FROM ingest_reprocessings").fetchone()[0] == 0
            assert (
                conn.execute("SELECT count(*) FROM memory_overview_dependencies").fetchone()[0] == 0
            )
            assert (
                conn.execute(
                    "SELECT requested_generation, processing_generation FROM consolidation_tasks"
                ).fetchall()
                == []
            )
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
