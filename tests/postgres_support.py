"""Isolated PostgreSQL control-plane stores for tests."""

from __future__ import annotations

import atexit
import os
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from engram.storage.postgres import PostgresStore

_SCHEMAS: list[tuple[str, str]] = []


def _test_dsn() -> str:
    dsn = os.environ.get("ENGRAM_TEST_DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "PostgreSQL-only tests require ENGRAM_TEST_DATABASE_URL pointing to an "
            "isolated test database"
        )
    return dsn


class PostgresTestStore(PostgresStore):
    """PostgreSQL store isolated in a unique schema for each test instance."""

    def __init__(self, _legacy_path: object | None = None) -> None:
        base_dsn = _test_dsn()
        schema = f"engram_test_{uuid.uuid4().hex}"
        with psycopg.connect(base_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        _SCHEMAS.append((base_dsn, schema))
        scoped_dsn = make_conninfo(base_dsn, options=f"-c search_path={schema}")
        super().__init__(scoped_dsn, initialize_schema=True)


@atexit.register
def _drop_test_schemas() -> None:
    while _SCHEMAS:
        dsn, schema = _SCHEMAS.pop()
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
                )
        except Exception:
            # Test teardown must not hide the original test result when the
            # database has already stopped.
            pass
