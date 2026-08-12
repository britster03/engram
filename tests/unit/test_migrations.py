"""Migration runner tests (§13.5)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from engram.config import EngramConfig
from engram.migrations.runner import (
    current_schema_version,
    is_in_maintenance,
    run_pending,
)
from engram.storage.sqlite import SqliteStore


@pytest.fixture
def cfg(tmp_path: Path) -> EngramConfig:
    return EngramConfig.model_validate({
        "api": {"api_key": "test-key"},
        "core_model": {"provider": "ollama_cloud", "api_key": "x"},
        "frontier_llm": {"provider": "ollama_cloud", "api_key": "x"},
        "filesystem": {"data_dir": str(tmp_path / "mem")},
        "event_ledger": {"path": str(tmp_path / "ev.db")},
        "knowledge_graph": {"writer_password": "x", "reader_password": "x"},
    })


def test_run_pending_sets_version_and_clears_maintenance(cfg: EngramConfig):
    summary = run_pending(cfg)
    assert summary["current_version"] >= 1
    sqlite = SqliteStore(cfg.event_ledger.path)
    assert current_schema_version(sqlite) >= 1
    assert is_in_maintenance(sqlite) is False


def test_run_pending_is_idempotent(cfg: EngramConfig):
    summary_1 = run_pending(cfg)
    summary_2 = run_pending(cfg)
    assert summary_2["applied"] == 0
    assert summary_2["current_version"] == summary_1["current_version"]


def test_run_pending_upgrades_existing_v3_consolidation_table(cfg: EngramConfig):
    conn = sqlite3.connect(cfg.event_ledger.path)
    conn.executescript(
        """
        CREATE TABLE consolidation_tasks (
            task_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT '_default',
            node_id TEXT NOT NULL,
            task_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            priority INTEGER NOT NULL DEFAULT 5,
            scheduled_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO meta (key, value) VALUES ('schema_version', '3');
        """
    )
    conn.close()

    summary = run_pending(cfg)
    assert summary["current_version"] >= 6
    upgraded = sqlite3.connect(cfg.event_ledger.path)
    columns = {row[1] for row in upgraded.execute("PRAGMA table_info(consolidation_tasks)")}
    upgraded.close()
    assert {"not_before", "generation", "claimed_generation", "child_signature"} <= columns


def test_migration_removes_global_event_pair_uniqueness(cfg: EngramConfig):
    conn = sqlite3.connect(cfg.event_ledger.path)
    conn.executescript(
        """
        CREATE TABLE events (
            event_id      TEXT PRIMARY KEY,
            pair_id       TEXT UNIQUE NOT NULL,
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
        );
        """
    )
    conn.execute(
        "INSERT INTO events (event_id, pair_id, tenant_id, session_id, source, event_type, payload) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("evt-old", "pair-1", "tenant-a", "s", "client", "INGEST", json.dumps({})),
    )
    conn.commit()
    conn.close()

    run_pending(cfg)
    sqlite = SqliteStore(cfg.event_ledger.path)
    eid_b, is_new_b = sqlite.record_event(
        pair_id="pair-1",
        session_id="s",
        source="client",
        event_type="INGEST",
        payload={},
        tenant_id="tenant-b",
    )
    eid_a, is_new_a = sqlite.record_event(
        pair_id="pair-1",
        session_id="s",
        source="client",
        event_type="INGEST",
        payload={},
        tenant_id="tenant-a",
    )
    assert is_new_b is True
    assert eid_b != "evt-old"
    assert is_new_a is False
    assert eid_a == "evt-old"


def test_migration_adds_persisted_event_retry_schedule(cfg: EngramConfig):
    conn = sqlite3.connect(cfg.event_ledger.path)
    conn.executescript(
        """
        CREATE TABLE events (
            event_id TEXT PRIMARY KEY,
            pair_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL DEFAULT '_default',
            session_id TEXT,
            source TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'RECEIVED',
            retry_count INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            processed_at TEXT
        );
        CREATE TABLE meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO meta (key, value) VALUES ('schema_version', '6');
        """
    )
    conn.close()

    summary = run_pending(cfg)

    assert summary["current_version"] >= 7
    upgraded = sqlite3.connect(cfg.event_ledger.path)
    columns = {row[1] for row in upgraded.execute("PRAGMA table_info(events)")}
    indexes = {row[1] for row in upgraded.execute("PRAGMA index_list(events)")}
    upgraded.close()
    assert "next_attempt_at" in columns
    assert "idx_events_claimable" in indexes
