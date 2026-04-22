"""Migration runner tests (§13.5)."""

from __future__ import annotations

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
        "core_model": {"provider": "anthropic", "api_key": "x"},
        "frontier_llm": {"provider": "anthropic", "api_key": "x"},
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
