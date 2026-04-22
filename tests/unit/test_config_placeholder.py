"""Placeholder-secret guard at config load time."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from engram.config import load_config


def _write_config(tmp_path: Path, api_key_value: str) -> Path:
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "api": {"api_key": api_key_value, "host": "127.0.0.1", "port": 8000,
                        "rate_limit_query_per_minute": 60,
                        "rate_limit_ingest_per_minute": 600},
                "core_model": {"provider": "anthropic", "api_key": "real-key"},
                "frontier_llm": {"provider": "anthropic", "api_key": "real-key"},
                "filesystem": {"data_dir": str(tmp_path / "mem")},
                "event_ledger": {"path": str(tmp_path / "ev.db")},
                "consolidation": {"db_path": str(tmp_path / "cons.db")},
                "knowledge_graph": {"writer_password": "ok", "reader_password": "ok"},
            }
        )
    )
    return cfg_path


def test_placeholder_rejected_by_default(tmp_path: Path):
    cfg_path = _write_config(tmp_path, "change-me-before-exposing-this-port")
    with pytest.raises(ValueError) as excinfo:
        load_config(cfg_path)
    assert "placeholder" in str(excinfo.value).lower()
    assert "api.api_key" in str(excinfo.value)


def test_placeholder_allowed_in_tests(tmp_path: Path):
    cfg_path = _write_config(tmp_path, "change-me-before-exposing-this-port")
    cfg = load_config(cfg_path, enforce_no_placeholders=False)
    assert cfg.api.api_key


def test_real_secret_boots(tmp_path: Path):
    cfg_path = _write_config(tmp_path, "sk-live-real-value-xyz")
    cfg = load_config(cfg_path)
    assert cfg.api.api_key == "sk-live-real-value-xyz"
