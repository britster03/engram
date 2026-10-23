"""Deployable configurations must select the same durable memory contract."""

from pathlib import Path

import pytest

from engram.config import load_config

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("profile", [
    "config.yaml", "config.opencode-go.yaml", "config.dev.postgres.yaml",
    "config.staging.yaml", "config.prod.yaml",
])
def test_deployment_profiles_use_canonical_runtime(profile, monkeypatch, tmp_path):
    for key in (
        "ENGRAM_API_KEY", "ENGRAM_ADMIN_KEY", "OPENCODE_GO_API_KEY", "NEO4J_ADMIN_PASSWORD",
        "ENGRAM_STAGING_API_KEY", "ENGRAM_STAGING_ADMIN_KEY", "ENGRAM_STAGING_NEO4J_PASSWORD",
    ):
        monkeypatch.setenv(key, "profile-validation-secret")
    for key in ("ENGRAM_DATABASE_URL", "ENGRAM_DEV_DATABASE_URL", "ENGRAM_STAGING_DATABASE_URL"):
        monkeypatch.setenv(key, "postgresql://test:test@localhost/isolated")
    monkeypatch.setenv("ENGRAM_STAGING_NEO4J_URI", "bolt://localhost:7687")
    monkeypatch.setenv("ENGRAM_STAGING_REDIS_URL", "redis://localhost:6379")
    monkeypatch.setenv("ENGRAM_STAGING_TEMPORAL_ADDRESS", "localhost:7233")
    env_file = tmp_path / ".env"
    env_file.write_text("")
    cfg = load_config(ROOT / profile, env_file=env_file)
    assert cfg.canonical_memory.enabled
    assert cfg.canonical_memory.require_temporal
    assert not cfg.canonical_memory.allow_filesystem_runtime
    assert cfg.temporal.enabled
    assert cfg.canonical_memory.evidence_mode == "enforce"
    assert cfg.consolidation.quality_mode == "enforce"
    assert cfg.retrieval.quality_mode == "enforce"
    assert not cfg.filesystem.create_dirs
    assert cfg.core_model.api_base == "https://opencode.ai/zen/go/v1"
    assert cfg.frontier_llm.api_base == cfg.core_model.api_base
