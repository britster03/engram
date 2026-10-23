
import pytest
import yaml
from pydantic import ValidationError

from engram.config import CONFIG_ENV_SETTINGS, SessionConfig, load_config


@pytest.fixture(autouse=True)
def clean_settings(monkeypatch):
    for name in [*CONFIG_ENV_SETTINGS, "ENGRAM_ENV_FILE", "LOCOMO_PROJECT"]:
        monkeypatch.delenv(name, raising=False)


def config_file(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump({"api": {"api_key": "test-key"},
        "event_ledger": {"dsn": "postgresql://test:test/test"},
        "session": {"compaction_trigger_tokens": 12000}}))
    return p


def test_process_over_dotenv_over_yaml_without_export(tmp_path, monkeypatch):
    p = config_file(tmp_path)
    (tmp_path / ".env").write_text("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS=24000\nENGRAM_RETRIEVAL_QUALITY_MODE=enforce\nONLY_IN_FILE=private\n")
    cfg = load_config(p)
    assert cfg.session.compaction_trigger_tokens == 24000
    assert cfg.retrieval.quality_mode == "enforce"
    monkeypatch.setenv("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS", "48000")
    assert load_config(p).session.compaction_trigger_tokens == 48000
    import os
    assert "ONLY_IN_FILE" not in os.environ


def test_optional_dotenv_and_typed_defaults(tmp_path):
    p = config_file(tmp_path)
    cfg = load_config(p)
    assert cfg.session.compaction_trigger_tokens == 12000
    assert cfg.session.compaction_input_tokens == 64000
    assert cfg.retrieval.quality_mode == "legacy"
    assert SessionConfig().compaction_trigger_tokens == 64000


def test_explicit_file_and_existing_secret_interpolation(tmp_path, monkeypatch):
    p = config_file(tmp_path)
    data = yaml.safe_load(p.read_text())
    data["api"]["api_key"] = "${PRIVATE_TEST_KEY}"
    p.write_text(yaml.safe_dump(data))
    selected = tmp_path / "selected.env"
    selected.write_text('PRIVATE_TEST_KEY="private-test-secret"\nENGRAM_SESSION_COMPACTION_TARGET_RATIO=0.6\n')
    monkeypatch.setenv("ENGRAM_ENV_FILE", str(selected))
    cfg = load_config(p)
    assert cfg.api.api_key == "private-test-secret"
    assert cfg.session.compaction_target_ratio == 0.6
    monkeypatch.setenv("ENGRAM_ENV_FILE", str(tmp_path / "missing.env"))
    with pytest.raises(FileNotFoundError):
        load_config(p)


@pytest.mark.parametrize("name,value", [
    ("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS", ""),
    ("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS", "64k"),
    ("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS", "2"),
    ("ENGRAM_SESSION_COMPACTION_TARGET_RATIO", "1"),
    ("ENGRAM_SESSION_COMPACTION_TARGET_RATIO", "nan"),
    ("ENGRAM_SESSION_COMPACTION_INPUT_TOKENS", "256"),
    ("ENGRAM_SESSION_COMPACTION_SUMMARY_TOKENS", "9000"),
    ("ENGRAM_RETRIEVAL_QUALITY_MODE", "incorrect"),
])
def test_invalid_environment_fails(tmp_path, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises((ValueError, ValidationError)):
        load_config(config_file(tmp_path))


def test_deprecated_keys_warn_but_do_not_select_turn_trigger():
    with pytest.warns(DeprecationWarning):
        cfg = SessionConfig(max_turns_before_window=2, window_threshold_ratio=0.1)
    assert cfg.compaction_trigger_tokens == 64000


def test_frozen_benchmark_settings_ignore_ambient_override(tmp_path, monkeypatch):
    p = config_file(tmp_path)
    data = yaml.safe_load(p.read_text())
    data["_benchmark_config_frozen"] = True
    p.write_text(yaml.safe_dump(data))
    monkeypatch.setenv("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS", "48000")
    with pytest.raises(ValueError, match="isolated"):
        load_config(p)
    monkeypatch.setenv("LOCOMO_PROJECT", "engram-lifecycle-test")
    assert load_config(p).session.compaction_trigger_tokens == 12000

@pytest.mark.parametrize("value", ["64000.0", 64000.0, True])
def test_token_budgets_require_integer_values(value):
    with pytest.raises(ValidationError, match="integers"):
        SessionConfig(compaction_trigger_tokens=value)


def test_explicit_empty_env_selector_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("ENGRAM_ENV_FILE", "")
    with pytest.raises(ValueError):
        load_config(config_file(tmp_path))
def test_api_and_durable_chat_share_effective_policy(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from engram.api.routes.chat import _manager as chat_manager
    from engram.api.routes.sessions import _manager as session_manager
    from engram.config import SessionCacheConfig
    from engram.deps import make_session_manager
    from engram.storage.redis_cache import SessionCache
    monkeypatch.setenv("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS", "48000")
    cfg = load_config(config_file(tmp_path))
    runtime = SimpleNamespace(cfg=cfg, session_cache=SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=3600),
                              control_plane=None)
    for manager in [chat_manager(runtime), session_manager(runtime), make_session_manager(runtime)]:
        assert manager.session_config is cfg.session
        assert manager.session_config.compaction_trigger_tokens == 48000
        assert manager.core_output_tokens == cfg.core_model.max_tokens


def test_benchmark_preparation_freezes_effective_values(tmp_path, monkeypatch):
    from benchmarks.lifecycle.infrastructure import prepare
    p = config_file(tmp_path)
    (tmp_path / ".env").write_text("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS=24000\nENGRAM_RETRIEVAL_QUALITY_MODE=enforce\n")
    directory = tmp_path / "run"
    prepare(directory, p, "engram-lifecycle-config-test")
    frozen = directory / ".runtime/config.yaml"
    data = yaml.safe_load(frozen.read_text())
    assert data["session"]["compaction_trigger_tokens"] == 24000
    assert data["retrieval"]["quality_mode"] == "enforce"
    assert data["_benchmark_config_frozen"] is True
    monkeypatch.setenv("LOCOMO_PROJECT", "engram-lifecycle-config-test")
    monkeypatch.setenv("ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS", "64000")
    monkeypatch.setenv("ENGRAM_RETRIEVAL_QUALITY_MODE", "legacy")
    credentials = directory / ".runtime/credentials.env"
    assert load_config(frozen, env_file=credentials).session.compaction_trigger_tokens == 24000
    assert load_config(frozen, env_file=credentials).retrieval.quality_mode == "enforce"
