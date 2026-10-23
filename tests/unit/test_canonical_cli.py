"""Migrations must use the database selected by the loaded profile."""

from types import SimpleNamespace

from engram.cli import cmd_migrate


def test_migration_uses_selected_config_database(monkeypatch):
    import alembic.command

    import engram.config

    expected = "postgresql://dev:dev@localhost:5433/isolated"
    monkeypatch.setenv("ENGRAM_DATABASE_URL", "postgresql://wrong:wrong@localhost/other")
    monkeypatch.setattr(engram.config, "get_config", lambda: SimpleNamespace(
        event_ledger=SimpleNamespace(dsn=expected)))
    seen = []
    monkeypatch.setattr(alembic.command, "upgrade", lambda cfg, target:
                       seen.append((cfg.attributes["connection_dsn"], target)))
    assert cmd_migrate(SimpleNamespace()) == 0
    assert seen == [(expected, "head")]


def test_smoke_uses_canonical_workers_without_constructing_filesystem_context(monkeypatch):
    from unittest.mock import Mock

    import engram.cli
    import engram.projection_status

    store = Mock()
    store.record_event.return_value = ("evt-test", True)
    store.get_event.return_value = {"status": "COMPLETE"}
    state = SimpleNamespace(cfg=SimpleNamespace(canonical_memory=SimpleNamespace(enabled=True)),
                            control_plane=store)
    monkeypatch.setattr(engram.cli, "build_state", lambda: state)
    legacy = Mock(side_effect=AssertionError("canonical smoke cannot use legacy ingestion"))
    monkeypatch.setattr(engram.cli, "make_ingest_context", legacy)
    monkeypatch.setattr(engram.projection_status, "projection_status_for_event", lambda *a, **k: "INDEXED")
    monkeypatch.setattr(engram.cli, "make_orchestrator_context", lambda state: object())
    monkeypatch.setattr(engram.cli, "run_query", lambda *a, **k: SimpleNamespace(
        answer="Verified answer", retrieval_metadata=SimpleNamespace(to_dict=lambda: {})))
    assert engram.cli.cmd_smoke(SimpleNamespace(timeout_seconds=1)) == 0
    assert store.record_event.call_count == len(engram.cli._SMOKE_CORPUS)
    legacy.assert_not_called()
