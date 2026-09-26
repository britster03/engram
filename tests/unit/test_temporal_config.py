from __future__ import annotations

import pytest

from engram.config import EngramConfig


def test_temporal_configuration_is_opt_in_and_has_safe_defaults():
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "test"},
            "event_ledger": {"dsn": "postgresql://test:test@localhost/test"},
        }
    )
    assert cfg.temporal.enabled is False
    assert cfg.temporal.namespace == "engram-prod"
    assert cfg.temporal.maintenance_task_queue == "engram-maintenance"
    assert cfg.temporal.history_retention_days == 14


def test_postgres_control_plane_requires_a_dsn():
    try:
        EngramConfig.model_validate(
            {
                "api": {"api_key": "test"},
                "event_ledger": {"backend": "postgres"},
            }
        )
    except ValueError as err:
        assert "event_ledger.dsn" in str(err)
    else:  # pragma: no cover - protects the production configuration invariant
        raise AssertionError("Postgres without a DSN must be rejected")


def test_control_plane_rejects_non_postgres_backend():
    with pytest.raises(ValueError, match="Input should be 'postgres'"):
        EngramConfig.model_validate(
            {
                "api": {"api_key": "test"},
                "event_ledger": {
                    "backend": "memory",
                    "dsn": "postgresql://test:test@localhost/test",
                },
            }
        )


def test_temporal_task_queue_names_must_be_distinct():
    with pytest.raises(ValueError, match="pairwise distinct"):
        EngramConfig.model_validate(
            {
                "api": {"api_key": "test"},
                "event_ledger": {"dsn": "postgresql://test:test@localhost/test"},
                "temporal": {
                    "ingest_task_queue": "engram-work",
                    "projection_task_queue": "engram-work",
                },
            }
        )
