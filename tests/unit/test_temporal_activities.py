from __future__ import annotations

from types import SimpleNamespace

import pytest
from temporalio.exceptions import ApplicationError

from engram.config import EngramConfig
from engram.temporal import activities
from tests.postgres_support import PostgresTestStore


def test_consolidation_activity_reclaims_processing_task_on_retry(
    tmp_path,
    monkeypatch,
):
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "test"},
            "event_ledger": {"dsn": "postgresql://test:test/test"},
        }
    )
    store = PostgresTestStore()
    task_id = store.enqueue_task(
        node_id="mem://user/entities/alice",
        task_type="REGENERATE_MANIFEST",
    )
    assert task_id is not None
    state = SimpleNamespace(
        cfg=cfg,
        control_plane=store,
        fs=object(),
        neo4j=object(),
        core=object(),
        embed=object(),
        overview_cache=None,
    )
    monkeypatch.setattr(activities, "get_state", lambda: state)

    def fail_once(_ctx, _task):
        raise RuntimeError("transient failure")

    monkeypatch.setattr(activities, "_dispatch", fail_once)
    with pytest.raises(RuntimeError, match="transient failure"):
        activities.process_consolidation_activity(task_id)

    failed_attempt = (
        store.get_conn()
        .execute(
            "SELECT status, retry_count, error_message FROM consolidation_tasks WHERE task_id = ?",
            (task_id,),
        )
        .fetchone()
    )
    assert failed_attempt["status"] == "PROCESSING"
    assert failed_attempt["error_message"] == "transient failure"

    monkeypatch.setattr(activities, "_dispatch", lambda _ctx, _task: None)
    assert activities.process_consolidation_activity(task_id) == "COMPLETE"
    completed = (
        store.get_conn()
        .execute(
            "SELECT status, retry_count FROM consolidation_tasks WHERE task_id = ?",
            (task_id,),
        )
        .fetchone()
    )
    assert completed["status"] == "COMPLETE"
    assert completed["retry_count"] == 1


def test_consolidation_activity_rejects_unknown_task_without_retry(
    tmp_path,
    monkeypatch,
):
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "test"},
            "event_ledger": {"dsn": "postgresql://test:test/test"},
        }
    )
    store = PostgresTestStore()
    task_id = store.enqueue_task(
        node_id="mem://user/entities/alice",
        task_type="NOT_A_REAL_TASK",
    )
    assert task_id is not None
    state = SimpleNamespace(
        cfg=cfg,
        control_plane=store,
        fs=object(),
        neo4j=object(),
        core=object(),
        embed=object(),
        overview_cache=None,
    )
    monkeypatch.setattr(activities, "get_state", lambda: state)

    with pytest.raises(ApplicationError) as exc_info:
        activities.process_consolidation_activity(task_id)

    assert exc_info.value.non_retryable
    row = (
        store.get_conn()
        .execute(
            "SELECT status, error_message FROM consolidation_tasks WHERE task_id = ?",
            (task_id,),
        )
        .fetchone()
    )
    assert row["status"] == "PROCESSING"
    assert row["error_message"] == "unknown consolidation task type: NOT_A_REAL_TASK"
