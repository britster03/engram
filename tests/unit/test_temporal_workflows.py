from __future__ import annotations

import inspect

from engram.config import EngramConfig
from engram.temporal.dispatcher import build_maintenance_schedules
from engram.temporal.workflows import (
    ConsolidationWorkflow,
    DecayWorkflow,
    IngestWorkflow,
    ReconciliationWorkflow,
    StaleOverviewWorkflow,
)


def test_workflow_entrypoints_accept_only_opaque_identifiers():
    assert list(inspect.signature(IngestWorkflow.run).parameters) == ["self", "event_id"]
    assert list(inspect.signature(ConsolidationWorkflow.run).parameters) == ["self", "task_id"]
    assert list(inspect.signature(ReconciliationWorkflow.run).parameters) == ["self"]
    assert list(inspect.signature(StaleOverviewWorkflow.run).parameters) == ["self"]
    assert list(inspect.signature(DecayWorkflow.run).parameters) == ["self"]


def test_maintenance_schedules_use_the_configured_queue_and_cadence():
    cfg = EngramConfig.model_validate(
        {
            "api": {"api_key": "test"},
            "event_ledger": {
                "backend": "postgres",
                "dsn": "postgresql://unused",
                "reconciliation_interval_seconds": 90,
                "stale_overview_scan_interval_seconds": 3600,
            },
            "temporal": {
                "enabled": True,
                "maintenance_task_queue": "custom-maintenance",
            },
            "decay": {
                "schedule": "15 4 * * *",
                "schedule_timezone": "Asia/Kolkata",
            },
        }
    )

    schedules = build_maintenance_schedules(cfg)

    assert set(schedules) == {
        "engram-reconciliation",
        "engram-stale-overview-scan",
        "engram-decay",
    }
    assert all(
        schedule.action.task_queue == "custom-maintenance" for schedule in schedules.values()
    )
    assert schedules["engram-reconciliation"].spec.intervals[0].every.total_seconds() == 90
    assert schedules["engram-stale-overview-scan"].spec.intervals[0].every.total_seconds() == 3600
    assert schedules["engram-decay"].spec.cron_expressions == ["15 4 * * *"]
    assert schedules["engram-decay"].spec.time_zone_name == "Asia/Kolkata"
