"""Regression coverage for full denominators and recovered workflow readiness."""

import hashlib
import os
from types import SimpleNamespace

import pytest

from benchmarks.lifecycle import activities
from benchmarks.lifecycle import compare_quality as comparison
from benchmarks.lifecycle.ledger import atomic, read
from benchmarks.lifecycle.report import csv_write


def make_run(directory):
    directory.mkdir()
    atomic(
        directory / "run_manifest.json", {"expected_evaluations": 2, "profile": "pilot", "seed": 42}
    )
    common = dict(
        conversation_id="conv", question="Question", prediction="Answer", status="COMPLETE"
    )
    csv_write(
        directory / "per_question.csv",
        [
            {
                **common,
                "evaluation_id": "native",
                "benchmark_track": "CANONICAL",
                "official_locomo_f1": 0.5,
            }
        ],
    )
    csv_write(
        directory / "search_comparison.csv",
        [
            {
                **common,
                "evaluation_id": "search",
                "benchmark_track": "SEARCH_COMPARISON",
                "prediction": "",
                "official_locomo_f1": None,
            }
        ],
    )
    return directory


def test_compare_counts_both_native_and_search_evaluations(tmp_path):
    a, b = make_run(tmp_path / "a"), make_run(tmp_path / "b")
    result = comparison.compare(a, b, tmp_path / "out")
    assert result["pairs"] == 2
    assert result["candidate_errors"] == 0


def test_candidate_manifest_denominator_is_checked(tmp_path):
    a, b = make_run(tmp_path / "a"), make_run(tmp_path / "b")
    atomic(b / "run_manifest.json", {"expected_evaluations": 3, "profile": "pilot", "seed": 42})
    with pytest.raises(ValueError, match="denominators"):
        comparison.compare(a, b, tmp_path / "out")


def test_retrieval_pair_requires_matching_checkpoint_identity(tmp_path):
    a, b = make_run(tmp_path / "a"), make_run(tmp_path / "b")
    with pytest.raises(ValueError, match="identical frozen"):
        comparison.compare(a, b, tmp_path / "out", require_same_snapshots=True)
    for directory in (a, b):
        (directory / "one").write_bytes(b"same")
        atomic(
            directory / "paired_source.json",
            {"snapshots": {"one": {"sha256": hashlib.sha256(b"same").hexdigest()}}},
        )
    result = comparison.compare(a, b, tmp_path / "out", require_same_snapshots=True)
    assert result["snapshot_equivalence"] == "IDENTICAL_VERIFIED_CHECKPOINTS"
    (b / "one").write_bytes(b"different")
    atomic(
        b / "paired_source.json",
        {"snapshots": {"one": {"sha256": hashlib.sha256(b"different").hexdigest()}}},
    )
    with pytest.raises(ValueError, match="identical frozen"):
        comparison.compare(a, b, tmp_path / "out", require_same_snapshots=True)


@pytest.mark.parametrize("change,match", [("NOT_EXECUTED", "executed"), ("duplicate", "Duplicate")])
def test_unexecuted_and_duplicate_search_rows_are_rejected(tmp_path, change, match):
    import csv

    a, b = make_run(tmp_path / "a"), make_run(tmp_path / "b")
    with (b / "search_comparison.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    if change == "duplicate":
        rows[0]["evaluation_id"] = "native"
    else:
        rows[0]["status"] = change
    csv_write(b / "search_comparison.csv", rows)
    with pytest.raises(ValueError, match=match):
        comparison.compare(a, b, tmp_path / "out")


def dispatch(identity, generation, kind="INGEST", tenant="one"):
    return dict(
        workflow_id=identity,
        generation=generation,
        aggregate_id="event",
        workflow_type=kind,
        tenant_id=tenant,
    )


def test_recovered_ingestion_supersedes_only_its_own_prior_generation():
    rows = [
        dispatch("old", 1),
        dispatch("recovery", 2),
        dispatch("other-tenant", 1, tenant="two"),
        dispatch("projection-old", 1, "PROJECTION"),
        dispatch("projection-new", 2, "PROJECTION"),
    ]
    expected = {"recovery", "other-tenant", "projection-old", "projection-new"}
    assert activities.effective_workflow_ids(rows) == expected
    assert activities.effective_workflow_ids(list(reversed(rows))) == expected


@pytest.mark.parametrize(
    "recovery_status,projection_status,ready,fatal",
    [
        ("COMPLETED", "COMPLETED", True, False),
        ("RUNNING", "COMPLETED", False, False),
        ("FAILED", "COMPLETED", False, True),
        ("COMPLETED", "FAILED", False, True),
    ],
)
def test_temporal_readiness_preserves_failed_history_but_uses_recovery_outcome(
    tmp_path, monkeypatch, recovery_status, projection_status, ready, fatal
):
    dispatches = [
        dispatch("old", 1),
        dispatch("recovery", 2),
        dispatch("projection", 1, "PROJECTION"),
    ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, query, args):
            assert args == ("one",)
            assert "generation" in query
            return SimpleNamespace(fetchall=lambda: dispatches)

    monkeypatch.setattr(activities.psycopg, "connect", lambda *a, **k: Connection())
    statuses = {"old": "FAILED", "recovery": recovery_status, "projection": projection_status}

    async def fetch(config, rows):
        return [
            {**r, "workflow_status": statuses[r["workflow_id"]], "activities": []} for r in rows
        ]

    monkeypatch.setattr(activities, "fetch", fetch)
    state = SimpleNamespace(
        cfg=SimpleNamespace(event_ledger=SimpleNamespace(dsn="isolated"), temporal=None)
    )
    result = activities.collect(state, "one", tmp_path)
    assert result["ready"] is ready
    assert result["fatal"] is fatal
    assert result["workflow_statuses"]["FAILED"] >= 1
    assert result["superseded_workflow_ids"] == ["old"]
    assert "old" not in result["unfinished_workflows"]
    # Terminal failure history must also survive outside process memory.
    assert any(r["workflow_id"] == "old" for r in read(tmp_path / "temporal_history_trace.jsonl"))


@pytest.mark.skipif(not os.environ.get("ENGRAM_TEST_DATABASE_URL"), reason="isolated PostgreSQL required")
def test_temporal_history_collect_reads_actual_recovery_generation(tmp_path, monkeypatch):
    from tests.postgres_support import PostgresTestStore

    store = PostgresTestStore()
    try:
        event, _ = store.record_event(pair_id="one", session_id="session", source="test", event_type="INGEST", payload={}, tenant_id="tenant")
        store.set_event_status(event, "FAILED", error_message="APITimeoutError: test", tenant_id="tenant")
        store.mark_aggregate_dispatches_dead("INGEST", event, "failed")
        store.retry_event(event, tenant_id="tenant")
        with store.transaction() as conn:
            conn.execute("UPDATE workflow_dispatches SET workflow_id='test-workflow-' || generation::text WHERE tenant_id=?", ("tenant",))
        async def fetch(config, rows):
            assert {r["generation"] for r in rows} == {1, 2}
            return [{**r, "workflow_status": "FAILED" if r["generation"] == 1 else "COMPLETED", "activities": []} for r in rows]
        monkeypatch.setattr(activities, "fetch", fetch)
        state = SimpleNamespace(cfg=SimpleNamespace(event_ledger=SimpleNamespace(dsn=store.dsn), temporal=None))
        result = activities.collect(state, "tenant", tmp_path)
        assert result["ready"] and not result["fatal"]
        assert result["superseded_workflow_ids"] == ["test-workflow-1"]
        assert result["workflow_statuses"] == {"FAILED": 1, "COMPLETED": 1}
    finally:
        store.close()
