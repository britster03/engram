"""Tests for the synthetic training-data generator.

Validates shape, quantity, schema compliance, and that a real SFT-style
pipeline can consume the produced JSONL without custom parsing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from engram.training.synthetic_data import (
    _TASKS,
    generate_all,
    gen_dedup,
    gen_entity_link,
    gen_extract,
    gen_gate_classifier,
    gen_gate_write,
    gen_l1_plan,
    gen_ln_plan,
    gen_overview,
    gen_session_compact,
    gen_unmerge,
    validate_record,
    write_jsonl,
)


def test_all_task_generators_registered():
    expected = {
        "gate_write", "extract", "l1_plan", "ln_plan", "dedup",
        "entity_link", "overview", "session_compact", "unmerge",
        "gate_classifier",
    }
    assert set(_TASKS.keys()) == expected


def test_gate_write_records_validate():
    rows = gen_gate_write(50, seed=0)
    assert len(rows) == 50
    for r in rows:
        validate_record(r)
        assert r["task_type"] == "gate_write"
        assert set(r["output"].keys()) == {"store", "reason"}


def test_extract_emits_at_least_some_triplets():
    rows = gen_extract(100, seed=0)
    with_triplets = sum(1 for r in rows if r["output"]["triplets"])
    assert with_triplets > 50  # most records carry at least one triplet


def test_l1_plan_schema_has_required_keys():
    for r in gen_l1_plan(20, seed=1):
        out = r["output"]
        assert out["predicted_depth"] in {"L1", "L2", "L3", "L4"}
        assert out["mode"] in {"AGFS", "KG", "HYBRID"}
        assert isinstance(out["vector_queries"], list)
        assert isinstance(out["commands"], list)


def test_ln_plan_has_terminate_flag():
    rows = gen_ln_plan(30)
    terminated = sum(1 for r in rows if r["output"]["terminate_cascade"])
    assert 0 < terminated < len(rows)  # mix of terminating and continuing


def test_dedup_cases_are_valid_enum():
    for r in gen_dedup(50):
        assert r["output"]["case"] in {"DUPLICATE", "CONTRADICTION", "CO_EXISTENCE"}


def test_entity_link_matched_id_format():
    for r in gen_entity_link(30):
        m = r["output"]["matched_id"]
        if m is not None:
            assert m.startswith("mem://user/entities/")


def test_overview_markdown_shape():
    for r in gen_overview(10):
        overview = r["output"]["overview"]
        assert isinstance(overview, str)
        assert overview.lstrip().startswith("#")


def test_session_compact_has_key_lists():
    for r in gen_session_compact(5):
        out = r["output"]
        assert isinstance(out["key_facts"], list)
        assert isinstance(out["key_entities"], list)


def test_unmerge_produces_two_splits():
    for r in gen_unmerge(10):
        assert len(r["output"]["splits"]) == 2


def test_gate_classifier_balanced():
    rows = gen_gate_classifier(200)
    labels = [r["label"] for r in rows]
    # Each generator call produces n//2 pairs, so two classes roughly balanced.
    class0 = labels.count(0)
    class1 = labels.count(1)
    assert class0 == class1 == 100


def test_validate_record_rejects_missing_keys():
    with pytest.raises(ValueError):
        validate_record({"task_type": "gate_write"})
    with pytest.raises(ValueError):
        validate_record({"query": "hi"})  # gate_classifier missing label
    with pytest.raises(ValueError):
        validate_record({"query": "hi", "label": 2, "task_type": "gate_classifier"})


def test_write_jsonl_roundtrip(tmp_path: Path):
    path = tmp_path / "x.jsonl"
    rows = gen_gate_write(5, seed=42)
    n = write_jsonl(path, rows)
    assert n == 5
    lines = path.read_text().splitlines()
    assert len(lines) == 5
    for line in lines:
        rec = json.loads(line)
        validate_record(rec)


def test_generate_all_covers_every_task(tmp_path: Path):
    summary = generate_all(
        tmp_path,
        counts={  # small counts so the test runs fast
            t: 5 for t in _TASKS
        },
        seed=7,
    )
    for task in _TASKS:
        path = tmp_path / f"{task}.jsonl"
        assert path.exists(), f"missing {task}.jsonl"
        lines = path.read_text().splitlines()
        assert len(lines) == 5
    assert set(summary.keys()) == set(_TASKS.keys())


def test_core_sft_load_traces_accepts_generated_data(tmp_path: Path):
    """The SFT training script must be able to ingest the synthetic output
    without modification (proves end-to-end compatibility without running
    actual torch training)."""
    generate_all(tmp_path, counts={t: 3 for t in _TASKS}, seed=0)
    from engram.training.core_sft import load_traces
    rows = load_traces(tmp_path)
    # gate_classifier.jsonl is skipped by load_traces; other 9 tasks × 3 each = 27
    assert len(rows) == 27
    for r in rows:
        assert "task_type" in r
        assert "system_prompt" in r
        assert "user_prompt" in r
