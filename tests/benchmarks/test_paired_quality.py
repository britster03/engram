import json

import pytest

from benchmarks.lifecycle import paired_quality as paired
from benchmarks.lifecycle.ledger import atomic


@pytest.mark.parametrize(
    "name,ingestion,retrieval",
    [
        ("baseline", "legacy", "legacy"),
        ("retrieval_only", "legacy", "enforce"),
        ("ingestion_only", "enforce", "legacy"),
        ("combined", "enforce", "enforce"),
    ],
)
def test_four_variants_preserve_models_and_search_settings(name, ingestion, retrieval):
    cfg = {
        "core_model": {"model_path": "native", "temperature": 1},
        "retrieval": {"max_l1_vector_results": 20},
        "consolidation": {"max_tokens": 8000},
    }
    result = paired.variant_config(cfg, name)
    assert result["core_model"] == cfg["core_model"]
    assert result["retrieval"]["quality_mode"] == retrieval
    assert result["canonical_memory"]["evidence_mode"] == ingestion
    assert result["consolidation"]["quality_mode"] == ingestion
    assert result["retrieval"]["max_l1_vector_results"] == 20
    assert "quality_mode" not in cfg["retrieval"]


def source_run(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    rel = "checkpoints/conv/completed.json.gz"
    monkeypatch.setattr(paired, "selected_checkpoints", lambda profile: [rel])
    atomic(
        source / "run_manifest.json",
        {
            "profile": "pilot",
            "expected_evaluations": 1,
            "quality_modes": {"canonical_memory": "enforce", "consolidation": "enforce"},
        },
    )
    atomic(source / "summary.json", {"status": "COMPLETE", "executed": 1, "remaining": 0})
    atomic(source / "accounting.json", {"status": "RECONCILED"})
    (source / "provenance.jsonl").write_text('{"event_id":"original"}\n')
    path = source / rel
    path.parent.mkdir(parents=True)
    path.write_bytes(b"immutable checkpoint bytes")
    proof_path = path.with_suffix(".manifest.json")
    atomic(
        proof_path,
        {
            "verified": True,
            "readiness": {"ready": True},
            "file_sha256": paired.file_hash(path),
            "fingerprints": {"postgres": "same", "neo4j": "same"},
        },
    )
    return source, path, proof_path


def test_retrieval_pairs_receive_identical_copied_checkpoints_without_usage_duplication(
    tmp_path, monkeypatch
):
    source, path, _ = source_run(tmp_path, monkeypatch)
    a, b = tmp_path / "a", tmp_path / "b"
    first = paired.materialize(source, a, "pilot", "enforce")
    second = paired.materialize(source, b, "pilot", "enforce")
    assert first == second
    assert (a / "checkpoints/conv/completed.json.gz").read_bytes() == path.read_bytes()
    assert not list(a.glob("provider_attempts*"))
    assert paired.materialize(source, a, "pilot", "enforce") == first
    (a / "provenance.jsonl").write_text("changed")
    with pytest.raises(ValueError, match="differs"):
        paired.materialize(source, a, "pilot", "enforce")


@pytest.mark.parametrize(
    "count,status",
    [
        (None, "UNAVAILABLE"),
        (0, "NO_OVERVIEWS_OBSERVED"),
        (3, "OVERVIEWS_PRESENT_REQUIRES_SEPARATE_QUALITY_AUDIT"),
    ],
)
def test_consolidation_policy_is_not_misreported_as_observed_execution(
    tmp_path, monkeypatch, count, status
):
    source, _, proof_path = source_run(tmp_path, monkeypatch)
    proof = json.loads(proof_path.read_text())
    proof["counts"] = {"memory_overviews": count, "consolidation_tasks": count}
    atomic(proof_path, proof)
    result = paired.inspect_source(source, "pilot", "enforce")
    assert result["consolidation_observation"]["status"] == status


@pytest.mark.parametrize(
    "failure",
    [
        "checksum",
        "projection_lag",
        "unverified",
        "not_ready",
        "incomplete",
        "accounting",
        "wrong_policy",
    ],
)
def test_incomplete_or_untrustworthy_sources_cannot_start_replay(tmp_path, monkeypatch, failure):
    source, path, proof_path = source_run(tmp_path, monkeypatch)
    proof = json.loads(proof_path.read_text())
    if failure == "checksum":
        path.write_bytes(b"tampered")
    elif failure == "projection_lag":
        proof["readiness"]["canonical_projection_lag"] = ["memory"]
    elif failure == "unverified":
        proof["verified"] = False
    elif failure == "not_ready":
        proof["readiness"]["ready"] = False
    elif failure == "incomplete":
        atomic(source / "summary.json", {"status": "INCOMPLETE"})
    elif failure == "accounting":
        atomic(source / "accounting.json", {"status": "MISMATCH"})
    elif failure == "wrong_policy":
        atomic(
            source / "run_manifest.json",
            {
                "profile": "pilot",
                "expected_evaluations": 1,
                "quality_modes": {"canonical_memory": "legacy"},
            },
        )
    atomic(proof_path, proof)
    with pytest.raises(ValueError):
        paired.materialize(source, tmp_path / "replay", "pilot", "enforce")
    assert not (tmp_path / "replay").exists()


def test_source_cannot_be_its_own_replay_destination(tmp_path, monkeypatch):
    source, _, _ = source_run(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="outside"):
        paired.materialize(source, source, "pilot", "enforce")


def test_source_query_failures_do_not_discard_verified_ingestion(tmp_path, monkeypatch):
    source, _, _ = source_run(tmp_path, monkeypatch)
    atomic(
        source / "summary.json",
        {"status": "COMPLETE_WITH_ERRORS", "executed": 1, "remaining": 0, "question_errors": 1},
    )
    proof = paired.materialize(source, tmp_path / "replay", "pilot", "enforce")
    assert proof["source_run"] == str(source)


def test_verified_ingestion_can_replay_after_interrupted_evaluation(tmp_path, monkeypatch):
    source, path, _ = source_run(tmp_path, monkeypatch)
    (source / "summary.json").unlink()
    config = tmp_path / "config.yaml"
    config.write_text("retrieval:\n  quality_mode: legacy\n")
    manifest = json.loads((source / "run_manifest.json").read_text())
    manifest.update(
        dataset_sha256=paired.DATASET_SHA256,
        config_hash=paired.digest({"retrieval": {"quality_mode": "legacy"}}),
        source_hashes={},
    )
    atomic(source / "run_manifest.json", manifest)
    target = tmp_path / "replay"
    proof = paired.materialize_verified_ingestion(source, target, "pilot", config)
    assert proof["snapshots"]["checkpoints/conv/completed.json.gz"]["sha256"] == paired.file_hash(path)
    assert (target / "checkpoints/conv/completed.json.gz").read_bytes() == path.read_bytes()
    assert "interrupted evaluation" in proof["scope"]
    assert paired.materialize_verified_ingestion(source, target, "pilot", config) == proof
    path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        paired.materialize_verified_ingestion(source, tmp_path / "other", "pilot", config)


@pytest.mark.parametrize("relative,allowed", [
    ("engram/retrieval/sufficiency.py", True),
    ("engram/session/manager.py", False),
    ("engram/models/core.py", False),
])
def test_interrupted_replay_only_allows_explicit_read_only_policy_change(
    tmp_path, monkeypatch, relative, allowed
):
    source, _, _ = source_run(tmp_path, monkeypatch)
    monkeypatch.setattr(paired, "__file__", str(tmp_path / "benchmarks/lifecycle/paired_quality.py"))
    candidate_file = tmp_path / relative
    candidate_file.parent.mkdir(parents=True)
    candidate_file.write_text("current candidate source")
    config = tmp_path / "frozen.yaml"
    configuration = {"_benchmark_config_frozen": True,
                     "session": {"compaction_trigger_tokens": 64000},
                     "retrieval": {"quality_mode": "enforce"}}
    atomic(config, configuration)
    manifest = json.loads((source / "run_manifest.json").read_text())
    manifest.update(dataset_sha256=paired.DATASET_SHA256,
                    config_hash=paired.digest(configuration),
                    source_hashes={relative: "old-source-digest"})
    atomic(source / "run_manifest.json", manifest)
    with pytest.raises(ValueError, match="Native engine source changed"):
        paired.materialize_verified_ingestion(source, tmp_path / "strict", "pilot", config)
    if not allowed:
        with pytest.raises(ValueError, match="Native engine source changed"):
            paired.materialize_verified_ingestion(source, tmp_path / "opt-in", "pilot", config,
                                                allow_sufficiency_policy_change=True)
        return
    proof = paired.materialize_verified_ingestion(source, tmp_path / "opt-in", "pilot", config,
                                                 allow_sufficiency_policy_change=True)
    assert proof["source_guard"] == "ALL_OTHER_ENGINE_FILES_UNCHANGED"
    assert proof["query_policy_source_changes"][relative]["source_sha256"] == "old-source-digest"


def test_frozen_replay_report_links_original_ingestion_without_duplicate_consumption(tmp_path):
    from benchmarks.lifecycle import report

    atomic(
        tmp_path / "run_manifest.json",
        {"run_id": "replay", "profile": "pilot", "git_sha": "test", "config_hash": "test"},
    )
    atomic(
        tmp_path / "paired_source.json",
        {
            "scope": "Frozen replay",
            "source_run": "/source",
            "ingestion_accounting": "/source/accounting.json",
        },
    )
    summary = report.generate(tmp_path, provisional=True)
    assert summary["status"] == "PROVISIONAL"
    assert summary["ingestion_source_run"] == "/source"
    assert "not repeated" in (tmp_path / "summary.md").read_text()
    assert "not repeated" in (tmp_path / "summary.html").read_text()
    account = json.loads((tmp_path / "accounting.json").read_text())
    assert "ingestion" not in account["components"]


def test_matrix_executes_four_variants_and_requires_snapshot_pairing(tmp_path, monkeypatch):
    import yaml

    from benchmarks.lifecycle import __main__ as host
    from benchmarks.lifecycle import compare_quality

    source, _, _ = source_run(tmp_path, monkeypatch)
    baseline = tmp_path / "baseline-source"
    import shutil

    shutil.copytree(source, baseline)
    manifest = json.loads((baseline / "run_manifest.json").read_text())
    manifest["quality_modes"] = {"canonical_memory": "legacy", "consolidation": "legacy"}
    atomic(baseline / "run_manifest.json", manifest)
    models = {
        "core": {"model_path": "native", "temperature": 1},
        "frontier": {"model_path": "native", "temperature": 1},
    }
    for directory in (source, baseline):
        atomic(directory / "native_runtime.json", {"models": models})
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        yaml.safe_dump({"core_model": models["core"], "frontier_llm": models["frontier"]})
    )
    invoked = []

    def run(profile, directory, config):
        invoked.append((directory.name, yaml.safe_load(config.read_text())))
        assert (directory / "checkpoints/conv/completed.json.gz").exists()
        return {"status": "COMPLETE"}

    monkeypatch.setattr(host, "run", run)
    monkeypatch.setattr(paired, "compose", lambda *a, **k: None)
    comparisons = []

    def compare(a, b, output, **kwargs):
        comparisons.append((a.name, b.name, kwargs["require_same_snapshots"]))
        return {"pairs": 1}

    monkeypatch.setattr(compare_quality, "compare", compare)
    monkeypatch.setenv("LOCOMO_IMAGE_TAG", "initial-test")
    output = tmp_path / "matrix"
    paired.run_matrix(baseline, source, cfg, output, "pilot", "test")
    assert [name for name, _ in invoked] == list(paired.VARIANTS)
    assert comparisons == [
        ("baseline", "retrieval_only", True),
        ("ingestion_only", "combined", True),
        ("baseline", "ingestion_only", False),
        ("baseline", "combined", False),
    ]
    result = json.loads((output / "matrix_status.json").read_text())
    assert result["status"] == "COMPLETE"
    assert result["release_status"].startswith("REQUIRES_INDEPENDENT")
