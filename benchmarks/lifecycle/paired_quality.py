"""Run four policy variants on two verified ingestion histories.

Ingestion is measured in the two source lifecycle runs, once per history. Each
variant restores byte-identical checkpoints within its retrieval pair. This is
an offline experiment runner, not a production rollout approval.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import shutil
from pathlib import Path

import yaml

from . import DATASET_SHA256
from .dataset import evaluations, load, select
from .infrastructure import compose
from .ledger import atomic, digest

VARIANTS = {
    "baseline": ("legacy", "legacy"),
    "retrieval_only": ("legacy", "enforce"),
    "ingestion_only": ("enforce", "legacy"),
    "combined": ("enforce", "enforce"),
}


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def variant_config(configuration, variant):
    ingestion, retrieval = VARIANTS[variant]
    cfg = copy.deepcopy(configuration)
    cfg.setdefault("canonical_memory", {}).update(
        evidence_mode=ingestion, recover_transient_failures=ingestion == "enforce"
    )
    cfg.setdefault("consolidation", {})["quality_mode"] = ingestion
    cfg.setdefault("retrieval", {})["quality_mode"] = retrieval
    cfg["_benchmark_config_frozen"] = True
    return cfg


def selected_checkpoints(profile):
    selected = select(load(), profile)
    return sorted(
        {
            str(
                Path("checkpoints")
                / c.id
                / (
                    "completed.json.gz"
                    if e.variant in {"completed", "annotation_limited"}
                    else f"session-{e.cutoff:03d}.json.gz"
                )
            )
            for c in load()
            for e in evaluations(c, selected)
        }
    )


def inspect_source(source, profile, ingestion_mode):
    source = Path(source).resolve()
    manifest = json.loads((source / "run_manifest.json").read_text())
    summary = json.loads((source / "summary.json").read_text())
    if manifest["profile"] != profile:
        raise ValueError("Source and replay profiles must match")
    if summary["status"] not in {"COMPLETE", "COMPLETE_WITH_ERRORS"}:
        raise ValueError("Source lifecycle run must have completed coverage")
    if summary.get("executed") != manifest["expected_evaluations"] or summary.get("remaining") != 0:
        raise ValueError("Source lifecycle denominator is incomplete")
    accounting = json.loads((source / "accounting.json").read_text())
    if accounting["status"] != "RECONCILED":
        raise ValueError("Source lifecycle accounting is not reconciled")
    for section in ("canonical_memory", "consolidation"):
        if manifest.get("quality_modes", {}).get(section) != ingestion_mode:
            raise ValueError(f"Source {section} policy does not match variant")
    snapshots = {}
    consolidation = {}
    for relative in selected_checkpoints(profile):
        path = source / relative
        proof = json.loads(path.with_suffix(".manifest.json").read_text())
        readiness = proof.get("readiness", {})
        if not proof.get("verified") or not readiness.get("ready"):
            raise ValueError("Source checkpoint was not verified ready")
        if readiness.get("canonical_projection_lag") or readiness.get("projection_mismatches"):
            raise ValueError("Source checkpoint has projection lag")
        if file_hash(path) != proof["file_sha256"]:
            raise ValueError("Source checkpoint checksum mismatch")
        snapshots[relative] = {
            "sha256": proof["file_sha256"],
            "fingerprints": proof["fingerprints"],
        }
        counts = proof.get("counts", {})
        consolidation[relative] = {
            "overviews": counts.get("memory_overviews"),
            "tasks": counts.get("consolidation_tasks"),
        }
    return {
        "source_run": str(source),
        "source_manifest_sha256": file_hash(source / "run_manifest.json"),
        "provenance_sha256": file_hash(source / "provenance.jsonl"),
        "snapshots": snapshots,
        "consolidation_observation": {
            "checkpoints": consolidation,
            "status": "UNAVAILABLE"
            if any(v["overviews"] is None for v in consolidation.values())
            else "OVERVIEWS_PRESENT_REQUIRES_SEPARATE_QUALITY_AUDIT"
            if any(v["overviews"] for v in consolidation.values())
            else "NO_OVERVIEWS_OBSERVED",
            "interpretation": "Policy enabled does not establish that native consolidation ran; zero overviews cannot measure consolidation quality.",
        },
        "ingestion_accounting": str(source / "accounting.json"),
        "ingestion_consumption": "REUSED_FROM_SOURCE_NOT_REBILLED_OR_DUPLICATED",
        "scope": "Frozen retrieval replay; ingestion measurements remain in source run",
    }


def materialize(source, destination, profile, ingestion_mode):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination or source in destination.parents:
        raise ValueError("Replay output must be outside its immutable source run")
    proof = inspect_source(source, profile, ingestion_mode)
    destination.mkdir(parents=True, exist_ok=True)
    identity_path = destination / "paired_source.json"
    if identity_path.exists() and json.loads(identity_path.read_text()) != proof:
        raise ValueError("Cannot resume replay with a different source identity")
    paths = ["provenance.jsonl"]
    for relative in proof["snapshots"]:
        paths.extend([relative, str(Path(relative).with_suffix(".manifest.json"))])
    for relative in paths:
        target, original = destination / relative, source / relative
        if target.exists():
            if file_hash(target) != file_hash(original):
                raise ValueError("Existing replay input differs from source")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
        if file_hash(target) != file_hash(original):
            raise ValueError("Replay input changed during copy")
    atomic(identity_path, proof)
    return proof


def materialize_verified_ingestion(source, destination, profile, configuration, *,
                                  allow_sufficiency_policy_change=False):
    """Replay verified ingestion after interrupted evaluation.

    Pass the source's frozen runtime YAML to compare its effective configuration.
    The opt-in exception covers only the read-only sufficiency controller;
    ingestion, compaction, models, schemas, and all other engine files stay pinned.
    """
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination or source in destination.parents:
        raise ValueError("Replay output must be outside its immutable source run")
    manifest = json.loads((source / "run_manifest.json").read_text())
    if manifest["profile"] != profile or manifest["dataset_sha256"] != DATASET_SHA256:
        raise ValueError("Source profile or dataset differs from replay")
    if manifest["config_hash"] != digest(yaml.safe_load(Path(configuration).read_text())):
        raise ValueError("Source and replay configuration differ")
    query_policy_changes = {}
    for relative, expected in manifest["source_hashes"].items():
        if relative.startswith(("engram/", "templates/")):
            current = Path(__file__).resolve().parents[2] / relative
            actual = file_hash(current) if current.is_file() else None
            if actual != expected:
                if (allow_sufficiency_policy_change and actual is not None
                        and relative == "engram/retrieval/sufficiency.py"):
                    query_policy_changes[relative] = {"source_sha256": expected,
                                                      "candidate_sha256": actual}
                    continue
                raise ValueError(f"Native engine source changed: {relative}")
    snapshots = {}
    consolidation = {}
    for relative in selected_checkpoints(profile):
        path = source / relative
        proof = json.loads(path.with_suffix(".manifest.json").read_text())
        readiness = proof.get("readiness", {})
        if not proof.get("verified") or not readiness.get("ready"):
            raise ValueError(f"Source checkpoint not verified ready: {relative}")
        if readiness.get("canonical_projection_lag") or readiness.get("projection_mismatches"):
            raise ValueError(f"Source checkpoint has projection lag: {relative}")
        if file_hash(path) != proof["file_sha256"]:
            raise ValueError(f"Source checkpoint checksum mismatch: {relative}")
        snapshots[relative] = {"sha256": proof["file_sha256"], "fingerprints": proof["fingerprints"]}
        counts = proof.get("counts", {})
        consolidation[relative] = {
            "overviews": counts.get("memory_overviews"),
            "tasks": counts.get("consolidation_tasks"),
        }
    observation = {
        "checkpoints": consolidation,
        "status": "UNAVAILABLE" if any(v["overviews"] is None for v in consolidation.values())
        else "OVERVIEWS_PRESENT_REQUIRES_SEPARATE_QUALITY_AUDIT"
        if any(v["overviews"] for v in consolidation.values()) else "NO_OVERVIEWS_OBSERVED",
        "interpretation": "Verified ingestion does not establish consolidation quality.",
    }
    proof = {
        "source_run": str(source),
        "source_manifest_sha256": file_hash(source / "run_manifest.json"),
        "provenance_sha256": file_hash(source / "provenance.jsonl"),
        "snapshots": snapshots,
        "consolidation_observation": observation,
        "ingestion_accounting": str(source / "accounting.json"),
        "ingestion_consumption": "REUSED_FROM_SOURCE_NOT_REBILLED_OR_DUPLICATED",
        "scope": "Verified ingestion replay from interrupted evaluation; ingestion measurements remain in source run",
    }
    if allow_sufficiency_policy_change:
        proof["query_policy_source_changes"] = query_policy_changes
        proof["source_guard"] = "ALL_OTHER_ENGINE_FILES_UNCHANGED"
    destination.mkdir(parents=True, exist_ok=True)
    identity_path = destination / "paired_source.json"
    if identity_path.exists() and json.loads(identity_path.read_text()) != proof:
        raise ValueError("Cannot resume replay with a different source identity")
    paths = ["provenance.jsonl"]
    for relative in snapshots:
        paths.extend([relative, str(Path(relative).with_suffix(".manifest.json"))])
    for relative in paths:
        target, original = destination / relative, source / relative
        if target.exists():
            if file_hash(target) != file_hash(original):
                raise ValueError("Existing replay input differs from source")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
        if file_hash(target) != file_hash(original):
            raise ValueError("Replay input changed during copy")
    atomic(identity_path, proof)
    return proof


def run_matrix(baseline_source, candidate_source, configuration, directory, profile, image_tag):
    from engram.config import apply_config_environment, configuration_environment

    from .__main__ import run
    from .compare_quality import compare

    baseline_source, candidate_source = Path(baseline_source), Path(candidate_source)
    source_manifests = [
        json.loads((s / "run_manifest.json").read_text())
        for s in (baseline_source, candidate_source)
    ]
    for key in (
        "dataset_sha256",
        "official_upstream_revision",
        "profile",
        "seed",
        "expected_evaluations",
    ):
        if source_manifests[0].get(key) != source_manifests[1].get(key):
            raise ValueError(f"Incompatible source histories: {key}")
    runtime_models = [
        json.loads((s / "native_runtime.json").read_text())["models"]
        for s in (baseline_source, candidate_source)
    ]
    for role in ("core", "frontier"):
        for field in ("model_path", "temperature", "backend", "provider"):
            if runtime_models[0][role].get(field) != runtime_models[1][role].get(field):
                raise ValueError(f"Source model settings differ: {role}.{field}")
    # Resolve all preconditions before starting potentially billable work.
    inspect_source(baseline_source, profile, "legacy")
    inspect_source(candidate_source, profile, "enforce")
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    cfg = apply_config_environment(yaml.safe_load(Path(configuration).read_text()), configuration_environment(configuration))
    for section, role in (("core_model", "core"), ("frontier_llm", "frontier")):
        for field in ("model_path", "temperature"):
            if cfg.get(section, {}).get(field) != runtime_models[0][role].get(field):
                raise ValueError(
                    f"Replay model configuration differs from source: {section}.{field}"
                )
    os.environ["LOCOMO_IMAGE_TAG"] = image_tag
    results = {}
    for variant, (ingestion, _) in VARIANTS.items():
        target = directory / variant
        source = baseline_source if ingestion == "legacy" else candidate_source
        materialize(source, target, profile, ingestion)
        config_path = directory / f"{variant}.yaml"
        config_text = yaml.safe_dump(variant_config(cfg, variant), sort_keys=False)
        if config_path.exists() and config_path.read_text() != config_text:
            raise ValueError("Cannot resume with changed variant configuration")
        config_path.write_text(config_text)
        config_path.chmod(0o600)
        try:
            results[variant] = run(profile, target, config_path)
        except Exception as error:
            atomic(
                directory / "matrix_status.json",
                {
                    "status": "INCOMPLETE",
                    "results": results,
                    "failed_variant": variant,
                    "error_type": type(error).__name__,
                },
            )
            if (target / ".runtime/compose.env").exists():
                compose(
                    target,
                    "kill",
                    "-s",
                    "SIGKILL",
                    "worker",
                    "dispatcher",
                    check=False,
                    capture=True,
                )
                compose(target, "stop", "driver", check=False, capture=True)
            raise
        atomic(directory / "matrix_status.json", {"status": "RUNNING", "results": results})
        if results[variant]["status"] not in {"COMPLETE", "COMPLETE_WITH_ERRORS"}:
            atomic(directory / "matrix_status.json", {"status": "INCOMPLETE", "results": results})
            return results
        # Retain private data, release phase resources, never select production.
        compose(target, "stop", "postgres", "neo4j", "redis", "temporal", capture=True)
    comparisons = {}
    for old, new in (
        ("baseline", "retrieval_only"),
        ("ingestion_only", "combined"),
        ("baseline", "ingestion_only"),
        ("baseline", "combined"),
    ):
        comparisons[f"{old}-to-{new}"] = compare(
            directory / old,
            directory / new,
            directory / "comparisons" / f"{old}-to-{new}",
            require_same_snapshots=VARIANTS[old][0] == VARIANTS[new][0],
        )
    atomic(
        directory / "matrix_status.json",
        {
            "status": "COMPLETE",
            "results": results,
            "comparisons": comparisons,
            "release_status": "REQUIRES_INDEPENDENT_SUPPORT_AUDIT_AND_PAIRED_REVIEW",
        },
    )
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--candidate-source", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--profile", choices=["pilot", "validation", "full"], default="pilot")
    parser.add_argument("--image-tag", required=True)
    args = parser.parse_args()
    run_matrix(
        args.baseline_source,
        args.candidate_source,
        args.config,
        args.run_dir,
        args.profile,
        args.image_tag,
    )
