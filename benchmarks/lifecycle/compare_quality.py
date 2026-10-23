"""Compare completed native policy variants without changing benchmark scoring."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from .ledger import atomic
from .metrics import bootstrap_conversations
from .report import csv_write


def _number(value):
    return float(value) if value not in (None, "") else None


def compare(baseline: Path, candidate: Path, output: Path, *, require_same_snapshots=False):
    manifests = [
        json.loads((directory / "run_manifest.json").read_text())
        for directory in (baseline, candidate)
    ]
    for key in ("dataset_sha256", "official_upstream_revision", "profile", "seed"):
        if manifests[0].get(key) != manifests[1].get(key):
            raise ValueError(f"Incompatible comparison: {key}")

    def rows(directory):
        data = []
        for filename in ("per_question.csv", "search_comparison.csv"):
            path = directory / filename
            if path.exists():
                with path.open(newline="") as stream:
                    data.extend(csv.DictReader(stream))
        if any(r.get("status") == "NOT_EXECUTED" for r in data):
            raise ValueError(
                "Comparison requires executed evaluations; retain unfinished blocks separately"
            )
        keyed = {r["evaluation_id"]: r for r in data}
        if len(keyed) != len(data):
            raise ValueError("Duplicate evaluation rows")
        return keyed

    left, right = rows(baseline), rows(candidate)
    if (
        left.keys() != right.keys()
        or len(left) != manifests[0]["expected_evaluations"]
        or len(right) != manifests[1]["expected_evaluations"]
    ):
        raise ValueError("Comparison requires equal complete evaluation denominators")
    snapshots = []
    for directory in (baseline, candidate):
        path = directory / "paired_source.json"
        snapshots.append(json.loads(path.read_text()).get("snapshots", {}) if path.exists() else {})
    for directory, records in zip((baseline, candidate), snapshots, strict=True):
        for relative, proof in records.items():
            path = (directory / relative).resolve()
            if directory.resolve() not in path.parents:
                raise ValueError("Snapshot path is outside the comparison run")
            h = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    h.update(chunk)
            if h.hexdigest() != proof["sha256"]:
                raise ValueError("Frozen comparison snapshot changed")
    same_snapshots = bool(snapshots[0]) and snapshots[0] == snapshots[1]
    if require_same_snapshots and not same_snapshots:
        raise ValueError("Retrieval comparison requires identical frozen snapshots")
    pairs = []
    for key, old in left.items():
        new = right[key]
        pair = {
            "evaluation_id": key,
            "conversation_id": old["conversation_id"],
            "benchmark_track": old["benchmark_track"],
            "question": old["question"],
            "baseline_answer": old["prediction"],
            "candidate_answer": new["prediction"],
            "baseline_status": old["status"],
            "candidate_status": new["status"],
        }
        for field in (
            "unknown_usage_calls",
            "estimated_usage_calls",
            "retry_count",
            "packed_provenance_status",
            "provenance_status",
            "answerability",
            "terminal_stage",
            "terminal_action",
            "stop_reason",
        ):
            pair["baseline_" + field] = old.get(field)
            pair["candidate_" + field] = new.get(field)
        for metric in (
            "official_locomo_f1",
            "packed_fraction_evidence_recalled",
            "total_tokens",
            "known_total_tokens",
            "total_latency_ms",
        ):
            a, b = _number(old.get(metric)), _number(new.get(metric))
            pair.update(
                {
                    "baseline_" + metric: a,
                    "candidate_" + metric: b,
                    metric + "_delta": b - a if a is not None and b is not None else None,
                }
            )
        pairs.append(pair)
    groups = defaultdict(list)
    for pair in pairs:
        groups[pair["benchmark_track"]].append(pair)
    aggregates = []
    for track, values in groups.items():
        for metric in (
            "official_locomo_f1",
            "packed_fraction_evidence_recalled",
            "total_tokens",
            "known_total_tokens",
            "total_latency_ms",
        ):
            valid = [r for r in values if r[metric + "_delta"] is not None]
            low, high = bootstrap_conversations(valid, metric + "_delta")
            aggregates.append(
                {
                    "track": track,
                    "metric": metric,
                    "expected_pairs": len(values),
                    "evaluable_pairs": len(valid),
                    "mean_delta": sum(r[metric + "_delta"] for r in valid) / len(valid)
                    if valid
                    else None,
                    "conversation_ci_low": low,
                    "conversation_ci_high": high,
                }
            )
    output.mkdir(parents=True, exist_ok=True)
    csv_write(output / "paired_questions.csv", pairs)
    csv_write(output / "paired_aggregates.csv", aggregates)
    result = {
        "baseline_run": str(baseline),
        "candidate_run": str(candidate),
        "pairs": len(pairs),
        "baseline_errors": sum(r["baseline_status"] != "COMPLETE" for r in pairs),
        "candidate_errors": sum(r["candidate_status"] != "COMPLETE" for r in pairs),
        "quality_modes": [m.get("quality_modes") for m in manifests],
        "release_status": "REQUIRES_PAIRED_REVIEW_AND_INDEPENDENT_SUPPORT_AUDIT",
        "snapshot_equivalence": "IDENTICAL_VERIFIED_CHECKPOINTS"
        if same_snapshots
        else "DIFFERENT_INGESTION_STATES"
        if all(snapshots)
        else "NOT_ESTABLISHED_BY_ANSWER_COMPARISON",
        "matching_snapshot_count": len(snapshots[0]) if same_snapshots else 0,
    }
    atomic(output / "comparison.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.baseline, args.candidate, args.output)))


if __name__ == "__main__":
    main()
