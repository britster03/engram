"""Trustworthy LoCoMo QA baseline runner for Engram.

The runner builds the expected-work manifest before any API calls, preserves
LoCoMo provenance and rolling prior-turn context during ingest, waits on exact
event IDs, stores redacted retrieval traces, and makes deterministic answer F1
and evidence Recall@k the primary metrics.  The LLM judge is opt-in and
diagnostic only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import subprocess
import sys
import time
from collections import defaultdict
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any, TextIO

from prometheus_client.parser import text_string_to_metric_families

# Allow ``python benchmarks/run_locomo.py`` from the repository root while
# retaining one canonical module namespace for type checking and tests.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.engram_client import (
    DrainConfig,
    DrainTimeoutError,
    EngramClient,
    EngramError,
    IngestFailedError,
)
from benchmarks.judge import OllamaJudge
from benchmarks.loader import Conversation, Turn, load_locomo
from benchmarks.metrics import (
    MM_RELEVANCE_EVALUATOR_VERSION,
    SUMMARY_EVALUATOR_VERSION,
    evidence_recall_at_ks,
    qa_score,
)

_LOCOMO_LICENSE = "CC BY-NC 4.0"
_LOCOMO_UPSTREAM = "https://github.com/snap-research/locomo"
_MIN_AUTO_DRAIN_TIMEOUT_S = 600.0
_AUTO_DRAIN_SECONDS_PER_PAIR = 30.0
_RETRIEVAL_MODES = ("adaptive", "forced", "no_memory", "vector_only")


def _effective_drain_timeout(requested_s: float | None, event_count: int) -> float:
    """Return an explicit deadline or a corpus-sized conservative default.

    A full LoCoMo conversation contains hundreds of pairs and hosted model
    providers intentionally serialize calls.  The old fixed ten-minute
    default could therefore fail a healthy run after only a fraction of its
    events had completed.  Explicit caller deadlines remain authoritative.
    """
    if requested_s is not None:
        return requested_s
    return max(
        _MIN_AUTO_DRAIN_TIMEOUT_S,
        event_count * _AUTO_DRAIN_SECONDS_PER_PAIR,
    )


def _session_pairs(conv: Conversation):
    """Yield pairs within source sessions; never cross a session boundary."""
    by_session: dict[int, list[Turn]] = defaultdict(list)
    for turn in conv.turns:
        by_session[turn.session_idx].append(turn)
    for session_idx in sorted(by_session):
        turns = by_session[session_idx]
        for offset in range(0, len(turns), 2):
            yield (
                f"s{session_idx}",
                f"session_{session_idx}",
                turns[offset],
                turns[offset + 1] if offset + 1 < len(turns) else None,
            )


def _dated(turn: Turn) -> str:
    """Include the source session date so relative dates can be normalized."""
    if turn.timestamp:
        return f"[{turn.timestamp}] {turn.attributed()}"
    return turn.attributed()


def _rolling_context(prior_turns: list[Turn], *, max_turns: int) -> str | None:
    selected = prior_turns[-max_turns:] if max_turns else []
    if not selected:
        return None
    # The API cap is 64k characters. Preserve the most recent context on overflow.
    rendered = "\n".join(_dated(turn) for turn in selected)
    return rendered[-64_000:]


def ingest_conversation(
    client: EngramClient,
    conv: Conversation,
    *,
    limit_pairs: int = 0,
    context_turns: int = 12,
) -> list[str]:
    """Ingest a conversation and return every exact event ID submitted."""
    event_ids: list[str] = []
    prior_turns: list[Turn] = []
    for pair_idx, (session_id, source_session_id, user, assistant) in enumerate(
        _session_pairs(conv)
    ):
        if limit_pairs and pair_idx >= limit_pairs:
            break
        assistant_content = _dated(assistant) if assistant is not None else ""
        assistant_timestamp = assistant.timestamp if assistant is not None else user.timestamp
        response = client.ingest_pair(
            session_id=session_id,
            user_content=_dated(user),
            assistant_content=assistant_content,
            user_timestamp=user.timestamp,
            assistant_timestamp=assistant_timestamp,
            user_turn_idx=2 * pair_idx,
            assistant_turn_idx=2 * pair_idx + 1,
            user_external_id=user.dia_id,
            assistant_external_id=assistant.dia_id if assistant is not None else None,
            user_speaker=user.speaker,
            assistant_speaker=assistant.speaker if assistant is not None else None,
            source_conversation_id=conv.sample_id,
            source_session_id=source_session_id,
            user_image_caption=user.blip_caption,
            assistant_image_caption=assistant.blip_caption if assistant is not None else None,
            user_image_urls=user.image_urls,
            assistant_image_urls=assistant.image_urls if assistant is not None else None,
            user_image_query=user.image_query,
            assistant_image_query=assistant.image_query if assistant is not None else None,
            session_context=_rolling_context(prior_turns, max_turns=context_turns),
            source="locomo",
            force_store=True,
        )
        event_ids.append(str(response["event_id"]))
        prior_turns.append(user)
        if assistant is not None:
            prior_turns.append(assistant)
    return event_ids


def _ingested_turn_ids(conv: Conversation, *, limit_pairs: int) -> set[str]:
    """Return the exact source turns present in a partial-corpus run."""
    included: set[str] = set()
    for pair_idx, (_session_id, _source_session_id, user, assistant) in enumerate(
        _session_pairs(conv)
    ):
        if limit_pairs and pair_idx >= limit_pairs:
            break
        included.add(user.dia_id)
        if assistant is not None:
            included.add(assistant.dia_id)
    return included


def _expected_pair_count(conv: Conversation, *, limit_pairs: int) -> int:
    total = sum(1 for _ in _session_pairs(conv))
    return min(total, limit_pairs) if limit_pairs else total


def _selected_questions(
    conv: Conversation,
    limit: int,
    *,
    seed: int = 42,
    limit_pairs: int = 0,
) -> list[tuple[int, Any]]:
    indexed = list(enumerate(conv.qa))
    if limit_pairs:
        available = _ingested_turn_ids(conv, limit_pairs=limit_pairs)
        # A partial-corpus score is valid only when every annotated evidence
        # turn was actually ingested.  This applies to adversarial probes too:
        # those ask Engram to reject a speaker-swapped claim from a real turn.
        indexed = [
            item
            for item in indexed
            if item[1].evidence and set(item[1].evidence).issubset(available)
        ]
    if not limit or limit >= len(indexed):
        return indexed
    by_category: dict[int, list[tuple[int, Any]]] = defaultdict(list)
    for item in indexed:
        by_category[int(item[1].category)].append(item)
    for category, items in by_category.items():
        stable_seed = int.from_bytes(
            hashlib.sha256(
                f"{seed}:{conv.sample_id}:{category}".encode()
            ).digest()[:8],
            "big",
        )
        random.Random(stable_seed).shuffle(items)
    selected: list[tuple[int, Any]] = []
    categories = sorted(by_category)
    while len(selected) < limit:
        made_progress = False
        for category in categories:
            if by_category[category] and len(selected) < limit:
                selected.append(by_category[category].pop())
                made_progress = True
        if not made_progress:
            break
    return selected


def _expected_manifest(conversations: list[Conversation], args: argparse.Namespace) -> dict:
    questions = [
        {
            "result_id": f"{conv.sample_id}:q{question_idx}",
            "sample_id": conv.sample_id,
            "question_index": question_idx,
            "category": probe.category,
        }
        for conv in conversations
        for question_idx, probe in _selected_questions(
            conv,
            args.limit_questions,
            seed=args.seed,
            limit_pairs=args.limit_pairs,
        )
    ]
    return {
        "conversations": [conv.sample_id for conv in conversations],
        "questions": questions,
        "expected_question_count": len(questions),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_metadata() -> dict[str, Any]:
    def run(*command: str) -> str:
        try:
            return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            return "unknown"

    status = run("git", "status", "--porcelain")
    return {
        "commit": run("git", "rev-parse", "HEAD"),
        "branch": run("git", "branch", "--show-current"),
        "dirty": bool(status and status != "unknown"),
    }


def _write_json_atomic(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _write_jsonl_atomic(path: Path, rows: list[dict[str, Any]]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _append_partial(handle: TextIO, row: dict[str, Any]) -> None:
    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    handle.flush()
    os.fsync(handle.fileno())


def _retrieved_turn_ids(trace: dict[str, Any] | None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for hit in (trace or {}).get("hits", []):
        for value in hit.get("source_turn_ids") or []:
            turn_id = str(value)
            if turn_id and turn_id not in seen:
                seen.add(turn_id)
                ordered.append(turn_id)
    return ordered


_BENCHMARK_METRIC_NAMES = {
    "engram_core_model_calls_total",
    "engram_core_model_tokens_total",
    "engram_frontier_calls_total",
    "engram_frontier_tokens_total",
    "engram_overview_model_calls_total",
    "engram_ingest_pipeline_stage_seconds_count",
    "engram_ingest_pipeline_stage_seconds_sum",
}


def _metrics_snapshot(text: str) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    """Parse the bounded process metrics needed for benchmark deltas."""
    snapshot: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name not in _BENCHMARK_METRIC_NAMES:
                continue
            labels = tuple(sorted((str(key), str(value)) for key, value in sample.labels.items()))
            snapshot[(sample.name, labels)] = float(sample.value)
    return snapshot


def _metrics_delta(
    before: dict[tuple[str, tuple[tuple[str, str], ...]], float],
    after: dict[tuple[str, tuple[tuple[str, str], ...]], float],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(set(before) | set(after)):
        delta = after.get(key, 0.0) - before.get(key, 0.0)
        if delta <= 0:
            continue
        name, labels = key
        rows.append({"name": name, "labels": dict(labels), "delta": delta})
    return rows


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _event_latency_seconds(event: dict[str, Any]) -> float | None:
    created = event.get("created_at")
    processed = event.get("processed_at")
    if not created or not processed:
        return None
    try:
        start = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
        finish = datetime.fromisoformat(str(processed).replace("Z", "+00:00"))
    except ValueError:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if finish.tzinfo is None:
        finish = finish.replace(tzinfo=timezone.utc)
    return max(0.0, (finish - start).total_seconds())


def _failure_rows(
    *,
    conv_idx: int,
    conv: Conversation,
    tenant_id: str,
    args: argparse.Namespace,
    status: str,
    error: str,
) -> list[dict[str, Any]]:
    return [
        {
            "result_id": f"{conv.sample_id}:q{question_idx}",
            "conv_idx": conv_idx,
            "sample_id": conv.sample_id,
            "tenant_id": tenant_id,
            "q_idx": question_idx,
            "question": probe.question,
            "category": probe.category,
            "category_name": probe.category_name,
            "is_adversarial": probe.is_adversarial,
            "gold": probe.answer,
            "evidence": probe.evidence,
            "predicted": "",
            "status": status,
            "error": error[:2_000],
            "answer_f1": 0.0,
            "evidence_recall_at_5": 0.0 if probe.evidence else 1.0,
            "evidence_recall_at_10": 0.0 if probe.evidence else 1.0,
            "evidence_recall_at_25": 0.0 if probe.evidence else 1.0,
            "judge_correct": None,
            "judged": False,
        }
        for question_idx, probe in _selected_questions(
            conv,
            args.limit_questions,
            seed=args.seed,
            limit_pairs=args.limit_pairs,
        )
    ]


def _make_manifest(
    *,
    args: argparse.Namespace,
    run_id: str,
    data_path: Path,
    expected: dict[str, Any],
) -> dict[str, Any]:
    prompt_files = sorted((Path(__file__).parents[1] / "engram" / "prompts").glob("*"))
    prompt_hashes = {
        path.name: _sha256(path) for path in prompt_files if path.is_file()
    }
    return {
        "schema_version": 1,
        "run_id": run_id,
        "status": "RUNNING",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "engram": _git_metadata(),
        "dataset": {
            "path": str(data_path.resolve()),
            "sha256": _sha256(data_path),
            "upstream": _LOCOMO_UPSTREAM,
            "upstream_commit": args.dataset_commit,
            "license": _LOCOMO_LICENSE,
            "noncommercial_research_only": True,
        },
        "runner": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "argv": sys.argv,
            "base_url": args.base_url
            or os.environ.get("ENGRAM_BASE_URL", "http://127.0.0.1:8000"),
            "max_depth": args.max_depth,
            "min_depth": args.min_depth,
            "max_reentries": args.max_reentries,
            "retrieval_mode": args.retrieval_mode,
            "drain_timeout_s": args.drain_timeout,
            "drain_timeout_policy": (
                "explicit" if args.drain_timeout is not None
                else "auto=max(600,pairs*30)"
            ),
            "retrieval_forced": args.retrieval_mode == "forced",
            "ingest_force_store": True,
            "corpus_run_id": args.corpus_run_id or run_id,
            "corpus_reused": bool(args.corpus_run_id),
            "seed": args.seed,
            "runner_sha256": _sha256(Path(__file__)),
            "prompt_hashes": prompt_hashes,
            "context_policy": {
                "name": "prior_turn_tail",
                "version": 1,
                "max_turns": args.context_turns,
                "max_chars": 64_000,
                "future_turns": False,
            },
            "judge": {
                "enabled": args.judge,
                "provider": "ollama_cloud" if args.judge else None,
                "model": args.judge_model if args.judge else None,
            },
            "evaluators": {
                "qa_answer": {
                    "name": "locomo_normalized_partial_match_f1",
                    "upstream_commit": args.dataset_commit,
                    "deterministic": True,
                },
                "retrieval": {
                    "name": "evidence_recall_at_5_10_25",
                    "deterministic": True,
                },
                "summary": {
                    "version": SUMMARY_EVALUATOR_VERSION,
                    "metrics": ["rouge_1", "rouge_2", "rouge_l", "adapted_fact_score"],
                    "adapted": True,
                },
                "multimodal": {
                    "version": MM_RELEVANCE_EVALUATOR_VERSION,
                    "metrics": ["bleu_1", "bleu_2", "rouge_l", "mm_relevance"],
                    "caption_conditioned": True,
                    "adapted": True,
                },
            },
        },
        "expected_work": expected,
        "operational": {
            "metrics_scope": "process-global delta; run on an exclusive benchmark stack",
            "conversations": [],
        },
    }


def run(args: argparse.Namespace) -> int:
    base_url = args.base_url or os.environ.get("ENGRAM_BASE_URL", "http://127.0.0.1:8000")
    admin_key = args.admin_key or os.environ.get("ENGRAM_ADMIN_KEY")
    if not admin_key:
        print("error: set ENGRAM_ADMIN_KEY (needed to create tenants)", file=sys.stderr)
        return 2
    if args.judge and not os.environ.get("OLLAMA_API_KEY"):
        print("error: --judge requires OLLAMA_API_KEY", file=sys.stderr)
        return 2

    data_path = Path(args.data)
    conversations = load_locomo(data_path)
    if args.limit_convs:
        conversations = conversations[: args.limit_convs]
    expected = _expected_manifest(conversations, args)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out_dir = Path(args.out) / f"locomo-{run_id}"
    out_dir.mkdir(parents=True, exist_ok=args.resume)
    rows_path = out_dir / "rows.jsonl"
    partial_path = out_dir / "rows.partial.jsonl"
    summary_path = out_dir / "summary.json"
    manifest_path = out_dir / "manifest.json"
    manifest = _make_manifest(args=args, run_id=run_id, data_path=data_path, expected=expected)
    _write_json_atomic(manifest_path, manifest)

    rows: list[dict[str, Any]] = []
    completed_ids: set[str] = set()
    if args.resume and partial_path.exists():
        for line in partial_path.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            result_id = str(row["result_id"])
            if result_id in completed_ids:
                raise RuntimeError(f"duplicate partial result ID: {result_id}")
            completed_ids.add(result_id)
            rows.append(row)

    print(f"run {run_id}: {len(conversations)} conversation(s) -> {out_dir}")
    judge_context = (
        OllamaJudge(model=args.judge_model) if args.judge else nullcontext(None)
    )
    runtime_config_recorded = False
    with judge_context as judge, partial_path.open("a", encoding="utf-8") as partial:
        for conv_idx, conv in enumerate(conversations):
            corpus_run_id = args.corpus_run_id or run_id
            tenant_id = f"{args.tenant_prefix}-{corpus_run_id}-c{conv_idx}"
            expected_for_conv = {
                f"{conv.sample_id}:q{question_idx}"
                for question_idx, _ in _selected_questions(
                    conv,
                    args.limit_questions,
                    seed=args.seed,
                    limit_pairs=args.limit_pairs,
                )
            }
            if expected_for_conv and expected_for_conv.issubset(completed_ids):
                continue
            print(f"\n[conv {conv_idx}] {conv.sample_id} tenant={tenant_id}")
            try:
                if args.corpus_run_id:
                    client = EngramClient.bind_existing_tenant(
                        base_url=base_url,
                        admin_key=admin_key,
                        tenant_id=tenant_id,
                        query_timeout_s=args.query_timeout,
                    )
                else:
                    client = EngramClient.create_tenant(
                        base_url=base_url,
                        admin_key=admin_key,
                        tenant_id=tenant_id,
                        display_name=conv.sample_id,
                        query_timeout_s=args.query_timeout,
                    )
            except EngramError as error:
                failed = _failure_rows(
                    conv_idx=conv_idx,
                    conv=conv,
                    tenant_id=tenant_id,
                    args=args,
                    status="TENANT_SETUP_FAILED",
                    error=str(error),
                )
                for row in failed:
                    if row["result_id"] not in completed_ids:
                        rows.append(row)
                        completed_ids.add(row["result_id"])
                        _append_partial(partial, row)
                continue

            with client:
                try:
                    if not runtime_config_recorded:
                        runtime_config = client.configuration()
                        canonical = json.dumps(
                            runtime_config, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8")
                        manifest["runtime_config"] = runtime_config
                        manifest["runtime_config_sha256"] = hashlib.sha256(
                            canonical
                        ).hexdigest()
                        manifest["runner"]["effective_l0_skip"] = bool(
                            (runtime_config.get("retrieval") or {}).get("l0_skip")
                        )
                        manifest["runner"]["query_force_retrieval"] = (
                            args.retrieval_mode == "forced"
                        )
                        _write_json_atomic(manifest_path, manifest)
                        runtime_config_recorded = True
                    if args.corpus_run_id:
                        expected_pairs = _expected_pair_count(
                            conv,
                            limit_pairs=args.limit_pairs,
                        )
                        event_set = client.list_events(
                            source="locomo",
                            limit=min(500, expected_pairs + 1),
                        )
                        total_events = int(event_set.get("total_count") or 0)
                        event_ids = [str(value) for value in event_set.get("event_ids") or []]
                        if total_events != expected_pairs or len(event_ids) != expected_pairs:
                            raise IngestFailedError(
                                f"corpus {args.corpus_run_id} has {total_events} locomo events; "
                                f"expected exactly {expected_pairs}"
                            )
                        readiness = client.wait_for_events(
                            event_ids,
                            DrainConfig(
                                max_wait_s=_effective_drain_timeout(
                                    args.drain_timeout,
                                    len(event_ids),
                                )
                            ),
                        )
                        manifest["operational"]["conversations"].append({
                            "sample_id": conv.sample_id,
                            "tenant_id": tenant_id,
                            "event_count": len(event_ids),
                            "corpus_reused": True,
                            "memory_ready": bool(readiness.get("memory_ready")),
                            "terminal_status_counts": {
                                status: sum(
                                    event.get("status") == status
                                    for event in readiness["events"]
                                )
                                for status in sorted({
                                    str(event.get("status"))
                                    for event in readiness["events"]
                                })
                            },
                        })
                        _write_json_atomic(manifest_path, manifest)
                        print(
                            f"  verified and reused {len(event_ids)} memory-ready events "
                            f"from {args.corpus_run_id}"
                        )
                    else:
                        metrics_before = _metrics_snapshot(client.metrics_text())
                        started = time.monotonic()
                        event_ids = ingest_conversation(
                            client,
                            conv,
                            limit_pairs=args.limit_pairs,
                            context_turns=args.context_turns,
                        )
                        drain_cfg = DrainConfig(
                            max_wait_s=_effective_drain_timeout(
                                args.drain_timeout,
                                len(event_ids),
                            )
                        )
                        readiness = client.wait_for_events(event_ids, drain_cfg)
                        memory_ready_s = time.monotonic() - started
                        overview = client.wait_for_overview_ready(drain_cfg)
                        total_ready_s = time.monotonic() - started
                        metrics_after = _metrics_snapshot(client.metrics_text())
                        event_latencies = [
                            latency
                            for event in readiness["events"]
                            if (latency := _event_latency_seconds(event)) is not None
                        ]
                        completed_tasks = int(
                            (overview.get("by_status") or {}).get("COMPLETE", 0)
                        )
                        manifest["operational"]["conversations"].append({
                            "sample_id": conv.sample_id,
                            "tenant_id": tenant_id,
                            "event_count": len(event_ids),
                            "artifact_count": sum(
                                int(event.get("artifact_count") or 0)
                                for event in readiness["events"]
                            ),
                            "memory_ready_s": memory_ready_s,
                            "overview_ready_s": total_ready_s,
                            "event_ingest_latency_s": {
                                "p50": _percentile(event_latencies, 0.50),
                                "p95": _percentile(event_latencies, 0.95),
                                "max": max(event_latencies) if event_latencies else None,
                            },
                            "terminal_status_counts": {
                                status: sum(
                                    event.get("status") == status
                                    for event in readiness["events"]
                                )
                                for status in sorted({
                                    str(event.get("status"))
                                    for event in readiness["events"]
                                })
                            },
                            "consolidation": overview,
                            "task_amplification_per_pair": (
                                completed_tasks / len(event_ids) if event_ids else 0.0
                            ),
                            "metrics_delta": _metrics_delta(metrics_before, metrics_after),
                        })
                        _write_json_atomic(manifest_path, manifest)
                        print(
                            f"  {len(event_ids)} pairs memory-ready in "
                            f"{memory_ready_s:.1f}s, overviews ready in {total_ready_s:.1f}s "
                            f"(gated_skip={sum(e['status'] == 'GATED_SKIP' for e in readiness['events'])})"
                        )
                except (EngramError, IngestFailedError, DrainTimeoutError, ValueError) as error:
                    failed = _failure_rows(
                        conv_idx=conv_idx,
                        conv=conv,
                        tenant_id=tenant_id,
                        args=args,
                        status="INGEST_FAILED",
                        error=str(error),
                    )
                    for row in failed:
                        if row["result_id"] not in completed_ids:
                            rows.append(row)
                            completed_ids.add(row["result_id"])
                            _append_partial(partial, row)
                    continue

                for question_idx, probe in _selected_questions(
                    conv,
                    args.limit_questions,
                    seed=args.seed,
                    limit_pairs=args.limit_pairs,
                ):
                    result_id = f"{conv.sample_id}:q{question_idx}"
                    if result_id in completed_ids:
                        continue
                    query_started = time.monotonic()
                    try:
                        response = client.query(
                            probe.question,
                            max_depth=args.max_depth,
                            min_depth=args.min_depth,
                            max_reentries=args.max_reentries,
                            include_trace=True,
                            force_retrieval=False,
                            retrieval_mode=args.retrieval_mode,
                        )
                        answer = str(response.get("answer", ""))
                        metadata = response.get("retrieval_metadata") or {}
                        trace = response.get("retrieval_trace") or {}
                        retrieved_ids = _retrieved_turn_ids(trace)
                        recalls = evidence_recall_at_ks(retrieved_ids, probe.evidence)
                        answer_f1 = qa_score(answer, probe.answer, category=probe.category)
                        status = "COMPLETE"
                        error_text = None
                    except EngramError as error:
                        answer, metadata, trace, retrieved_ids = "", {}, {}, []
                        recalls = evidence_recall_at_ks([], probe.evidence)
                        answer_f1 = 0.0
                        status = "QUERY_FAILED"
                        error_text = str(error)
                    except Exception as error:
                        answer, metadata, trace, retrieved_ids = "", {}, {}, []
                        recalls = evidence_recall_at_ks([], probe.evidence)
                        answer_f1 = 0.0
                        status = "EVALUATOR_FAILED"
                        error_text = str(error)

                    judge_result = None
                    if judge is not None and status == "COMPLETE":
                        judge_result = judge.judge(
                            question=probe.question,
                            gold_answer=probe.answer,
                            predicted_answer=answer,
                            is_adversarial=probe.is_adversarial,
                        )
                    row = {
                        "result_id": result_id,
                        "conv_idx": conv_idx,
                        "sample_id": conv.sample_id,
                        "tenant_id": tenant_id,
                        "q_idx": question_idx,
                        "question": probe.question,
                        "category": probe.category,
                        "category_name": probe.category_name,
                        "is_adversarial": probe.is_adversarial,
                        "gold": probe.answer,
                        "evidence": probe.evidence,
                        "predicted": answer,
                        "status": status,
                        "error": error_text,
                        "answer_f1": answer_f1,
                        "evidence_recall_at_5": recalls["recall_at_5"],
                        "evidence_recall_at_10": recalls["recall_at_10"],
                        "evidence_recall_at_25": recalls["recall_at_25"],
                        "retrieved_turn_ids": retrieved_ids,
                        "trace_id": response.get("trace_id") if status == "COMPLETE" else None,
                        "retrieval_trace": trace,
                        "retrieval_metadata": metadata,
                        "query_latency_s": time.monotonic() - query_started,
                        "judge_correct": judge_result.correct if judge_result else None,
                        "judged": judge_result.judged if judge_result else False,
                        "judge_reason": judge_result.reason if judge_result else None,
                        "judge_raw": (
                            judge_result.raw if judge_result and not judge_result.judged else ""
                        ),
                    }
                    rows.append(row)
                    completed_ids.add(result_id)
                    _append_partial(partial, row)
                    if question_idx % 10 == 0 or answer_f1 < 1.0:
                        print(
                            f"    [F1 {answer_f1:.2f}] q{question_idx} "
                            f"{probe.category_name:>11} :: {probe.question[:60]}"
                        )

    expected_ids = {item["result_id"] for item in expected["questions"]}
    actual_ids = [str(row["result_id"]) for row in rows]
    duplicate_ids = sorted({value for value in actual_ids if actual_ids.count(value) > 1})
    missing_ids = sorted(expected_ids - set(actual_ids))
    unexpected_ids = sorted(set(actual_ids) - expected_ids)
    failed_rows = [row for row in rows if row.get("status") != "COMPLETE"]
    complete = not (duplicate_ids or missing_ids or unexpected_ids or failed_rows)
    summary = summarize(rows)
    summary["operational"] = manifest["operational"]
    summary.update(
        {
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "complete": complete,
            "headline_metrics_valid": complete,
            "expected_count": len(expected_ids),
            "actual_count": len(actual_ids),
            "missing_result_ids": missing_ids,
            "unexpected_result_ids": unexpected_ids,
            "duplicate_result_ids": duplicate_ids,
            "failed_result_count": len(failed_rows),
        }
    )
    manifest["status"] = "COMPLETE" if complete else "INCOMPLETE"
    manifest["completed_at"] = summary["generated_at"]
    manifest["completeness"] = {
        key: summary[key]
        for key in (
            "complete",
            "expected_count",
            "actual_count",
            "missing_result_ids",
            "unexpected_result_ids",
            "duplicate_result_ids",
            "failed_result_count",
        )
    }
    _write_jsonl_atomic(rows_path, rows)
    _write_json_atomic(summary_path, summary)
    _write_json_atomic(manifest_path, manifest)
    print_summary(summary, rows_path, summary_path, manifest_path)
    return 0 if complete else 1


def _mean(rows: list[dict[str, Any]], key: str) -> float:
    return fmean(float(row.get(key, 0.0)) for row in rows) if rows else 0.0


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_category: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_category[str(row["category_name"])].append(row)

    def metrics(group: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "n": len(group),
            "answer_f1": _mean(group, "answer_f1"),
            "evidence_recall_at_5": _mean(group, "evidence_recall_at_5"),
            "evidence_recall_at_10": _mean(group, "evidence_recall_at_10"),
            "evidence_recall_at_25": _mean(group, "evidence_recall_at_25"),
        }

    judged = [row for row in rows if row.get("judged")]
    completed = [row for row in rows if row.get("status") == "COMPLETE"]
    query_latencies = [float(row["query_latency_s"]) for row in completed]
    depth_counts: dict[str, int] = defaultdict(int)
    model_usage: dict[str, dict[str, Any]] = {}
    candidate_counts: list[float] = []
    level_recalls: dict[str, list[float]] = defaultdict(list)
    for row in completed:
        metadata = row.get("retrieval_metadata") or {}
        depth_counts[str(metadata.get("cascade_depth_reached") or "unknown")] += 1
        trace = row.get("retrieval_trace") or {}
        hits = trace.get("hits") or []
        candidate_counts.append(float(len(hits)))
        evidence = row.get("evidence") or []
        ids_by_level: dict[str, list[str]] = defaultdict(list)
        for hit in hits:
            level = str(hit.get("retrieval_level") or "unknown").split("_", 1)[0]
            ids_by_level[level].extend(str(value) for value in hit.get("source_turn_ids") or [])
        for level, turn_ids in ids_by_level.items():
            recall = evidence_recall_at_ks(turn_ids, evidence)["recall_at_25"]
            level_recalls[level].append(recall)
        for call in trace.get("model_calls") or []:
            key = ":".join(
                str(call.get(field) or "unknown")
                for field in ("family", "task", "provider", "model")
            )
            usage = model_usage.setdefault(
                key,
                {
                    "family": call.get("family"),
                    "task": call.get("task"),
                    "provider": call.get("provider"),
                    "model": call.get("model"),
                    "calls": 0,
                    "tokens_in": 0,
                    "tokens_out": 0,
                    "calls_without_token_counts": 0,
                },
            )
            usage["calls"] += int(call.get("provider_calls") or 1)
            if call.get("tokens_in") is None or call.get("tokens_out") is None:
                usage["calls_without_token_counts"] += int(call.get("provider_calls") or 1)
            usage["tokens_in"] += int(call.get("tokens_in") or 0)
            usage["tokens_out"] += int(call.get("tokens_out") or 0)
    return {
        "overall": metrics(rows),
        "by_category": {key: metrics(value) for key, value in sorted(by_category.items())},
        "adversarial_abstention_f1": _mean(by_category.get("adversarial", []), "answer_f1"),
        "judge": {
            "n": len(judged),
            "accuracy": (
                sum(bool(row.get("judge_correct")) for row in judged) / len(judged)
                if judged
                else None
            ),
            "failures": sum(
                row.get("judge_correct") is not None and not row.get("judged") for row in rows
            ),
        },
        "query_operations": {
            "latency_s": {
                "p50": _percentile(query_latencies, 0.50),
                "p95": _percentile(query_latencies, 0.95),
                "max": max(query_latencies) if query_latencies else None,
            },
            "reentry_rate": (
                sum(int((row.get("retrieval_metadata") or {}).get("reentries") or 0) > 0
                    for row in completed) / len(completed)
                if completed
                else 0.0
            ),
            "cascade_depth_distribution": dict(sorted(depth_counts.items())),
            "retrieval_candidates": {
                "mean": fmean(candidate_counts) if candidate_counts else 0.0,
                "p50": _percentile(candidate_counts, 0.50),
                "p95": _percentile(candidate_counts, 0.95),
            },
            "evidence_recall_at_25_by_hit_level": {
                level: fmean(values) for level, values in sorted(level_recalls.items())
            },
            "model_usage": [model_usage[key] for key in sorted(model_usage)],
        },
    }


def print_summary(
    summary: dict[str, Any],
    rows_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> None:
    overall = summary["overall"]
    verdict = "COMPLETE" if summary["complete"] else "INCOMPLETE"
    print("\n" + "=" * 68)
    print(
        f"{verdict}  answer F1={overall['answer_f1']:.3f}  "
        f"R@5={overall['evidence_recall_at_5']:.3f}  "
        f"R@10={overall['evidence_recall_at_10']:.3f}  "
        f"R@25={overall['evidence_recall_at_25']:.3f}"
    )
    print("=" * 68)
    print(f"rows:     {rows_path}")
    print(f"summary:  {summary_path}")
    print(f"manifest: {manifest_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a fail-closed LoCoMo QA baseline.")
    parser.add_argument("--data", default="benchmarks/data/locomo10.json")
    parser.add_argument("--dataset-commit", default="unrecorded")
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--admin-key", default=None)
    parser.add_argument("--limit-convs", type=int, default=0, help="0 = all")
    parser.add_argument("--limit-questions", type=int, default=0, help="0 = all per conv")
    parser.add_argument("--limit-pairs", type=int, default=0, help="0 = all")
    parser.add_argument("--context-turns", type=int, default=12)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--max-depth", choices=("L0", "L1", "L2", "L3", "L4"), default="L4"
    )
    parser.add_argument(
        "--min-depth",
        choices=("L1", "L2", "L3", "L4"),
        default=None,
        help="forced-mode lower bound on planned cascade visitation",
    )
    parser.add_argument("--max-reentries", type=int, default=1)
    parser.add_argument(
        "--retrieval-mode",
        choices=_RETRIEVAL_MODES,
        default="forced",
        help=(
            "adaptive uses normal L0 routing; forced always enters the cascade; "
            "no_memory uses only the frontier; vector_only runs one raw-query vector search"
        ),
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=None,
        help="seconds per conversation; default auto-scales as max(600, pairs*30)",
    )
    parser.add_argument("--query-timeout", type=float, default=300.0)
    parser.add_argument("--tenant-prefix", default="locomo")
    parser.add_argument(
        "--corpus-run-id",
        default=None,
        help="reuse an existing versioned corpus tenant and skip ingestion",
    )
    parser.add_argument("--out", default="benchmarks/results")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--judge", action="store_true", help="enable secondary LLM judge")
    parser.add_argument("--judge-model", default="gemma4:31b")
    args = parser.parse_args()
    if args.context_turns < 0:
        parser.error("--context-turns must be non-negative")
    if args.retrieval_mode == "forced" and args.min_depth is None:
        parser.error("--retrieval-mode forced requires an explicit --min-depth")
    if args.min_depth is not None:
        if args.retrieval_mode != "forced":
            parser.error("--min-depth requires --retrieval-mode forced")
        order = {"L0": 0, "L1": 1, "L2": 2, "L3": 3, "L4": 4}
        if order[args.min_depth] > order[args.max_depth]:
            parser.error("--min-depth cannot exceed --max-depth")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
