"""Replay known failures on verified frozen checkpoints; never a full benchmark score."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .adapter import Provenance, readonly_context, session_id, use_tenant
from .dataset import evaluations, load, select
from .evaluation import make_row
from .infrastructure import assert_isolated
from .ledger import Progress, append, append_trace, atomic, read
from .observer import Trace, capture, error_info, install
from .report import csv_write
from .snapshot import fingerprint, restore


def replay(source, output, conversation_id, cutoffs):
    assert_isolated()
    install()
    from engram.deps import get_state
    from engram.retrieval.orchestrator import run_query

    state = get_state()
    source, output = Path(source), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if (output / "result_rows.jsonl").exists():
        raise ValueError(
            "Use a fresh replay directory; existing attempts must not be overwritten or duplicated"
        )
    os.environ["LOCOMO_TRACE_DIR"] = str(output)
    manifest = json.loads((output / "run_manifest.json").read_text())
    conversation = next(c for c in load() if c.id == conversation_id)
    work = [
        e
        for e in evaluations(conversation, select(load(), "pilot"))
        if e.cutoff in cutoffs and e.variant in {"evidence_session", "following_session"}
    ]
    if not work:
        raise ValueError("No matching regression evaluations")
    progress = Progress(output, phase="frozen_checkpoint_replay")
    rows = []
    try:
        for cutoff in sorted(cutoffs):
            path = source / "checkpoints" / conversation_id / f"session-{cutoff:03d}.json.gz"
            proof = json.loads(path.with_suffix(".manifest.json").read_text())
            if not proof.get("verified") or not proof.get("readiness", {}).get("ready"):
                raise ValueError("Replay requires verified completed ingestion")
            data = restore(state, path)
            tid = use_tenant(state, conversation_id)
            allowed = {session_id(conversation_id, n) for n in conversation.sessions if n <= cutoff}
            if any(
                e.get("session_id") not in allowed
                for e in data["postgres"].get("events", [])
                if e.get("session_id")
            ):
                raise ValueError("Future session in replay source")
            provenance = Provenance(data, read(source / "provenance.jsonl"), conversation)
            before = fingerprint(state, tid)
            for evaluation in [e for e in work if e.cutoff == cutoff]:
                trace = Trace(evaluation.key)
                result, error = None, None
                progress.update(
                    completed=len(rows),
                    expected=len(work),
                    current_question=evaluation.question.qa_id,
                )
                with capture(trace):
                    try:
                        with readonly_context(state, tid) as ctx:
                            result = run_query(
                                ctx,
                                session_id=session_id(conversation_id, cutoff),
                                query=evaluation.question.question,
                                on_step=trace.step,
                            )
                    except Exception as failure:
                        error = error_info(failure)
                after = fingerprint(state, tid)
                if after != before:
                    atomic(output / "state_mutation.json", {"before": before, "after": after})
                    raise RuntimeError("Replay changed semantic state")
                trace.emit("semantic_fingerprint", before=before, after=after, verified=True)
                row = make_row(
                    evaluation,
                    trace,
                    provenance,
                    result,
                    run_id=output.name,
                    git_sha=manifest["git_sha"],
                    config_hash=manifest["config_hash"],
                    error=error,
                )
                row["source_snapshot_sha256"] = proof["file_sha256"]
                row.update(append_trace(output, trace))
                append(output / "result_rows.jsonl", row)
                rows.append(row)
                csv_write(output / "per_question.csv", rows)
                progress.update(
                    completed=len(rows),
                    expected=len(work),
                    errors=sum(r["status"] != "COMPLETE" for r in rows),
                    known_tokens=sum(r.get("known_total_tokens", 0) or 0 for r in rows),
                )
        atomic(
            output / "replay_summary.json",
            {
                "status": "COMPLETE",
                "evaluations": len(rows),
                "errors": sum(r["status"] != "COMPLETE" for r in rows),
                "source_run": str(source),
                "scope": "Targeted regression replay; not a pilot/full benchmark or release approval",
                "semantic_state_unchanged": True,
            },
        )
    finally:
        progress.close("COMPLETE" if len(rows) == len(work) else "INCOMPLETE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--conversation", default="conv-26")
    parser.add_argument("--cutoffs", type=int, nargs="+", default=[1, 2])
    args = parser.parse_args()
    replay(args.source, args.output, args.conversation, args.cutoffs)
