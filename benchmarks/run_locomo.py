"""LoCoMo baseline runner — the conductor.

Ties loader + engram_client + judge together into one benchmark pass:

    for each conversation:
        create a fresh isolated tenant            (Issue C: no cross-convo bleed)
        ingest every turn pair
        wait_for_drain()                          (Issue A: no async race)
        for each question:
            query Engram, judge the answer, record the FULL trace

Every question row keeps Engram's retrieval_metadata (l0 decision, cascade
depth, nodes retrieved, latency), not just right/wrong -- that trace is what
powers the later failure analysis without having to re-run the benchmark.

Usage (smoke first, then full):

    # smoke: 1 conversation, 10 questions
    python benchmarks/run_locomo.py --limit-convs 1 --limit-questions 10

    # full baseline
    python benchmarks/run_locomo.py

Env:
    ENGRAM_BASE_URL   default http://127.0.0.1:8000
    ENGRAM_ADMIN_KEY  required (used to create per-conversation tenants)
    OLLAMA_API_KEY    required (judge model)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Allow running as `python benchmarks/run_locomo.py` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from engram_client import DrainConfig, DrainTimeout, EngramClient, EngramError  # noqa: E402
from judge import OllamaJudge  # noqa: E402
from loader import Conversation, load_locomo  # noqa: E402


def _session_pairs(conv: Conversation):
    """Yield (session_id, user_turn, assistant_turn_or_None) grouped per session.

    Pairs are formed WITHIN a session so the session_id and timestamp on each
    pair stay consistent and turns never pair across a session boundary. LoCoMo
    speakers alternate, so consecutive turns are a natural user/assistant pair;
    speaker identity is preserved inside the text via Turn.attributed(), so which
    slot a speaker lands in does not matter. A session with an odd turn count
    leaves a trailing turn paired with None (the caller pads it).
    """
    by_session: dict[int, list] = defaultdict(list)
    for t in conv.turns:
        by_session[t.session_idx].append(t)
    for sidx in sorted(by_session):
        turns = by_session[sidx]
        session_id = f"s{sidx}"
        for i in range(0, len(turns), 2):
            user = turns[i]
            asst = turns[i + 1] if i + 1 < len(turns) else None
            yield session_id, user, asst


def _dated(turn) -> str:
    """Turn text with an absolute date anchor prepended.

    LoCoMo utterances use relative time ("yesterday", "last year") and the real
    date lives only in the session timestamp. Without an absolute date IN the
    memory text, Engram anchors temporal facts to the ingest date (e.g. 2026)
    instead of the conversation date (e.g. 2023), which breaks every temporal
    question. Prefixing the date -- mirroring how LoCoMo's own eval presents
    dated sessions -- gives the extractor something to anchor to.
    """
    if turn.timestamp:
        return f"[{turn.timestamp}] {turn.attributed()}"
    return turn.attributed()


def ingest_conversation(
    client: EngramClient, conv: Conversation, *, limit_pairs: int = 0
) -> int:
    """Ingest every pair of one conversation. Returns the pair count.

    `limit_pairs` (>0) caps ingest for cheap plumbing smoke tests; the baseline
    run leaves it at 0 (all pairs) so memory is complete.
    """
    n = 0
    for idx, (session_id, user, asst) in enumerate(_session_pairs(conv)):
        if limit_pairs and idx >= limit_pairs:
            break
        # Trailing lone turn -> empty assistant content (nothing extra to
        # extract), which keeps the user utterance in the record.
        asst_content = _dated(asst) if asst is not None else ""
        asst_ts = asst.timestamp if asst is not None else user.timestamp
        client.ingest_pair(
            session_id=session_id,
            user_content=_dated(user),
            assistant_content=asst_content,
            user_timestamp=user.timestamp,
            assistant_timestamp=asst_ts,
            user_turn_idx=2 * idx,
            assistant_turn_idx=2 * idx + 1,
            source="locomo",
        )
        n += 1
    return n


def run(args: argparse.Namespace) -> int:
    base_url = args.base_url or os.environ.get("ENGRAM_BASE_URL", "http://127.0.0.1:8000")
    admin_key = args.admin_key or os.environ.get("ENGRAM_ADMIN_KEY")
    if not admin_key:
        print("error: set ENGRAM_ADMIN_KEY (needed to create tenants)", file=sys.stderr)
        return 2
    if not os.environ.get("OLLAMA_API_KEY"):
        print("error: set OLLAMA_API_KEY (needed for the judge)", file=sys.stderr)
        return 2

    conversations = load_locomo(args.data)
    if args.limit_convs:
        conversations = conversations[: args.limit_convs]

    run_id = datetime.now().strftime("%m%d%H%M%S")
    out_dir = Path(args.out) / f"locomo-{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / "rows.jsonl"
    summary_path = out_dir / "summary.json"

    drain_cfg = DrainConfig(max_wait_s=args.drain_timeout)
    print(f"run {run_id}: {len(conversations)} conversation(s) -> {out_dir}")

    rows: list[dict] = []
    with OllamaJudge() as judge, open(rows_path, "w", encoding="utf-8") as rows_fh:
        for cidx, conv in enumerate(conversations):
            tenant_id = f"{args.tenant_prefix}-{run_id}-c{cidx}"
            print(f"\n[conv {cidx}] {conv.sample_id} tenant={tenant_id}")
            try:
                client = EngramClient.create_tenant(
                    base_url=base_url, admin_key=admin_key,
                    tenant_id=tenant_id, display_name=conv.sample_id,
                    query_timeout_s=args.query_timeout,
                )
            except EngramError as err:
                print(f"  ! tenant setup failed, skipping conv: {err}")
                continue

            with client:
                t0 = time.monotonic()
                n_pairs = ingest_conversation(client, conv, limit_pairs=args.limit_pairs)
                print(f"  ingested {n_pairs} pairs in {time.monotonic()-t0:.1f}s; draining...")
                try:
                    drain = client.wait_for_drain(drain_cfg)
                    print(f"  drained in {drain['waited_s']:.1f}s "
                          f"(saw_activity={bool(drain['saw_activity'])})")
                except DrainTimeout as err:
                    print(f"  ! drain timeout, querying anyway: {err}")

                questions = conv.qa
                if args.categories:
                    wanted = {c.strip() for c in args.categories.split(",") if c.strip()}
                    questions = [q for q in questions if q.category_name in wanted]
                if args.limit_questions:
                    questions = questions[: args.limit_questions]
                for qidx, probe in enumerate(questions):
                    try:
                        res = client.query(
                            probe.question,
                            max_depth=args.max_depth,
                            max_reentries=args.max_reentries,
                        )
                        answer = res.get("answer", "")
                        meta = res.get("retrieval_metadata", {}) or {}
                    except EngramError as err:
                        answer, meta = f"(query-error: {err})", {}

                    jr = judge.judge(
                        question=probe.question,
                        gold_answer=probe.answer,
                        predicted_answer=answer,
                        is_adversarial=probe.is_adversarial,
                    )
                    row = {
                        "conv_idx": cidx,
                        "sample_id": conv.sample_id,
                        "tenant_id": tenant_id,
                        "q_idx": qidx,
                        "question": probe.question,
                        "category": probe.category,
                        "category_name": probe.category_name,
                        "is_adversarial": probe.is_adversarial,
                        "gold": probe.answer,
                        "evidence": probe.evidence,
                        "predicted": answer,
                        "correct": jr.correct,
                        "judged": jr.judged,
                        "judge_reason": jr.reason,
                        # raw judge text kept only when it failed, for debugging
                        "judge_raw": jr.raw if not jr.judged else "",
                        # --- retrieval trace (for failure analysis) ---
                        "l0_decision": meta.get("l0_decision"),
                        "l0_reason": meta.get("l0_reason"),
                        "predicted_depth": meta.get("predicted_depth"),
                        "cascade_depth_reached": meta.get("cascade_depth_reached"),
                        "levels_visited": meta.get("levels_visited"),
                        "nodes_retrieved": meta.get("nodes_retrieved"),
                        "reentries": meta.get("reentries"),
                        "total_context_tokens": meta.get("total_context_tokens"),
                        "latency_ms": meta.get("latency_ms"),
                    }
                    rows.append(row)
                    rows_fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                    rows_fh.flush()

                    mark = "OK " if jr.correct else "XX "
                    if qidx % 10 == 0 or not jr.correct:
                        print(f"    [{mark}] q{qidx} {probe.category_name:>11} "
                              f"l0={row['l0_decision']} depth={row['cascade_depth_reached']}"
                              f" :: {probe.question[:60]}")

    summary = summarize(rows)
    summary["run_id"] = run_id
    summary["base_url"] = base_url
    summary["generated_at"] = datetime.now(timezone.utc).isoformat()
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print_summary(summary, rows_path, summary_path)
    return 0


def _acc(rows: list[dict]) -> dict:
    n = len(rows)
    c = sum(r["correct"] for r in rows)
    return {"n": n, "correct": c, "accuracy": (c / n if n else 0.0)}


def summarize(rows: list[dict]) -> dict:
    by_category: dict[str, list] = defaultdict(list)
    by_l0: dict[str, list] = defaultdict(list)
    by_depth: dict[str, list] = defaultdict(list)
    for r in rows:
        by_category[r["category_name"]].append(r)
        by_l0[str(r["l0_decision"])].append(r)
        by_depth[str(r["cascade_depth_reached"])].append(r)

    return {
        "overall": _acc(rows),
        "by_category": {k: _acc(v) for k, v in sorted(by_category.items())},
        "by_l0_decision": {k: _acc(v) for k, v in sorted(by_l0.items())},
        "by_cascade_depth": {k: _acc(v) for k, v in sorted(by_depth.items())},
        "judge_failures": sum(1 for r in rows if not r["judged"]),
    }


def print_summary(summary: dict, rows_path: Path, summary_path: Path) -> None:
    o = summary["overall"]
    print("\n" + "=" * 60)
    print(f"BASELINE  accuracy = {o['accuracy']:.1%}  ({o['correct']}/{o['n']})")
    print("=" * 60)

    def _block(title: str, d: dict) -> None:
        print(f"\n{title}")
        for k, v in d.items():
            print(f"  {k:>14}: {v['accuracy']:.1%}  ({v['correct']}/{v['n']})")

    _block("by question category", summary["by_category"])
    _block("by L0 decision  (BYPASS vs CONTINUE)", summary["by_l0_decision"])
    _block("by cascade depth reached", summary["by_cascade_depth"])
    if summary["judge_failures"]:
        print(f"\n  ! {summary['judge_failures']} judge failure(s) — flagged for re-judging")
    print(f"\nrows:    {rows_path}")
    print(f"summary: {summary_path}")


def main() -> int:
    p = argparse.ArgumentParser(description="Run the LoCoMo baseline against Engram.")
    p.add_argument("--data", default="benchmarks/data/locomo10.json")
    p.add_argument("--base-url", default=None)
    p.add_argument("--admin-key", default=None)
    p.add_argument("--limit-convs", type=int, default=0, help="0 = all")
    p.add_argument("--limit-questions", type=int, default=0, help="0 = all per conv")
    p.add_argument("--categories", default="",
                   help="comma-separated category filter, e.g. 'temporal' "
                        "(names: multi_hop,temporal,open_domain,single_hop,adversarial)")
    p.add_argument("--limit-pairs", type=int, default=0,
                   help="0 = all; cap ingested pairs for a cheap plumbing smoke test")
    p.add_argument("--max-depth", default=None, help="cap cascade depth, e.g. L2")
    p.add_argument("--max-reentries", type=int, default=None)
    p.add_argument("--drain-timeout", type=float, default=240.0,
                   help="max wait for consolidation to settle; core memory is "
                        "ready well before this, so we proceed on timeout")
    p.add_argument("--query-timeout", type=float, default=300.0,
                   help="per-query HTTP timeout (s); queries drive several LLM calls")
    p.add_argument("--tenant-prefix", default="locomo")
    p.add_argument("--out", default="benchmarks/results")
    return run(p.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
