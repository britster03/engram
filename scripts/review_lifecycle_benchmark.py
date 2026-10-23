"""Create a read-only, reproducible review of a lifecycle benchmark.

The benchmark container renders existing lossless request/context details. This
stdlib-only companion adds all-track QA comparisons and descriptive diagnostics;
it never changes the scorer, calls a judge model, or writes to the source run.

python scripts/review_lifecycle_benchmark.py RUN --output REPORT --notes NOTES
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import html
import json
import os
import re
import shutil
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_FILES = (
    "result_rows.jsonl", "trace.jsonl", "evaluation_attempts.jsonl",
    "run_manifest.json", "progress.json", "readiness.json", "readiness_trace.jsonl",
    "validation.json", "validation_attempt_ids.json", "validation_workflow_ids.json",
    "measurement_corrections.jsonl", "semantic_audit.jsonl", "secondary_scores.json",
    "data_dictionary.md", "isolation_manifest.json", "stack_verified.json",
)
CSV_FIELDS = (
    "evaluation_id", "qa_id", "track", "conversation", "category", "checkpoint",
    "cutoff_session", "search_method", "status", "question", "reference_answer",
    "engram_answer", "official_f1", "answer_mode", "stop_reason",
    "known_query_tokens", "estimated_query_tokens", "unknown_usage_calls",
    "usage_status", "answer_producing_known_tokens", "final_answer_known_tokens",
    "core_known_tokens", "frontier_known_tokens", "sdk_attempts", "retry_count",
    "latency_seconds", "retrieved_gold_ids", "supplied_gold_ids",
    "retrieval_provenance", "supplied_provenance", "diagnostics",
    "review_status", "analyst_assessment", "recommended_action", "detail_page",
    "detail_json", "review_page",
)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
                                    allow_nan=False) + "\n", encoding="utf-8")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def records(path):
    if Path(path).exists():
        with Path(path).open(encoding="utf-8") as stream:
            for line in stream:
                if line.strip():
                    yield json.loads(line)


def snapshot(source, destination):
    """Copy complete journal prefixes; hard-link only immutable trace archives.

    Rows are copied first: each committed row already has a durable trace. The
    snapshot is an explicitly bounded observation window, not an atomic database
    checkpoint. Calls in progress during that window remain unknown consumption.
    """
    destination.mkdir(parents=True, exist_ok=False)
    metadata = {"source_run": str(source.resolve()), "started_at": utc_now(),
                "files": {}, "capture_mode": "COMPLETE_JOURNAL_PREFIXES"}
    paths = [source / name for name in SNAPSHOT_FILES]
    paths.extend(sorted(source.glob("provider_attempts.*.jsonl")))
    for path in paths:
        if not path.exists():
            continue
        size = path.stat().st_size
        with path.open("rb") as stream:
            content = stream.read(size)
        if path.suffix == ".jsonl" and content and not content.endswith(b"\n"):
            content = content[:content.rfind(b"\n") + 1]
        (destination / path.name).write_bytes(content)
        metadata["files"][path.name] = {"bytes": len(content), "sha256": digest(content)}
    for row in records(destination / "trace.jsonl"):
        relative = row.get("trace_archive")
        if not relative:
            continue
        original = (source / relative).resolve()
        if not original.is_relative_to(source.resolve()):
            raise ValueError("Trace archive must be inside the source run")
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            try:
                os.link(original, target)
            except OSError:
                shutil.copyfile(original, target)
    metadata["completed_at"] = utc_now()
    metadata["committed_evaluations"] = sum(1 for _ in records(destination / "result_rows.jsonl"))
    write_json(destination / "snapshot_manifest.json", metadata)
    return metadata


def normalized_tokens(value):
    # Diagnostic lexical coverage only. This is deliberately not an answer judge.
    return re.findall(r"\w+", str(value or "").casefold())


def inspect_detail(detail, relative, notes):
    row = detail["evaluation"]
    usage = detail["query_usage"]
    executed = row["status"] != "NOT_EXECUTED"
    scored = executed and row["benchmark_track"] != "SEARCH_COMPARISON"
    calls = detail["model_calls"]
    flags, observations = [], []
    gold_ids = set(row.get("gold_evidence_ids") or [])
    retrieved = sorted(gold_ids.intersection(row.get("retrieved_ids") or []))
    supplied = sorted(gold_ids.intersection(row.get("packed_evidence_ids") or []))
    if not executed:
        flags.append("NOT_EXECUTED")
    elif row["status"] != "COMPLETE":
        flags.append("EXECUTION_ERROR")
    abstained = scored and (row.get("native_abstention") is True
                           or row.get("answer_mode") == "evidence-terminal")
    if abstained:
        flags.append("ABSTENTION")
        if retrieved:
            flags.append("GOLD_RETRIEVED_BUT_ABSTAINED")
            observations.append("Some annotated evidence was retrieved. This does not establish that it reached generation or justified an exhaustive answer.")
    if scored and row.get("provenance_status") == "INCOMPLETE":
        flags.append("INCOMPLETE_RETRIEVAL_PROVENANCE")
    if usage["unknown_usage_calls"]:
        flags.append("UNKNOWN_USAGE")
    empty_checks = [c for c in calls if c.get("phase") == "output_support"
                    and c.get("status") == "RETURNED" and not (c.get("response_text") or "").strip()]
    if empty_checks:
        flags.append("EMPTY_OUTPUT_VALIDATION")
        observations.append(f"{len(empty_checks)} output-validation SDK attempts returned empty visible text.")
    if any(c.get("produced_answer") for c in calls) and not any(c.get("produced_final_answer") for c in calls) and scored:
        flags.append("GENERATED_CANDIDATE_NOT_RETURNED")
        observations.append("A Frontier operation emitted an answer candidate, but it is not the returned Engram answer. Its correctness is not established by this flag.")
    reference = detail["reference_answer_display"]
    ref_tokens = set(normalized_tokens(reference))
    answer_tokens = normalized_tokens(row.get("prediction"))
    if scored and row.get("category_id") != 5 and ref_tokens and ref_tokens.issubset(answer_tokens) and len(answer_tokens) > len(normalized_tokens(reference)):
        flags.append("REFERENCE_WORDS_WITH_EXTRA_TEXT")
        observations.append("All reference words occur in a longer response. This is a lexical diagnostic, not a semantic correctness verdict.")
    note = notes.get(row["evaluation_id"])
    if note and (note["prediction_sha256"] != digest(str(row.get("prediction")).encode())
                 or note["trace_id"] != row.get("trace_id")):
        raise ValueError("Analyst note does not match the recorded answer: " + row["evaluation_id"])
    answer_usage = detail["answer_producing_usage"]
    final_usage = detail.get("final_answer_usage")
    roles = collections.Counter()
    phases = collections.defaultdict(lambda: {"attempts": 0, "known_tokens": 0, "unknown_usage_calls": 0})
    for call in calls:
        roles[call["provider_role"]] += call["known_total_tokens"]
        phase = phases[call.get("phase") or "unavailable"]
        phase["attempts"] += 1
        phase["known_tokens"] += call["known_total_tokens"]
        phase["unknown_usage_calls"] += int(call.get("input_tokens") is None or call.get("output_tokens") is None)
    return {
        "evaluation_id": row["evaluation_id"], "qa_id": row["qa_id"],
        "track": row["benchmark_track"], "conversation": row["conversation_id"],
        "category": row["category_name"], "checkpoint": row["checkpoint_variant"],
        "cutoff_session": row["evaluation_cutoff_session"], "search_method": row["search_method"],
        "status": row["status"], "question": row["question"], "reference_answer": reference,
        "engram_answer": row.get("prediction"),
        "official_f1": row.get("official_locomo_f1") if scored else None,
        "answer_mode": row.get("answer_mode"), "stop_reason": row.get("stop_reason"),
        "known_query_tokens": usage["known_total_tokens"] if calls else (0 if executed else None),
        "estimated_query_tokens": usage["estimated_total_tokens"] if calls else None,
        "unknown_usage_calls": usage["unknown_usage_calls"] if calls else None,
        "usage_status": "UNKNOWN_PARTIAL" if usage["unknown_usage_calls"] else "ESTIMATED_PARTIAL" if usage["estimated_usage_calls"] else "PROVIDER_REPORTED" if calls else "NO_OBSERVED_MODEL_CALLS" if executed else "NOT_EXECUTED",
        "answer_producing_known_tokens": answer_usage["known_total_tokens"] if executed or calls else None,
        "final_answer_known_tokens": final_usage["known_total_tokens"] if executed and final_usage else None,
        "core_known_tokens": roles.get("core", 0) if calls else None,
        "frontier_known_tokens": roles.get("frontier", 0) if calls else None,
        "sdk_attempts": len(calls), "retry_count": usage["retry_count"],
        "latency_seconds": row["total_latency_ms"] / 1000 if row.get("total_latency_ms") is not None else None,
        "retrieved_gold_ids": retrieved, "supplied_gold_ids": supplied,
        "retrieval_provenance": row.get("provenance_status"),
        "supplied_provenance": row.get("packed_provenance_status"),
        "diagnostics": flags, "observations": observations, "phases": dict(phases),
        "review_status": "ANALYST_REVIEWED" if note else "AUTOMATED_OBSERVATIONS" if executed else "NOT_EXECUTED",
        "analyst_assessment": note.get("assessment") if note else None,
        "recommended_action": note.get("action") if note else None,
        "analyst_evidence": note.get("evidence", []) if note else [],
        "detail_page": relative.with_suffix(".html").as_posix(),
        "detail_json": relative.as_posix(), "review_page": "reviews/" + relative.stem + ".html",
        "usage_reconciliation": detail["usage_reconciliation"],
    }


def aggregate(rows):
    completed = [r for r in rows if r["status"] != "NOT_EXECUTED"]
    scores = [r["official_f1"] for r in completed if r["official_f1"] is not None]
    latency = [r["latency_seconds"] for r in completed if r["latency_seconds"] is not None]
    return {"expected": len(rows), "executed": len(completed),
            "completed_without_execution_error": sum(r["status"] == "COMPLETE" for r in completed),
            "errors": sum(r["status"] != "COMPLETE" for r in completed),
            "pending": len(rows) - len(completed), "scored": len(scores),
            "official_f1": statistics.mean(scores) if scores else None,
            "known_query_tokens": sum(r["known_query_tokens"] or 0 for r in rows),
            "mean_executed_known_tokens": statistics.mean(r["known_query_tokens"] or 0 for r in completed) if completed else None,
            "unknown_usage_calls": sum(r["unknown_usage_calls"] or 0 for r in rows),
            "median_latency_seconds": statistics.median(latency) if latency else None,
            "abstentions": sum("ABSTENTION" in r["diagnostics"] for r in completed)}


def paired(rows):
    groups = collections.defaultdict(dict)
    for row in rows:
        if row["track"] == "SESSION_ROUTING" and row["status"] != "NOT_EXECUTED":
            groups[row["qa_id"]][row["checkpoint"]] = row
    result = []
    for qa_id, variants in groups.items():
        a, b = variants.get("evidence_session"), variants.get("following_session")
        if not a or not b:
            continue
        result.append({"qa_id": qa_id, "question": a["question"],
                       "evidence_session_f1": a["official_f1"], "following_session_f1": b["official_f1"],
                       "f1_delta": b["official_f1"] - a["official_f1"],
                       "known_token_delta": b["known_query_tokens"] - a["known_query_tokens"],
                       "unknown_usage_calls": (a["unknown_usage_calls"] or 0) + (b["unknown_usage_calls"] or 0),
                       "evidence_review": a["review_page"], "following_review": b["review_page"]})
    return result


STYLE = """body{font:15px system-ui,sans-serif;color:#1b2b40;background:#f6f8fc;margin:0}main{max-width:1450px;margin:auto;padding:32px}h1{font-size:32px;margin:6px 0 12px}h2{margin-top:32px}p{line-height:1.6;max-width:1100px}a{color:#1558b5}table{border-collapse:collapse;width:100%;background:white}th,td{text-align:left;vertical-align:top;padding:12px;border-bottom:1px solid #dbe3ee}th{background:#eaf0f8}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#eef2f8;padding:16px}.cards{display:flex;gap:14px;flex-wrap:wrap}.card{padding:18px;background:white;border:1px solid #dbe3ee;border-radius:10px;min-width:170px}.card strong{display:block;font-size:26px}.muted{color:#596b83}.badge{display:inline-block;background:#fff1ce;padding:4px 9px;border-radius:5px}.controls{display:flex;gap:10px;flex-wrap:wrap;margin:20px 0}input,select,button{font:inherit;padding:9px;border:1px solid #b7c4d7;border-radius:5px}input{min-width:280px}.scroll{overflow-x:auto}details{background:white;padding:16px;margin:12px 0;border:1px solid #dbe3ee;border-radius:8px}summary{cursor:pointer;font-weight:600}.finding{padding:16px;border-left:4px solid #cf8a13;background:white;margin:14px 0}.question{min-width:230px}.answer{min-width:200px;max-width:350px}.small{font-size:13px}.links{margin:16px 0}button{cursor:pointer;background:white}code{overflow-wrap:anywhere}@media(max-width:700px){main{padding:16px}h1{font-size:25px}}"""


def esc(value):
    return html.escape("Unavailable" if value is None else str(value), quote=True)


def page(title, content):
    return f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{esc(title)}</title><style>{STYLE}</style></head><body><main>{content}</main></body></html>'


def display(value, percent=False):
    if value is None:
        return "Unavailable"
    return f"{value * 100:.2f}%" if percent else f"{value:,.2f}" if isinstance(value, float) else esc(value)


def write_csv(path, rows, fields):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value
                             for key, value in row.items() if key in fields})


def review_page(row):
    observation = "".join(f"<li>{esc(o)}</li>" for o in row["observations"])
    evidence = "".join(f"<li>{esc(o)}</li>" for o in row["analyst_evidence"])
    fields = [("Dataset reference", row["reference_answer"]), ("Exact Engram answer", row["engram_answer"]),
              ("Official LoCoMo F1", display(row["official_f1"], True)), ("Execution status", row["status"]),
              ("Known whole-query tokens", row["known_query_tokens"]), ("Usage status", row["usage_status"]),
              ("Unknown usage attempts", row["unknown_usage_calls"]), ("Answer-producing known tokens", row["answer_producing_known_tokens"]),
              ("Final-answer known tokens", row["final_answer_known_tokens"]), ("Core known tokens", row["core_known_tokens"]),
              ("Frontier known tokens", row["frontier_known_tokens"]), ("SDK attempts", row["sdk_attempts"]),
              ("Native query seconds", display(row["latency_seconds"]))]
    table = "".join(f"<tr><th>{esc(k)}</th><td>{esc(v)}</td></tr>" for k, v in fields)
    return page(row["question"], f'<a href="../analysis.html">← Benchmark review</a><p class="muted">{esc(row["evaluation_id"])}</p><h1>{esc(row["question"])}</h1><table>{table}</table>'
                f'<h2>Recorded assessment</h2><p>{esc(row["analyst_assessment"] or "No manual assessment is attached. The observations below describe recorded events only.")}</p>'
                f'<p><strong>Recommended action:</strong> {esc(row["recommended_action"] or "Inspect the supplied context and model calls before assigning a cause.")}</p><ul>{evidence}</ul>'
                f'<h2>Observed diagnostics</h2><p>{esc(", ".join(row["diagnostics"]) or "No selected diagnostic flags")}</p><ul>{observation}</ul>'
                f'<p><strong>Retrieved annotated evidence:</strong> {esc(row["retrieved_gold_ids"])}<br><strong>Annotated evidence supplied to final generation:</strong> {esc(row["supplied_gold_ids"])}</p>'
                f'<p class="muted">Retrieval provenance: {esc(row["retrieval_provenance"])}. Supplied provenance: {esc(row["supplied_provenance"])}. An empty final-generation set does not mean earlier Core assessments received no evidence.</p>'
                f'<p><a href="../{esc(row["detail_page"])}">Full instructions, exact requests, raw outputs, evidence and trace references</a> · <a href="../{esc(row["detail_json"])}">Lossless detail JSON</a></p>'
                f'<details><summary>Token consumption by phase</summary><pre>{esc(json.dumps(row["phases"], indent=2))}</pre></details>')


INDEX_JS = r"""
const rows=JSON.parse(document.getElementById('data').textContent);let offset=0;
const el=id=>document.getElementById(id),fmt=n=>n==null?'—':n.toLocaleString();
for(const [id,key] of [['track','track'],['category','category'],['flag','diagnostics']]){
 const values=[...new Set(rows.flatMap(r=>Array.isArray(r[key])?r[key]:[r[key]]))].sort();
 for(const v of values){const o=document.createElement('option');o.value=v;o.textContent=v;el(id).append(o)}
}
function render(){const q=el('search').value.toLowerCase();const selected=rows.filter(r=>
 (el('status').value==='all'||(el('status').value==='executed'?r.status!=='NOT_EXECUTED':r.status==='NOT_EXECUTED'))&&
 (!el('track').value||r.track===el('track').value)&&(!el('category').value||r.category===el('category').value)&&
 (!el('flag').value||r.diagnostics.includes(el('flag').value))&&
 [r.question,r.reference_answer,r.engram_answer,r.qa_id].join(' ').toLowerCase().includes(q));
 if(offset>=selected.length)offset=0;el('count').textContent=`${selected.length} matching evaluations · showing ${selected.length?offset+1:0}–${Math.min(offset+50,selected.length)}`;
 el('results').replaceChildren();for(const r of selected.slice(offset,offset+50)){
  const tr=document.createElement('tr');const cells=[r.question,r.reference_answer,r.engram_answer,
   r.official_f1==null?'Unavailable':(r.official_f1*100).toFixed(2)+'%',
   fmt(r.known_query_tokens)+(r.unknown_usage_calls?' + unknown usage':''),r.status,r.checkpoint,r.diagnostics.join(', ')];
  cells.forEach((v,i)=>{const td=document.createElement('td');td.textContent=v??'Unavailable';td.className=i===0?'question':i<3?'answer':'small';tr.append(td)});
  const td=document.createElement('td'),a=document.createElement('a');a.href=r.review_page;a.textContent='Review + full context';td.append(a);tr.append(td);el('results').append(tr);
 }el('prev').disabled=offset===0;el('next').disabled=offset+50>=selected.length;
}
for(const id of ['search','status','track','category','flag'])el(id).addEventListener('input',()=>{offset=0;render()});
el('prev').onclick=()=>{offset=Math.max(0,offset-50);render()};el('next').onclick=()=>{offset+=50;render()};render();
"""


def index_page(review):
    total = review["coverage"]
    cards = [("Expected evaluations", total["expected"]), ("Executed", total["executed"]),
             ("Pending", total["pending"]), ("Execution errors", total["errors"])]
    hero = "".join(f'<div class="card"><strong>{v:,}</strong>{k}</div>' for k, v in cards)
    tracks = "".join(f'<tr><td>{esc(k)}</td><td>{v["executed"]}/{v["expected"]}</td><td>{display(v["official_f1"], True)}</td><td>{display(v["mean_executed_known_tokens"])}</td><td>{display(v["median_latency_seconds"])}</td></tr>' for k, v in review["tracks"].items())
    categories = "".join(f'<tr><td>{esc(g["track"])}</td><td>{esc(g["category"])}</td><td>{g["executed"]}/{g["expected"]}</td><td>{display(g["official_f1"], True)}</td><td>{g["abstentions"]}</td></tr>' for g in review["categories"])
    findings = []
    for finding in review["engineering_findings"]:
        references = []
        for reference in finding.get("references", []):
            question, _, checkpoint = reference.partition(" / ")
            related = [r for r in review["evaluations"] if r.get("qa_id") == question
                       and r.get("track") == "SESSION_ROUTING"
                       and (checkpoint == "both checkpoints" or r.get("checkpoint") == checkpoint)]
            references.extend(f'<a href="{esc(r["review_page"])}">{esc(question)} / {esc(r["checkpoint"])}</a>' for r in related)
            if not related:
                references.append(f'<code>{esc(reference)}</code>')
        findings.append(f'<div class="finding"><strong>{esc(finding["priority"])} — {esc(finding["title"])}</strong><p>{esc(finding["observation"])}</p><p><strong>Action:</strong> {esc(finding["action"])}</p><p class="small">Evidence: {" · ".join(references)}</p></div>')
    data = json.dumps(review["evaluations"], ensure_ascii=False).replace("<", "\\u003c").replace("&", "\\u0026")
    lifecycle = "".join(f'<tr><td>{esc(k)}</td><td>{v["known_total_tokens"]:,}</td><td>{v["unknown_usage_calls"]}</td></tr>' for k, v in review["provider_accounting"]["components"].items())
    captured_at = review["snapshot"]["completed_at"]
    try:
        captured_at = datetime.fromisoformat(captured_at).astimezone(ZoneInfo("Asia/Kolkata")).strftime("%d %b %Y, %H:%M IST")
    except ValueError:
        pass
    return page("Engram question and answer benchmark", f'<span class="badge">{esc(review["status"])}</span><h1>Engram question and answer benchmark</h1>'
                f'<p class="muted">LoCoMo · seed 42 · captured {esc(captured_at)} · {esc(", ".join(review["models"]))} · {review["conversation_count"]} conversation(s)</p><div class="cards">{hero}</div>'
                '<p>Official scores below use executed evaluations, including execution failures. Pending evaluations remain in coverage and have no measured score. F1 measures reference-word overlap; it is not the percentage of factually correct answers. Retrieval comparisons generate no answers.</p>'
                '<div class="links"><a href="question_review.csv">Question / answer CSV</a> · <a href="review.json">Review JSON</a> · <a href="model_calls.csv">Every SDK attempt</a> · <a href="paired_checkpoints.csv">Checkpoint comparisons</a> · <a href="data_dictionary.md">Field definitions</a></div>'
                f'<h2>Measured results by track</h2><div class="scroll"><table><thead><tr><th>Track</th><th>Executed / expected</th><th>Official F1</th><th>Mean known query tokens</th><th>Median seconds</th></tr></thead><tbody>{tracks}</tbody></table></div>'
                f'<details><summary>Results by question category</summary><table><tr><th>Track</th><th>Category</th><th>Executed / expected</th><th>Official F1</th><th>Abstentions</th></tr>{categories}</table></details>'
                f'<h2>Engineering findings</h2>{"".join(findings)}<h2>Inspect questions and answers</h2>'
                '<div class="controls"><input id="search" aria-label="Search questions and answers" placeholder="Search question, answer or ID"><select id="status" aria-label="Execution coverage"><option value="executed">Executed evaluations</option><option value="all">All expected evaluations</option><option value="pending">Pending evaluations</option></select><select id="track" aria-label="Track"><option value="">All tracks</option></select><select id="category" aria-label="Category"><option value="">All categories</option></select><select id="flag" aria-label="Diagnostic"><option value="">All diagnostics</option></select></div>'
                '<p id="count" aria-live="polite"></p><div class="scroll"><table><thead><tr><th>Question</th><th>Dataset reference</th><th>Exact Engram answer</th><th>Official F1</th><th>Known query tokens</th><th>Status</th><th>Checkpoint</th><th>Observed diagnostics</th><th>Details</th></tr></thead><tbody id="results"></tbody></table></div><p><button id="prev">Previous 50</button> <button id="next">Next 50</button></p>'
                f'<details><summary>Provider consumption by lifecycle component</summary><p>Verified provider subtotals. Unknown billing is additional and is never represented as zero. Reasoning and cached tokens are subsets of provider totals.</p><table><tr><th>Component</th><th>Known tokens</th><th>Unknown attempts</th></tr>{lifecycle}</table></details>'
                '<p class="muted">Gold answers and evidence annotations are used for scoring and review only. Analyst notes are interpretations of captured records, not new judge-model scores. This report makes no causal accuracy claim from lexical flags. Exact prompts, outputs and trace archives remain linked on each detail page.</p>'
                f'<script type="application/json" id="data">{data}</script><script>{INDEX_JS}</script>')


def build_review(output, notes):
    report_manifest = read_json(output / "report_manifest.json")
    capture = read_json(output / "source_snapshot/snapshot_manifest.json")
    source_manifest = read_json(output / "source_snapshot/run_manifest.json")
    rows, models = [], set()
    note_map = notes.get("evaluations", {})
    for path in sorted((output / "questions").glob("*.json")):
        detail = read_json(path)
        rows.append(inspect_detail(detail, path.relative_to(output), note_map))
        models.update(c["model"] for c in detail["model_calls"] if c.get("model"))
    if len(rows) != source_manifest["expected_evaluations"] or len({r["evaluation_id"] for r in rows}) != len(rows):
        raise ValueError("Expected evaluation coverage is not complete")
    if report_manifest["usage_reconciliation"] != "MATCH":
        raise ValueError("Detail accounting does not reconcile")
    if source_manifest.get("report_schema_version", 1) >= 2 and report_manifest["observed_execution_status"] != "CAPTURE_COMPLETE":
        raise ValueError("Observed requests or attempts have incomplete capture")
    rows.sort(key=lambda r: (r["status"] == "NOT_EXECUTED", r["qa_id"], r["track"], r["checkpoint"], r["search_method"]))
    tracks = {track: aggregate([r for r in rows if r["track"] == track]) for track in sorted({r["track"] for r in rows})}
    categories = [{"track": track, "category": category, **aggregate([r for r in rows if r["track"] == track and r["category"] == category])}
                  for track, category in sorted({(r["track"], r["category"]) for r in rows})]
    coverage = aggregate(rows)
    # Aggregating canonical and checkpoint scores would double-count questions.
    coverage.pop("official_f1")
    review = {"review_schema_version": 1, "reviewer_source_sha256": digest(Path(__file__).read_bytes()),
              "status": "PROVISIONAL" if coverage["pending"] else "COMPLETE_WITH_ERRORS" if coverage["errors"] else "COMPLETE",
              "snapshot": capture, "source_run_id": source_manifest["run_id"], "dataset_sha256": source_manifest["dataset_sha256"],
              "source_config_hash": source_manifest["config_hash"], "models": sorted(models),
              "quality_modes": source_manifest.get("quality_modes"),
              "conversation_count": len({r["conversation"] for r in rows}), "coverage": coverage, "tracks": tracks,
              "categories": categories, "paired_checkpoints": paired(rows), "engineering_findings": notes.get("findings", []),
              "provider_accounting": read_json(output / "snapshot_accounting.json"),
              "diagnostic_counts": dict(collections.Counter(flag for r in rows if r["status"] != "NOT_EXECUTED" for flag in r["diagnostics"])),
              "analyst_reviewed_evaluations": sum(r["review_status"] == "ANALYST_REVIEWED" for r in rows),
              "evaluations": rows}
    if review["provider_accounting"]["status"] == "MISMATCH":
        raise ValueError("Provider journals failed reconciliation")
    write_json(output / "review.json", review)
    write_csv(output / "question_review.csv", rows, CSV_FIELDS)
    write_csv(output / "category_results.csv", categories, ["track", "category", *aggregate([])])
    write_csv(output / "paired_checkpoints.csv", review["paired_checkpoints"], ["qa_id", "question", "evidence_session_f1", "following_session_f1", "f1_delta", "known_token_delta", "unknown_usage_calls", "evidence_review", "following_review"])
    (output / "reviews").mkdir(exist_ok=True)
    for row in rows:
        (output / row["review_page"]).write_text(review_page(row), encoding="utf-8")
    (output / "analysis.html").write_text(index_page(review), encoding="utf-8")
    dictionary = output / "data_dictionary.md"
    dictionary.write_text(dictionary.read_text(encoding="utf-8").split("\n\n## Engineering review\n")[0] + "\n\n## Engineering review\n\n"
                          "`question_review.csv` covers every expected evaluation in every track. Pending scores are empty, and executed failures retain their official zero. Track/category means use executed scored evaluations only. Track means are never combined into an overall accuracy. All fields named `known` are provider-verified lower bounds when unknown usage exists. `answer_producing_known_tokens` includes generated candidates, even if later rejected; `final_answer_known_tokens` is linked to the returned answer where verifiable. Zero known model consumption for a deterministic answer means no observed model call, not zero unknown billing. Core/Frontier are provider roles, independent of model name.\n\n"
                          "`diagnostics` are non-exclusive observations, not judged failure causes. `REFERENCE_WORDS_WITH_EXTRA_TEXT` is lexical containment and does not prove correctness or detect negation. Retrieved gold IDs and supplied gold IDs are distinct. Incomplete provenance prevents exhaustive recall claims. `analyst_assessment` and `recommended_action` are manually reviewed notes bound to the exact answer hash; unreviewed rows are labelled. `phases` contains attempt counts and provider subtotals. `paired_checkpoints.csv` compares the same question at evidence/following checkpoints, not independent samples.\n\n"
                          "`source_snapshot` contains copied complete journal prefixes and immutable trace archives. Its manifest records capture start/end and file hashes. Concurrent lifecycle attempts may be in flight during capture. Gold annotations in report artifacts never enter benchmark ingestion or model prompts. No new model calls are made to generate this report.\n\n"
                          + "\n".join(f"- `{field}`" for field in CSV_FIELDS) + "\n", encoding="utf-8")
    return review


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--notes", type=Path)
    parser.add_argument("--image", default="engram-lifecycle:20261012-descriptive-v2")
    args = parser.parse_args()
    source, output = args.source.resolve(), args.output.resolve()
    if output == source or output.is_relative_to(source):
        parser.error("Review output must be separate from the active run")
    output.mkdir(parents=True, exist_ok=False)
    snapshot(source, output / "source_snapshot")
    notes = read_json(args.notes) if args.notes else {}
    write_json(output / "analyst_notes.json", notes)
    script = (
        "from pathlib import Path; from benchmarks.lifecycle.descriptive import generate_details; "
        "from benchmarks.lifecycle.report import accounting; from benchmarks.lifecycle.ledger import atomic; "
        "source=Path('/review/source_snapshot'); output=Path('/review'); "
        "generate_details(source,output); "
        "atomic(output/'snapshot_accounting.json',accounting(source,verify_archives=True))"
    )
    subprocess.run(["docker", "run", "--rm", "--network", "none", "--cpus", "2",
                    "--user", f"{os.getuid()}:{os.getgid()}", "--entrypoint", "python",
                    "-v", f"{ROOT / 'benchmarks'}:/workspace/benchmarks:ro",
                    "-v", f"{output}:/review", args.image, "-c", script], check=True)
    review = build_review(output, notes)
    print(json.dumps({"report": str(output / "analysis.html"), "status": review["status"], "coverage": review["coverage"], "tracks": review["tracks"]}, indent=2))


if __name__ == "__main__":
    main()
