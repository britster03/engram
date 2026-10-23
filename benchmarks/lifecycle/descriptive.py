"""Offline question reports, streamed from immutable lifecycle evidence.

Run: python -m benchmarks.lifecycle.descriptive SOURCE --output DESTINATION
Historical runs are never modified by this command. Integrated reports write
inside the new run. SQLite indexes journals without retaining all prompts in RAM.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import lzma
import os
import shutil
import sqlite3
import tempfile
from collections import Counter
from pathlib import Path

from .dataset import load
from .export_calc import compact_context
from .ledger import atomic, expand_trace, iter_read, safe
from .observer import summarize_usage

REPORT_SCHEMA_VERSION = 2
EXTRA_COLUMNS = [
    "Question ID", "Evaluation ID", "Core Model", "Frontier Model",
    "Query Input Tokens", "Query Output Tokens", "Known Query Tokens",
    "Estimated Query Tokens", "Unknown Usage Calls", "Token Count Source",
    "Answer-producing Input Tokens", "Answer-producing Output Tokens",
    "Answer-producing Known Tokens", "Final Answer Known Tokens",
    "Answer Link Status", "Detail Page", "Detail JSON",
]
CALL_COLUMNS = [
    "evaluation_id", "qa_id", "trace_id", "attempt_id", "operation_id",
    "provider_role", "role_source", "provider", "model", "sdk_api", "phase",
    "status", "submission", "attempt_number", "input_tokens", "output_tokens",
    "provider_input_tokens", "provider_output_tokens", "known_total_tokens",
    "reasoning_tokens", "cached_input_tokens", "token_count_source",
    "latency_ms", "request_status", "request_sha256", "response_sha256",
    "produced_answer", "produced_final_answer", "detail_json",
]
STYLE = """body{font:16px system-ui;margin:2rem auto;max-width:1250px;padding:0 1rem;color:#172033}
a{color:#1658b7}table{border-collapse:collapse;width:100%;margin:1rem 0}td,th{border:1px solid #d8dee8;padding:.55rem;text-align:left;vertical-align:top}
th{background:#edf2f8}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f4f6fa;padding:1rem;border-radius:6px}
details{margin:1rem 0}summary{cursor:pointer;font-weight:600}input,select{padding:.5rem;margin:.3rem;max-width:100%}
.note{color:#526075}.answer{background:#edf5ff;padding:1rem}h2{margin-top:2rem}code{overflow-wrap:anywhere}"""


def _dump(value):
    return json.dumps(safe(value), ensure_ascii=False, allow_nan=False)


def _hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _text(value):
    return "Unavailable" if value is None else str(value)


def _escape(value):
    return html.escape(_text(value), quote=True)


def _pre(value):
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)
    return "<pre>" + _escape(text) + "</pre>"


def _page(title, content):
    return ('<!doctype html><html lang="en"><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            f"<title>{_escape(title)}</title><style>{STYLE}</style><body>{content}</body></html>")


def _write_text(path, text):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def _write_csv(path, rows, fields):
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _dump(value) if isinstance(value, (dict, list))
                             else "" if value is None else value for key, value in row.items()})
    temporary.replace(path)


def _preview(value, depth=0):
    """Bound rendered diagnostics; full values remain in the trace archive."""
    if isinstance(value, str):
        return value if len(value) <= 1200 else {"preview": value[:1200], "characters": len(value), "truncated": True}
    if depth >= 7 and isinstance(value, (dict, list)):
        return {"preview_status": "NESTING_LIMIT", "count": len(value)}
    if isinstance(value, list):
        result = [_preview(v, depth + 1) for v in value[:10]]
        return result if len(value) <= 10 else {"preview": result, "total_count": len(value), "truncated": True}
    if isinstance(value, dict):
        return {k: _preview(v, depth + 1) for k, v in value.items()}
    return value


def _archive_reference(index, trace_id, trace, destination):
    stored = index.db.execute("SELECT payload FROM traces WHERE id=?", (trace_id,)).fetchone()
    record = json.loads(stored[0]) if stored else {}
    reference = {k: record[k] for k in ("trace_id", "trace_archive", "trace_archive_sha256") if k in record}
    if destination is None:
        return reference or {"trace_id": trace_id, "source": "trace.jsonl"}
    if reference.get("trace_archive"):
        target = destination / reference["trace_archive"]
        original = index.source / reference["trace_archive"]
        if target.resolve() != original.resolve() and not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(original, target)
            except OSError:
                shutil.copyfile(original, target)
        return reference
    encoded = _dump(trace).encode("utf-8")
    checksum = hashlib.sha256(encoded).hexdigest()
    relative = f"trace_payloads/{checksum}.json.xz"
    target = destination / relative
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        with lzma.open(target, "wb", preset=1) as stream:
            stream.write(encoded)
    return {"trace_id": trace_id, "trace_archive": relative, "trace_archive_sha256": checksum}


class JournalIndex:
    def __init__(self, source, database):
        self.source = source
        self.db = sqlite3.connect(database)
        self.db.executescript("""
            CREATE TABLE attempts(id TEXT PRIMARY KEY, trace TEXT, start TEXT, finish TEXT);
            CREATE INDEX attempts_trace ON attempts(trace);
            CREATE TABLE traces(id TEXT PRIMARY KEY, payload TEXT);
            CREATE TABLE submissions(trace TEXT PRIMARY KEY, evaluation TEXT);
            CREATE INDEX submissions_evaluation ON submissions(evaluation);
        """)
        excluded_file = source / "validation_attempt_ids.json"
        excluded = set(json.loads(excluded_file.read_text()).get("attempt_ids", [])) if excluded_file.exists() else set()
        for path in sorted(source.glob("provider_attempts.*.jsonl")):
            for record in iter_read(path, live=True):
                if record["attempt_id"] in excluded:
                    continue
                column = "start" if record["state"] == "STARTED" else "finish"
                previous = self.db.execute(f"SELECT {column} FROM attempts WHERE id=?", (record["attempt_id"],)).fetchone()
                if previous and previous[0] is not None:
                    raise ValueError("Duplicate provider attempt state: " + record["attempt_id"])
                self.db.execute("INSERT OR IGNORE INTO attempts(id,trace) VALUES(?,?)",
                                (record["attempt_id"], record["trace_id"]))
                self.db.execute(f"UPDATE attempts SET {column}=? WHERE id=?", (_dump(record), record["attempt_id"]))
                if record.get("evaluation_id"):
                    self._submission(record["trace_id"], record["evaluation_id"])
        for record in iter_read(source / "trace.jsonl", live=True):
            self.db.execute("INSERT INTO traces VALUES(?,?)", (record["trace_id"], _dump(record)))
        for record in iter_read(source / "evaluation_attempts.jsonl", live=True):
            self._submission(record["trace_id"], record["evaluation_id"])
        self.db.commit()

    def _submission(self, trace, evaluation):
        previous = self.db.execute("SELECT evaluation FROM submissions WHERE trace=?", (trace,)).fetchone()
        if previous and previous[0] != evaluation:
            raise ValueError("Trace belongs to multiple evaluations: " + trace)
        self.db.execute("INSERT OR IGNORE INTO submissions VALUES(?,?)", (trace, evaluation))

    def trace_ids(self, row):
        ids = [r[0] for r in self.db.execute("SELECT trace FROM submissions WHERE evaluation=? ORDER BY trace", (row["evaluation_id"],))]
        if row.get("trace_id") and row["trace_id"] not in ids:
            ids.append(row["trace_id"])
        return ids

    def trace(self, trace_id):
        found = self.db.execute("SELECT payload FROM traces WHERE id=?", (trace_id,)).fetchone()
        return expand_trace(self.source, json.loads(found[0])) if found else None

    def calls(self, trace_id):
        result = []
        for start, finish in self.db.execute("SELECT start,finish FROM attempts WHERE trace=?", (trace_id,)):
            beginning, ending = json.loads(start) if start else {}, json.loads(finish) if finish else {}
            call = {**beginning, **ending}
            call["started_time_ns"] = beginning.get("time_ns")
            call["finished_time_ns"] = ending.get("time_ns")
            if not ending:
                call.update(status="IN_FLIGHT_OR_INTERRUPTED", input_tokens=None,
                            output_tokens=None, token_count_source="unavailable")
            result.append(call)
        return sorted(result, key=lambda c: (c.get("started_time_ns") or 0, c["attempt_id"]))

    def signature(self, row):
        """Immutable attempts and trace identities invalidate provisional caches."""
        identity = [row]
        for trace_id in self.trace_ids(row):
            identity.append(list(self.db.execute("""SELECT id,length(start),length(finish),
                json_extract(finish,'$.time_ns'),json_extract(finish,'$.status')
                FROM attempts WHERE trace=? ORDER BY id""", (trace_id,))))
            stored = self.db.execute("SELECT payload FROM traces WHERE id=?", (trace_id,)).fetchone()
            if stored:
                trace = json.loads(stored[0])
                archive = self.source / trace["trace_archive"] if trace.get("trace_archive") else None
                identity.append([trace_id, trace.get("trace_archive_sha256"),
                                 archive.stat().st_mtime_ns if archive and archive.exists() else None,
                                 _hash(stored[0]) if not archive else None])
        return _hash(_dump(identity))

    def close(self):
        self.db.close()


def _detail(row, index, question, conversation, destination=None):
    calls, reads, contexts, events, archives = [], [], [], [], []
    for trace_id in index.trace_ids(row):
        trace = index.trace(trace_id)
        trace_events = trace.get("events", []) if trace else []
        reference = _archive_reference(index, trace_id, trace, destination) if trace else None
        if reference:
            archives.append(reference)
        observed = {c["attempt_id"]: c for c in trace.get("calls", [])} if trace else {}
        methods = {e.get("operation_id"): e.get("method") for e in trace_events if e["kind"] in {"provider_result", "provider_error"}}
        journal_calls = index.calls(trace_id)
        # Old inline traces can be rendered even when no SDK journals survive.
        if not journal_calls and observed:
            journal_calls = list(observed.values())
        ordinal = Counter()
        for call in journal_calls:
            # Native evaluation can reclassify a sufficiency ANSWER as generation.
            # Keep requests from the durable journal and phase metadata from trace.
            for field in ("phase", "caller_phase", "operation_role"):
                if field in observed.get(call["attempt_id"], {}):
                    call[field] = observed[call["attempt_id"]][field]
            method = call.get("method") or methods.get(call.get("operation_id"))
            role = call.get("provider_role")
            call["role_source"] = "captured" if role else "observed_method" if method else "unavailable"
            call["provider_role"] = role or ("core" if method == "complete" else "frontier" if method in {"answer", "plan_retrieval"} else "unknown")
            call["submission"] = "current" if trace_id == row.get("trace_id") else "prior"
            call["evaluation_id"], call["qa_id"] = row["evaluation_id"], row["qa_id"]
            ordinal[call.get("operation_id")] += 1
            call["attempt_number"] = ordinal[call.get("operation_id")]
            call["request_status"] = "CAPTURED" if isinstance(call.get("request"), dict) else "NOT_CAPTURED"
            call["request_sha256"] = _hash(_dump(call["request"])) if call["request_status"] == "CAPTURED" else None
            call["response_sha256"] = _hash(call["response_text"]) if isinstance(call.get("response_text"), str) else None
            call["known_total_tokens"] = summarize_usage([call])["known_total_tokens"]
            try:
                call["parsed_response"] = json.loads(call.get("response_text") or "")
            except (ValueError, TypeError):
                call["parsed_response"] = None
            calls.append(call)
        for event_index, event in enumerate(trace_events):
            enriched = {"trace_id": trace_id, **event}
            if event["kind"] == "store":
                read = _preview({k: v for k, v in enriched.items() if k != "result"})
                read.update(result_preview=_preview(event.get("result")),
                            result_status="RETURNED" if "result" in event else "FAILED",
                            raw_trace=reference, event_index=event_index)
                reads.append(read)
            elif event["kind"] == "model_context":
                contexts.append(enriched)
            elif event["kind"] in {"provider_result", "provider_error", "tool", "cascade", "semantic_fingerprint", "verification_rejection", "quality_packing", "packing", "context"}:
                events.append(enriched)

    answer_operations = set()
    final_operations = []
    deterministic = row.get("answer_mode") in {"deterministic-canonical", "evidence-terminal"}
    for event in events:
        result = event.get("result") or {}
        if event["kind"] == "provider_result" and event.get("method") == "answer" and isinstance(result, dict) and result.get("verdict") == "ANSWER" and result.get("answer"):
            identity = (event["trace_id"], event.get("operation_id"))
            answer_operations.add(identity)
            if not deterministic and event["trace_id"] == row.get("trace_id") and result["answer"] == row.get("prediction"):
                final_operations.append(identity)
    for call in calls:
        result = call.get("parsed_response")
        if call["provider_role"] == "frontier" and isinstance(result, dict) and result.get("verdict") == "ANSWER" and result.get("answer"):
            answer_operations.add((call["trace_id"], call.get("operation_id")))
    final_identity = final_operations[-1] if final_operations else None
    for call in calls:
        identity = (call["trace_id"], call.get("operation_id"))
        call["produced_answer"] = identity in answer_operations
        call["produced_final_answer"] = identity == final_identity
    answer_calls = [c for c in calls if c["produced_answer"]]
    final_calls = [c for c in calls if c["produced_final_answer"]]
    link_status = "NO_FRONTIER_GENERATION" if deterministic else "VERIFIED" if final_calls else "NOT_APPLICABLE" if row["benchmark_track"] == "SEARCH_COMPARISON" else "UNAVAILABLE"
    whole = summarize_usage(calls)
    recorded = row.get("known_total_tokens")
    reconciliation = "IN_PROGRESS_OR_NOT_EXECUTED" if row.get("status") == "NOT_EXECUTED" else "MATCH" if calls and recorded == whole["known_total_tokens"] else "MISMATCH" if calls and recorded is not None else "NOT_CAPTURED" if recorded else "MATCH"
    evidence = []
    if question and conversation:
        wanted = set(question.evidence)
        evidence = [{"dialogue_id": t.dia_id, "session": t.session, "speaker": t.speaker,
                     "timestamp": t.timestamp, "original_timestamp": t.raw_timestamp,
                     "text": t.text, "caption": t.caption, "content": t.content}
                    for t in conversation.turns if t.dia_id in wanted]
    return {"report_schema_version": REPORT_SCHEMA_VERSION, "evaluation": row,
            "reference_answer_display": "Unanswerable from the dialogue (adversarial)" if row.get("category_id") == 5 else row.get("gold_answer"),
            "gold_evidence": evidence, "gold_evidence_status": question.annotation_status if question else "UNAVAILABLE",
            "gold_annotation_notes": list(question.annotation_notes) if question else [],
            "retrieval": row.get("retrieval_context"), "source_reads": reads,
            "model_contexts": contexts, "operations": [_preview(e) for e in events],
            "model_calls": calls, "trace_archives": archives,
            "answer_link_status": link_status, "final_answer_operation": final_identity,
            "query_usage": whole, "answer_producing_usage": summarize_usage(answer_calls),
            "final_answer_usage": summarize_usage(final_calls) if final_calls or deterministic else None,
            "usage_reconciliation": reconciliation}


def _detail_html(detail, related):
    row = detail["evaluation"]
    content = '<p><a href="../question_report.html">← Question index</a></p>'
    content += f'<h1>{_escape(row["question"])}</h1><p class="note">{_escape(row["qa_id"])} · {_escape(row["category_name"])} · {_escape(row["benchmark_track"])} · checkpoint {_escape(row.get("evaluation_cutoff_session"))} · {_escape(row["status"])}</p>'
    content += '<h2>Expected answer</h2>' + _pre(detail["reference_answer_display"])
    content += '<h2>Engram answer</h2><div class="answer">' + _pre(row.get("prediction")) + '</div>'
    score = row.get("official_locomo_f1")
    content += f'<p>Official score: {_escape(round(score * 100, 2) if score is not None else None)}%. Answer linkage: {_escape(detail["answer_link_status"])}.</p>'
    reads = Counter(e.get("store") for e in detail["source_reads"])
    content += '<h2>Observed execution</h2><p>' + _escape(
        f"This evaluation recorded {len(detail['model_calls'])} SDK attempts and {len(detail['source_reads'])} store operations. "
        f"Store operation counts: {dict(reads)}. Terminal stage: {row.get('terminal_stage', 'unavailable')}. "
        f"The verified provider subtotal is {detail['query_usage']['known_total_tokens']} tokens; "
        f"{detail['query_usage']['unknown_usage_calls']} attempts have unknown usage.") + '</p>'
    content += '<p class="note">Returned candidates and supplied context are shown separately. Store and operation previews are bounded; complete results are in the lossless trace archives below. Model requests and responses are shown in full. Evidence coverage does not establish which claims caused the answer. Historical fields absent from journals remain unavailable.</p>'
    content += '<h2>Tokens and timing</h2>' + _pre({
        "whole_query": detail["query_usage"], "answer_producing": detail["answer_producing_usage"],
        "final_answer": detail["final_answer_usage"], "recorded_query_tokens": row.get("total_tokens"),
        "response_seconds": row.get("total_latency_ms", 0) / 1000 if row.get("total_latency_ms") is not None else None,
        "usage_reconciliation": detail["usage_reconciliation"]})
    content += '<h2>Supporting source records</h2>' + _pre({"annotation_status": detail["gold_evidence_status"], "annotation_notes": detail["gold_annotation_notes"], "records": detail["gold_evidence"]})
    content += '<h2>Retrieval and supplied evidence</h2>' + _pre({k: row.get(k) for k in ("retrieved_ids", "packed_evidence_ids", "provenance_status", "packed_provenance_status", "evidence_recall_at_k", "packed_fraction_evidence_recalled", "declared_support", "evidence_diagnostics")})
    for number, context in enumerate(detail["model_contexts"], 1):
        content += f'<details><summary>Supplied memory context {number}: {_escape(context.get("phase"))}</summary>' + _pre(context) + '</details>'
    for number, read in enumerate(detail["source_reads"], 1):
        content += f'<details><summary>Store operation {number}: {_escape(read.get("store"))} / {_escape(read.get("function"))}</summary>' + _pre(read) + '</details>'
    content += '<h2>Model calls: exact inputs and outputs</h2>'
    if not detail["model_calls"]:
        content += '<p>No model attempt was recorded. See execution status and answer mode.</p>'
    for number, call in enumerate(detail["model_calls"], 1):
        content += f'<details><summary>Call {number}: {_escape(call["provider_role"])} · {_escape(call.get("model"))} · {_escape(call.get("phase"))} · {_escape(call.get("status"))}</summary>'
        content += '<h3>Submitted SDK request</h3>' + _pre(call.get("request"))
        content += '<h3>Raw model response</h3>' + _pre(call.get("response_text"))
        content += '<h3>Usage and outcome</h3>' + _pre({k: v for k, v in call.items() if k not in {"request", "response_text"}}) + '</details>'
    content += '<details><summary>Operation results and diagnostics</summary>' + _pre(detail["operations"]) + '</details>'
    content += '<h2>Lossless raw trace archives</h2><ul>' + ''.join(
        f'<li><a href="../{_escape(a["trace_archive"])}">{_escape(a["trace_id"])}</a> · SHA-256 {_escape(a["trace_archive_sha256"])}</li>'
        for a in detail["trace_archives"] if a.get("trace_archive")) + '</ul>'
    content += '<h2>Related evaluations</h2><ul>' + ''.join(f'<li><a href="{_hash(e["evaluation_id"])}.html">{_escape(e["benchmark_track"])} / {_escape(e.get("checkpoint_variant"))} / {_escape(e.get("search_method"))}</a></li>' for e in related) + '</ul>'
    content += f'<p><a href="{_hash(row["evaluation_id"])}.json">Full detail JSON</a></p>'
    return _page(row["question"], content)


def _index_html(records):
    content = '<h1>Engram LoCoMo: question-by-question report</h1><p>Original questions, reference and actual answers, provider consumption, and full submitted model inputs.</p><p><a href="management_compact.csv">Spreadsheet CSV</a> · <a href="management.csv">Full management CSV</a> · <a href="model_calls.csv">Model attempts</a> · <a href="report_manifest.json">Report manifest</a> · <a href="data_dictionary.md">Field definitions</a></p>'
    content += '<label>Search <input id="search" type="search" placeholder="Question, reference or answer"></label>'
    for key, label in [("conversation", "Conversation"), ("category", "Category"), ("status", "Status")]:
        values = sorted({str(r[key]) for r in records})
        content += f'<label>{label} <select id="{key}"><option value="">All</option>' + ''.join(f'<option value="{_escape(v)}">{_escape(v)}</option>' for v in values) + '</select></label>'
    content += '<label>Score <select id="score"><option value="">All</option><option value="full">100%</option><option value="partial">Between 0% and 100%</option><option value="zero">0%</option><option value="unknown">Unavailable</option></select></label><label>Usage <select id="usage"><option value="">All</option><option value="unknown">Has unknown usage</option><option value="known">No unknown attempts</option></select></label><p id="count" aria-live="polite"></p><table><thead><tr><th>Conversation / category</th><th>Question</th><th>Expected answer</th><th>Engram answer</th><th>Score %</th><th>Known query tokens</th><th>Unknown calls</th><th>Status</th></tr></thead><tbody>'
    for r in records:
        attrs = {k: r[k] for k in ("conversation", "category", "status", "score", "unknown")}
        attrs["search"] = ' '.join(_text(r[k]) for k in ("question", "gold", "answer")).casefold()
        content += '<tr ' + ' '.join(f'data-{k}="{_escape(v if v is not None else "")}"' for k, v in attrs.items()) + '>'
        content += f'<td>{_escape(r["conversation"])}<br>{_escape(r["category"])}</td><td><a href="{r["page"]}">{_escape(r["question"])}</a></td>'
        content += ''.join(f'<td>{_escape(r[k])}</td>' for k in ("gold", "answer", "score", "known", "unknown", "status")) + '</tr>'
    content += """</tbody></table><script>
const controls=['search','conversation','category','status','score','usage'].map(id=>document.getElementById(id));
function filter(){let count=0;for(const row of document.querySelectorAll('tbody tr')){
const d=row.dataset;const [search,conversation,category,status,score,usage]=controls.map(c=>c.value);
const n=Number(d.score);const matchesScore=!score||(score==='unknown'?d.score==='':d.score!==''&&(score==='full'?n===100:score==='zero'?n===0:n>0&&n<100));
const visible=d.search.includes(search.toLowerCase())&&(!conversation||d.conversation===conversation)&&(!category||d.category===category)&&(!status||d.status===status)&&matchesScore&&(!usage||(usage==='unknown'?Number(d.unknown)>0:Number(d.unknown)===0));
row.hidden=!visible;if(visible)count++;}document.getElementById('count').textContent=count+' questions shown';}
for(const c of controls)c.addEventListener('input',filter);filter();
</script>"""
    return _page("Engram LoCoMo descriptive benchmark", content)


def generate_details(source, destination=None, *, rows=None, management=None):
    from .report import MANAGEMENT, completed_rows, denominator_rows, management_rows
    source = Path(source).resolve()
    destination = Path(destination).resolve() if destination else source
    destination.mkdir(parents=True, exist_ok=True)
    details_dir = destination / "questions"
    details_dir.mkdir(exist_ok=True)
    manifest = json.loads((source / "run_manifest.json").read_text())
    rows = denominator_rows(manifest, completed_rows(source)) if rows is None else rows
    management = management_rows(rows) if management is None else management
    canonical = [r for r in rows if r["benchmark_track"] == "CANONICAL"]
    management_by_id = {r["evaluation_id"]: m for r, m in zip(canonical, management, strict=True)}
    conversations = load()
    question_lookup = {q.qa_id: (q, c) for c in conversations for q in c.questions}
    related = {}
    for row in rows:
        related.setdefault(row["qa_id"], []).append(row)
    records, updated_management, compact_management = [], [], []
    totals, mismatches, uncaptured = Counter(), [], []
    missing_calls, unknown_roles = [], []
    cache_path = destination / "report_cache.json"
    implementation_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    previous_cache = json.loads(cache_path.read_text()) if cache_path.exists() else {}
    cached = previous_cache.get("evaluations", {}) if previous_cache.get("implementation_hash") == implementation_hash else {}
    new_cache = {}
    call_temp = destination / "model_calls.csv.tmp"
    with tempfile.TemporaryDirectory(prefix=".report-index-", dir=destination) as temporary:
        index = JournalIndex(source, Path(temporary) / "journals.sqlite")
        try:
            for row in rows:
                if row.get("trace_id"):
                    index._submission(row["trace_id"], row["evaluation_id"])
            index.db.commit()
            with call_temp.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=CALL_COLUMNS, extrasaction="ignore")
                writer.writeheader()
                for row in rows:
                    question, conversation = question_lookup.get(row["qa_id"], (None, None))
                    name = _hash(row["evaluation_id"])
                    json_path, page_path = f"questions/{name}.json", f"questions/{name}.html"
                    signature = index.signature(row)
                    entry = cached.get(row["evaluation_id"], {})
                    if entry.get("signature") == signature and (destination / json_path).exists() and (destination / page_path).exists() and all((destination / a["trace_archive"]).exists() for a in entry.get("trace_archives", [])):
                        detail = entry["summary"]
                    else:
                        detail = _detail(row, index, question, conversation, destination)
                        atomic(destination / json_path, detail)
                        _write_text(destination / page_path, _detail_html(detail, related[row["qa_id"]]))
                        detail = {**{k: detail[k] for k in ("query_usage", "answer_producing_usage", "final_answer_usage", "answer_link_status", "usage_reconciliation", "reference_answer_display", "trace_archives")},
                                  "model_calls": [{k: v for k, v in c.items() if k in CALL_COLUMNS} for c in detail["model_calls"]]}
                    new_cache[row["evaluation_id"]] = {"signature": signature, "summary": detail,
                                                       "trace_archives": detail["trace_archives"]}
                    if detail["usage_reconciliation"] == "MISMATCH":
                        mismatches.append(row["evaluation_id"])
                    if not detail["model_calls"] and (row.get("known_total_tokens") or
                            row.get("status") == "COMPLETE" and row.get("answer_mode") in {
                                "canonical-model", "current-session", "past-session", "parametric"}):
                        missing_calls.append(row["evaluation_id"])
                    for call in detail["model_calls"]:
                        writer.writerow({k: "" if v is None else v for k, v in {**call, "detail_json": json_path}.items() if k in CALL_COLUMNS})
                        totals["model_attempts"] += 1
                        totals["known_query_tokens"] += call["known_total_tokens"]
                        totals["unknown_usage_calls"] += int(call.get("input_tokens") is None or call.get("output_tokens") is None)
                        if call["request_status"] != "CAPTURED":
                            uncaptured.append(call["attempt_id"])
                        if call["provider_role"] == "unknown":
                            unknown_roles.append(call["attempt_id"])
                    if row["benchmark_track"] != "CANONICAL":
                        continue
                    whole, answer, final = detail["query_usage"], detail["answer_producing_usage"], detail["final_answer_usage"]
                    models = {role: sorted({c["model"] for c in detail["model_calls"] if c["provider_role"] == role and c.get("model")}) for role in ("core", "frontier")}
                    extra = dict(zip(EXTRA_COLUMNS, [row["qa_id"], row["evaluation_id"], models["core"] or None, models["frontier"] or None,
                        whole["total_input_tokens"], whole["total_output_tokens"], whole["known_total_tokens"], whole["estimated_total_tokens"], whole["unknown_usage_calls"], whole["token_count_source"],
                        answer["total_input_tokens"], answer["total_output_tokens"], answer["known_total_tokens"], final["known_total_tokens"] if final is not None else None,
                        detail["answer_link_status"], page_path, json_path], strict=True))
                    full_row = {**management_by_id[row["evaluation_id"]], **extra}
                    updated_management.append(full_row)
                    context = full_row["Retrieved Data / Frontier Context"]
                    compact_management.append({**full_row, "Retrieved Data / Frontier Context": compact_context(context, json_path)})
                    records.append({"conversation": row["conversation_id"], "category": row["category_name"], "question": row["question"], "gold": detail["reference_answer_display"], "answer": row.get("prediction"),
                                    "score": round(row["official_locomo_f1"] * 100, 2) if row.get("official_locomo_f1") is not None else None,
                                    "known": whole["known_total_tokens"] if detail["usage_reconciliation"] != "NOT_CAPTURED" else None,
                                    "unknown": whole["unknown_usage_calls"], "status": row["status"], "page": page_path})
            unmapped = [r[0] for r in index.db.execute("""SELECT id FROM attempts
                WHERE COALESCE(json_extract(finish,'$.component'),json_extract(start,'$.component'))='query'
                AND trace NOT IN (SELECT trace FROM submissions)""")]
        finally:
            index.close()
    call_temp.replace(destination / "model_calls.csv")
    atomic(cache_path, {"implementation_hash": implementation_hash, "evaluations": new_cache})
    _write_csv(destination / "management.csv", updated_management, MANAGEMENT + EXTRA_COLUMNS)
    _write_csv(destination / "management_compact.csv", compact_management, MANAGEMENT + EXTRA_COLUMNS)
    _write_text(destination / "question_report.html", _index_html(records))
    report_manifest = {"report_schema_version": REPORT_SCHEMA_VERSION, "source_run": str(source),
        "dataset_sha256": manifest.get("dataset_sha256"), "source_config_hash": manifest.get("config_hash"),
        "evaluation_count": len(rows), "canonical_question_count": len(canonical), **totals,
        "usage_reconciliation": "MISMATCH" if mismatches else "MATCH",
        "mismatched_evaluations": mismatches, "uncaptured_requests": uncaptured,
        "missing_call_evaluations": missing_calls, "unknown_role_attempts": unknown_roles,
        "unmapped_attempts": unmapped, "observed_execution_status": "UNAVAILABLE_HISTORICAL_FIELDS_ALLOWED" if manifest.get("report_schema_version", 1) < 2 else "CAPTURE_COMPLETE" if not any((uncaptured, unmapped, mismatches, missing_calls, unknown_roles)) else "INCOMPLETE",
        "reviewed_excluded_dataset": {"url": "https://huggingface.co/datasets/hqfang/memorybench", "reason": "Robotics spatial/action memory; no conversational QA protocol"}}
    atomic(destination / "report_manifest.json", report_manifest)
    dictionary = source / "data_dictionary.md"
    base_dictionary = dictionary.read_text() if dictionary.exists() else "# Data dictionary\n"
    marker = "\n## Descriptive report schema v2\n"
    base_dictionary = base_dictionary.split(marker)[0]
    _write_text(destination / "data_dictionary.md", base_dictionary + marker + "\n" +
        "Full requests, raw responses, store results and evidence are in the linked per-evaluation JSON and HTML. "
        "management_compact.csv bounds context previews; management.csv retains the original full context column. "
        "Query Tokens are original whole-query totals; Known Query Tokens exclude estimates and unknown billing. "
        "Answer-producing tokens cover all operations observed to return an answer, including retries in those operations. "
        "Final Answer Known Tokens requires a verified matching returned answer; deterministic rendering uses zero frontier tokens. "
        "Core and Frontier roles describe provider objects, even when their model names are identical. "
        "request_sha256 hashes UTF-8 JSON of the credential-free request; response_sha256 hashes unmodified UTF-8 response text. "
        "Role inference for historical data uses observed method identity only, never model-name guesses. "
        "Requests absent from historical journals are NOT_CAPTURED. Reasoning and cache counts are subsets of input/output. "
        "Lifecycle ingestion/audit usage is separate and remains in the source run's accounting.json.\n\n" +
        '\n'.join(f"- `{column}`" for column in EXTRA_COLUMNS + CALL_COLUMNS) + "\n")
    return report_manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.source.resolve() == args.output.resolve():
        parser.error("Historical report output must differ from the source run")
    print(json.dumps(generate_details(args.source, args.output), indent=2))


if __name__ == "__main__":
    main()
