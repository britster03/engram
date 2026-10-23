"""Management and technical reports with failures and full denominators visible."""
from __future__ import annotations

import csv
import gzip
import html
import json
from collections import defaultdict
from pathlib import Path

from .dataset import CATEGORIES, evaluations, load, select
from .ledger import atomic, expand_trace, iter_read, read, usage_record
from .metrics import bootstrap_conversations, quantile
from .observer import summarize_usage

MANAGEMENT = ["Conversation", "Category", "Question", "Expected Answer", "Engram Answer", "Retrieved Data / Frontier Context", "Official Score %", "Evidence Found@10 %", "Evidence Supplied to Answer %", "Semantic Preservation Status", "Terminal Stage", "Query Tokens", "Response Seconds", "Status", "Issue"]
TECHNICAL = "run_id git_sha config_hash benchmark_track evaluation_id trace_id conversation_id qa_id category_id category_name question gold_answer prediction gold_evidence_ids gold_evidence_sessions evaluation_cutoff_session checkpoint_variant question_status expected_level router_selected_level entered_levels executed_levels retrieval_source_levels supplied_context_levels answer_support_levels answer_support_observability routing_correct retrieval_triggered search_method search_mode hybrid_search search_components channel_statuses fusion_method reranker_used top_k retrieved_ids packed_evidence_ids any_evidence_recalled all_evidence_recalled fraction_evidence_recalled evidence_recall_at_k precision_at_k mrr ndcg_at_k packed_evidence_recall_at_k packed_precision_at_k packed_mrr packed_ndcg_at_k packed_fraction_evidence_recalled provenance_coverage provenance_status raw_question_tokens system_prompt_tokens current_session_context_tokens historical_context_tokens retrieved_context_tokens router_input_tokens router_output_tokens answerability_input_tokens answerability_output_tokens query_rewrite_input_tokens query_rewrite_output_tokens generation_input_tokens generation_output_tokens reasoning_tokens total_input_tokens total_output_tokens total_reasoning_tokens cached_input_tokens total_tokens known_total_tokens unknown_usage_calls token_count_source official_locomo_f1 exact_match raw_exact_match adversarial_correct rouge_l bertscore_f1 composite_similarity secondary_metric_status routing_latency_ms retrieval_latency_ms generation_latency_ms answerability_latency_ms total_latency_ms terminal_stage model_name fallback_used retry_count semantic_preservation_status status error".split()
LIMITATIONS = ["Classifier accuracy is unavailable: native routing is deterministic and predicted_depth is a constant.", "Exact dialogue recall is unavailable when delivered summaries or derived artifacts lack exact lineage; lower bounds are separately labelled.", "Generated answers do not identify contributing claims; answer_support_levels and actual_answer_graph_depth remain unavailable.", "Dataset timezone-free timestamps use UTC; native assertion time and query clock are preserved, which can limit historical queries.", "Unknown provider usage is retained as unknown; known token totals are lower bounds and reasoning/cache fields are subsets.", "Search comparisons produce retrieval metrics only. Secondary answer similarity and semantic audits do not replace official scoring.", "Graph paths returned can be observed; nodes scanned by the database cannot.", "Pilot confidence intervals have only one conversation cluster and cannot express between-conversation uncertainty.","Readiness traces record projection policy and revision lag. Enforced quality runs require canonical revision alignment; legacy runs may only verify requested publications."]


def csv_write(path, rows, fields=None):
    path = Path(path)
    fields = fields or list(dict.fromkeys(k for r in rows for k in r))
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False, allow_nan=False) if isinstance(v, (dict, list, tuple)) else "" if v is None else v for k,v in row.items()})
    temporary.replace(path)


def conservative_lower_bounds(row):
    # Unknown dialogue positions can move known evidence beyond K. Only
    # unbounded any/all/fraction coverage remains a defensible lower bound.
    for field,status in [("retrieval_lower_bound","provenance_status"),("packed_lower_bound","packed_provenance_status")]:
        value=row.get(field)
        if isinstance(value,dict) and row.get(status)=="INCOMPLETE":
            row[field]={**value,**{key:None for key in value if key=="mrr" or key.startswith(("recall_at_","precision_at_","ndcg_at_"))},"ranked_metric_status":"UNAVAILABLE_INCOMPLETE_ORDERING"}
    return row


def completed_rows(directory):
    rows = read(directory/"result_rows.jsonl")
    ids = [r["evaluation_id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise RuntimeError("Duplicate evaluation rows")
    audits = {r["qa_id"]: r for r in read(directory/"semantic_audit.jsonl")}
    secondary = json.loads((directory/"secondary_scores.json").read_text()) if (directory/"secondary_scores.json").exists() else {}
    corrections={r["trace_id"]:r["fields"] for r in read(directory/"measurement_corrections.jsonl")}
    submissions=defaultdict(set)
    for attempt in iter_read(directory/"evaluation_attempts.jsonl"):
        if attempt["state"]=="STARTED":
            submissions[attempt["evaluation_id"]].add(attempt["trace_id"])
    prior_traces={trace for row in rows for trace in submissions[row["evaluation_id"]] if trace!=row["trace_id"]}
    prior_calls=defaultdict(list)
    if prior_traces:
        for path in directory.glob("provider_attempts.*.jsonl"):
            for call in iter_read(path,live=True):
                if call["state"]=="FINISHED" and call["trace_id"] in prior_traces:
                    prior_calls[call["trace_id"]].append(usage_record(call))
    for row in rows:
        row.update(corrections.get(row["trace_id"],{}))
        conservative_lower_bounds(row)
        row.update(secondary.get(row["evaluation_id"], {}))
        audit = audits.get(row["qa_id"])
        if audit and row["checkpoint_variant"] == "completed":
            row["semantic_preservation_status"] = audit["semantic_preservation_status"]
        elif row.get("question_status") not in {"VALID","NORMALIZED"}:
            row["semantic_preservation_status"] = "NOT_EVALUABLE"
        prior=[c for trace in submissions[row["evaluation_id"]] if trace!=row["trace_id"] for c in prior_calls[trace]]
        totals=summarize_usage(prior)
        row.update(resubmission_count=max(0,len(submissions[row["evaluation_id"]])-1),current_attempt_known_tokens=row["known_total_tokens"],prior_attempt_known_tokens=totals["known_total_tokens"],prior_attempt_unknown_usage_calls=totals["unknown_usage_calls"])
        if prior:
            row["estimated_usage_calls"]=(row.get("estimated_usage_calls") or 0)+totals["estimated_usage_calls"]
            row["estimated_total_tokens"]=(row.get("estimated_total_tokens") or 0)+totals["estimated_total_tokens"]
            for key in ["total_tokens","total_input_tokens","total_output_tokens","total_reasoning_tokens","cached_input_tokens","generation_input_tokens","generation_output_tokens","router_input_tokens","router_output_tokens","answerability_input_tokens","answerability_output_tokens"]:
                row[key]=None if row.get(key) is None or totals.get(key) is None else row[key]+totals[key]
            row["known_total_tokens"]+=totals["known_total_tokens"]
            row["unknown_usage_calls"]+=totals["unknown_usage_calls"]
            if totals["unknown_usage_calls"]:
                row["token_count_source"]="unavailable_partial"
            elif row["estimated_usage_calls"]:
                row["token_count_source"]="provider_and_estimate"
    return rows


def denominator_rows(manifest, rows):
    completed = {r["evaluation_id"]: r for r in rows}
    dataset = load()
    selected = select(dataset, manifest["profile"])
    result = []
    for conv in dataset:
        for evaluation in evaluations(conv, selected):
            row = completed.get(evaluation.key)
            if row is None:
                q = evaluation.question
                row = {"run_id": manifest["run_id"], "git_sha": manifest["git_sha"], "config_hash": manifest["config_hash"], "evaluation_id": evaluation.key, "benchmark_track": evaluation.track, "conversation_id": conv.id, "qa_id": q.qa_id, "question": q.question, "gold_answer": q.answer, "category_id": q.category, "category_name": CATEGORIES[q.category], "checkpoint_variant": evaluation.variant, "evaluation_cutoff_session": evaluation.cutoff, "search_method": evaluation.method, "question_status": q.annotation_status, "status": "NOT_EXECUTED", "prediction": None, "official_locomo_f1": 0.0 if evaluation.track != "SEARCH_COMPARISON" else None, "error": "Block incomplete; question has not been executed", "total_tokens": None, "known_total_tokens": 0, "semantic_preservation_status": "NOT_EXECUTED"}
            result.append(row)
    if set(completed)-{r["evaluation_id"] for r in result}:
        raise RuntimeError("Results outside the declared evaluation profile")
    return result


def accounting(directory,*,verify_archives=False):
    starts, finished, duplicates = {}, {}, []
    exclusions=json.loads((directory/"validation_attempt_ids.json").read_text()) if (directory/"validation_attempt_ids.json").exists() else {"attempt_ids":[]}
    excluded=set(exclusions["attempt_ids"])
    validation_workflows=json.loads((directory/"validation_workflow_ids.json").read_text()).get("workflow_ids",[]) if (directory/"validation_workflow_ids.json").exists() else []
    for path in directory.glob("provider_attempts.*.jsonl"):
        for raw in iter_read(path,live=True):
            row=usage_record(raw)
            target = starts if row["state"] == "STARTED" else finished
            if row["attempt_id"] in excluded:
                continue
            if row.get("workflow_id") in validation_workflows:
                row["component"]="validation_live"
            if row["attempt_id"] in target:
                duplicates.append(row["attempt_id"])
            target[row["attempt_id"]] = row
    attempts = list(finished.values())
    interrupted = [{**r, "status": "INTERRUPTED", "input_tokens": None, "output_tokens": None, "token_count_source": "unavailable"} for key,r in starts.items() if key not in finished]
    groups = {}
    for component in {r["component"] for r in [*attempts,*interrupted]}:
        groups[component] = summarize_usage([r for r in [*attempts,*interrupted] if r["component"] == component])
    result_rows=read(directory/"result_rows.jsonl")
    needed={r["trace_id"] for r in result_rows}
    traces={}
    archive_failures=[]
    for t in iter_read(directory/"trace.jsonl"):
        if t.get("trace_archive"):
            try:
                if not (directory/t["trace_archive"]).is_file():
                    raise FileNotFoundError("Trace archive missing")
                if verify_archives:
                    full=expand_trace(directory,t)
                    if full["calls"]!=t["calls"] or len(full["events"])!=t["event_count"]:
                        raise ValueError("Trace index differs from archive")
            except Exception as error:
                archive_failures.append({"trace_id":t["trace_id"],"error_type":type(error).__name__})
        if t["trace_id"] in needed:
            traces[t["trace_id"]]={"attempt_ids":{c["attempt_id"] for c in t["calls"]}}
    query_mismatches = []
    calls_by_trace=defaultdict(list)
    for call in attempts:
        calls_by_trace[call["trace_id"]].append(call)
    for row in result_rows:
        trace = traces.get(row["trace_id"])
        actual=calls_by_trace[row["trace_id"]]
        if trace is None or trace["attempt_ids"] != {c["attempt_id"] for c in actual} or summarize_usage(actual)["known_total_tokens"] != row["known_total_tokens"]:
            query_mismatches.append(row["evaluation_id"])
    orphan_finished=sorted(set(finished)-set(starts))
    return {"status": "MISMATCH" if duplicates or query_mismatches or orphan_finished or archive_failures else "IN_FLIGHT_OR_INTERRUPTED" if interrupted else "RECONCILED", "started": len(starts), "finished": len(finished), "excluded_nonbillable_validation_attempt_ids":sorted(excluded), "interrupted_attempt_ids": [r["attempt_id"] for r in interrupted],"recovered_interrupted_attempt_ids":[r["attempt_id"] for r in attempts if r.get("status")=="INTERRUPTED"],"orphan_finished_attempt_ids":orphan_finished, "duplicate_attempt_ids": duplicates, "question_accounting_mismatches": query_mismatches,"trace_archive_failures":archive_failures,"trace_archive_checks":"FULL_CHECKSUM_AND_INDEX" if verify_archives else "FILE_EXISTENCE", "components": groups}


def aggregates(rows,*,bootstrap_samples=2000):
    groups = defaultdict(list)
    for row in rows:
        track = row["benchmark_track"]
        for dimension, value in [("overall", "all"), ("category", row["category_name"]), ("conversation", row["conversation_id"]), ("search_method", row["search_method"]), ("terminal_stage", row.get("terminal_stage")), ("search_mode", row.get("search_mode")),*[("executed_level",v) for v in row.get("executed_levels",[])],*[("retrieval_source_level",v) for v in row.get("retrieval_source_levels",[])],*[("supplied_context_level",v) for v in row.get("supplied_context_levels",[])]]:
            if value is not None:
                groups[(track, dimension, value)].append(row)
    result = []
    for (track, dimension, value), subset in groups.items():
        scores = [r["official_locomo_f1"] for r in subset if r.get("official_locomo_f1") is not None]
        completed = sum(r["status"] == "COMPLETE" for r in subset)
        errors = sum(r["status"] not in {"COMPLETE", "NOT_EXECUTED"} for r in subset)
        recall = [(r.get("evidence_recall_at_k") or {}).get("10") for r in subset]
        valid = [v for v in recall if v is not None]
        lo, hi = bootstrap_conversations(subset, "official_locomo_f1",samples=bootstrap_samples)
        record={"track": track, "dimension": dimension, "group": value, "expected": len(subset), "completed": completed, "errors": errors, "not_executed": sum(r["status"] == "NOT_EXECUTED" for r in subset), "completion_rate": completed/len(subset), "error_rate": errors/len(subset), "official_score": sum(scores)/len(scores) if scores else None, "official_ci_low": lo, "official_ci_high": hi, "evidence_recall_at_10": sum(valid)/len(valid) if valid else None, "evidence_metric_denominator": len(valid), "known_query_tokens": sum(r.get("known_total_tokens") or 0 for r in subset), "unknown_usage_calls": sum(r.get("unknown_usage_calls") or 0 for r in subset), **{f"latency_p{p}_ms": quantile([r.get("total_latency_ms") for r in subset], p/100) for p in [50,95,99]}}
        for prefix,field in [("recall","evidence_recall_at_k"),("precision","precision_at_k"),("ndcg","ndcg_at_k"),("packed_recall","packed_evidence_recall_at_k"),("packed_precision","packed_precision_at_k"),("packed_ndcg","packed_ndcg_at_k")]:
            for k in [1,3,5,10,20]:
                values=[r[field].get(str(k)) for r in subset if isinstance(r.get(field),dict) and r[field].get(str(k)) is not None]
                record[f"{prefix}_at_{k}"]=sum(values)/len(values) if values else None
                record[f"{prefix}_at_{k}_denominator"]=len(values)
        result.append(record)
    return result


def extraction_counts(event, artifacts, legacy):
    """Read the native canonical artifacts, retaining missing measurements."""
    def payload(key):
        value=artifacts.get((event["event_id"],key),{}).get("payload")
        return json.loads(value) if isinstance(value,str) else value
    gate=payload("gate-v1")
    raw,typed=payload("extract-v1"),payload("typed-v1")
    normalized=len(typed["triplets"]) if isinstance(typed,dict) and isinstance(typed.get("triplets"),list) else None
    if isinstance(raw,dict) and isinstance(raw.get("triplets"),list):
        count,source=len(raw["triplets"]),"CANONICAL_ARTIFACT"
    elif event["status"]=="GATED_SKIP":
        count,normalized,source=0,0,"GATED_SKIP_NO_EXTRACTION"
    elif event["event_id"] in legacy:
        triplets=legacy[event["event_id"]].get("triplets")
        triplets=json.loads(triplets) if isinstance(triplets,str) else triplets
        count,source=(len(triplets),"LEGACY_EXTRACTION") if isinstance(triplets,list) else (None,"UNAVAILABLE")
    else:
        count,source=None,"UNAVAILABLE"
    return {"extracted_triplets":count,"normalized_extracted_triplets":normalized,"extraction_count_source":source,"gate_store":gate.get("store") if isinstance(gate,dict) else None}


def ingestion_rows(directory):
    index = {r["event_id"]: r for r in read(directory/"provenance.jsonl")}
    latest = {}
    for path in [*directory.glob("checkpoints/*/*.json.gz"),*directory.glob("diagnostics/*/*.json.gz")]:
        meta=json.loads(path.with_suffix(".manifest.json").read_text())
        conv=meta["conversation_id"]
        key=(meta.get("final",False),meta["cutoff"])
        if conv not in latest or key>latest[conv][0]:
            latest[conv]=(key,path)
    rows = []
    provider_calls = [usage_record(r) for path in directory.glob("provider_attempts.*.jsonl") for r in iter_read(path,live=True) if r["state"] == "FINISHED" and r["component"] == "ingestion"]
    calls_by_workflow=defaultdict(list)
    activity_histories={r["workflow_id"]:r for r in read(directory/"temporal_history_trace.jsonl")}
    for call in provider_calls:
        if call.get("workflow_id"):
            calls_by_workflow[call["workflow_id"]].append(call)
    for _,path in latest.values():
        with gzip.open(path,"rt") as stream:
            data=json.load(stream)
        pg = data["postgres"]
        workflows = {r["aggregate_id"]:r["workflow_id"] for r in pg.get("workflow_dispatches",[]) if r["workflow_type"] == "INGEST"}
        extracted = {r["event_id"]:r for r in pg.get("extractions",[])}
        artifacts={(r["event_id"],r["artifact_key"]):r for r in pg.get("ingest_artifacts",[]) if r["artifact_type"]=="EXTRACTION"}
        for event in pg.get("events", []):
            if event["event_id"] not in index:
                continue
            record = index[event["event_id"]]
            payload = json.loads(event["payload"]) if isinstance(event["payload"], str) else event["payload"]
            versions = [v for v in pg.get("memory_versions",[]) if v.get("source_event_id") == event["event_id"]]
            claims = [v for v in pg.get("memory_claims",[]) if v.get("source_event_id") == event["event_id"]]
            retained = payload.get("turn_pair",{}).get("user",{}).get("content","")
            from .ledger import digest
            calls=calls_by_workflow.get(workflows.get(event["event_id"]),[])
            extraction=extraction_counts(event,artifacts,extracted)
            activities=activity_histories.get(workflows.get(event["event_id"]),{}).get("activities",[])
            activity_timings=defaultdict(float)
            for activity in activities:
                if activity.get("schedule_to_terminal_ms") is not None:
                    activity_timings[activity["activity_type"]]+=activity["schedule_to_terminal_ms"]
            from datetime import datetime
            elapsed = (datetime.fromisoformat(event["processed_at"])-datetime.fromisoformat(event["created_at"])).total_seconds() if event.get("processed_at") else None
            rows.append({**record,"status":event["status"],"raw_retention_verified":digest(retained)==record["content_sha256"],"assistant_empty":not payload.get("turn_pair",{}).get("assistant",{}).get("content"),**extraction,"activity_stage_timings_ms":dict(activity_timings) if activities else None,"activity_timing_status":"TEMPORAL_HISTORY" if activities else "UNAVAILABLE","stage_timings_source":"MODEL_OPERATION_LATENCY_ONLY","new_memory_count":sum(n.get("origin_event_id")==event["event_id"] for n in pg.get("memory_nodes",[])),"persisted_versions":len(versions),"persisted_memory_ids":list(dict.fromkeys(v["memory_id"] for v in versions)),"persisted_claims":len(claims),"retry_count":event["retry_count"],"created_at":event["created_at"],"processed_at":event["processed_at"],"pipeline_latency_seconds":elapsed,"stage_timings_ms":{phase:None if any(c.get("latency_ms") is None for c in calls if c.get("phase")==phase) else sum(c["latency_ms"] for c in calls if c.get("phase")==phase) for phase in {c.get("phase") for c in calls}},"error":event.get("error_message"),**summarize_usage(calls)})
    # Accepted but unfinished events remain visible even without a snapshot.
    present = {r["event_id"] for r in rows}
    rows.extend({**r,"status":"ACCEPTED_NOT_VERIFIED","raw_retention_verified":None} for key,r in index.items() if key not in present)
    return rows


def paired(rows):
    grouped = defaultdict(dict)
    for row in rows:
        if row["benchmark_track"] == "SEARCH_COMPARISON":
            grouped[row["qa_id"]][row["search_method"]] = row
    result = []
    for qa, methods in grouped.items():
        for method,row in methods.items():
            baseline = methods.get("bm25")
            a = (row.get("evidence_recall_at_k") or {}).get("10")
            b = ((baseline or {}).get("evidence_recall_at_k") or {}).get("10")
            result.append({"qa_id":qa,"conversation_id":row["conversation_id"],"method":method,"baseline":"bm25","recall_at_10_delta":a-b if a is not None and b is not None else None,"pair_status":"COMPLETE" if a is not None and b is not None else "UNEVALUABLE"})
    return result


def ingestion_aggregates(rows):
    """Measured event throughput; no inference from literal dialogue dates."""
    from collections import Counter
    from datetime import datetime
    sessions=defaultdict(list)
    for row in rows:
        sessions[(row["conversation_id"],row["session"])].append(row)
    complete={"COMPLETE","GATED_SKIP"}
    spans=[]
    for group in sessions.values():
        if all(r.get("created_at") and r.get("processed_at") and r["status"] in complete for r in group):
            seconds=(max(datetime.fromisoformat(r["processed_at"]) for r in group)-min(datetime.fromisoformat(r["created_at"]) for r in group)).total_seconds()
            if seconds>0:
                spans.append((len(group),seconds))
    elapsed=sum(seconds for _,seconds in spans)
    stages=defaultdict(list)
    for row in rows:
        for stage,ms in (row.get("activity_stage_timings_ms") or {}).items():
            if ms is not None:
                stages[stage].append(ms)
    counts={}
    for field in ["extracted_triplets","normalized_extracted_triplets","new_memory_count","persisted_versions","persisted_claims"]:
        counts[field+"_known_total"]=sum(r.get(field) or 0 for r in rows)
        counts[field+"_unknown_events"]=sum(r.get(field) is None for r in rows)
    return {"accepted_events":len(rows),"status_counts":dict(Counter(r["status"] for r in rows)),"raw_retention_verified_events":sum(r.get("raw_retention_verified") is True for r in rows),"raw_retention_failed_events":sum(r.get("raw_retention_verified") is False for r in rows),"raw_retention_unknown_events":sum(r.get("raw_retention_verified") is None for r in rows),"verified_complete_sessions":len(spans),"throughput_event_denominator":sum(count for count,_ in spans),"event_processing_span_seconds":elapsed,"events_per_second":sum(count for count,_ in spans)/elapsed if elapsed else None,"throughput_scope":"Sum of verified per-session first event acceptance to last event completion. Excludes benchmark QA pauses between sessions and subsequent session commit/consolidation readiness; includes ingestion backlogs and retries.","pipeline_latency_seconds":{f"p{p}":quantile([r.get("pipeline_latency_seconds") for r in rows],p/100) for p in [50,95,99]},"activity_stage_timings_ms":{stage:{"event_denominator":len(values),**{f"p{p}":quantile(values,p/100) for p in [50,95,99]}} for stage,values in stages.items()},**counts}


def paired_aggregates(rows,*,bootstrap_samples=2000):
    pairs=paired(rows)
    result=[]
    for method in ["bm25","vector","hybrid","kg","hybrid_kg"]:
        subset=[r for r in pairs if r["method"]==method]
        valid=[r for r in subset if r["pair_status"]=="COMPLETE"]
        lo,hi=bootstrap_conversations(valid,"recall_at_10_delta",samples=bootstrap_samples)
        result.append({"method":method,"baseline":"bm25","expected_pairs":len(subset),"evaluable_pairs":len(valid),"recall_at_10_delta":sum(r["recall_at_10_delta"] for r in valid)/len(valid) if valid else None,"ci_low":lo,"ci_high":hi,"bootstrap_unit":"conversation","bootstrap_samples":bootstrap_samples})
    return result


def local_scoring_accounting(directory):
    starts,finished={},{}
    duplicates=[]
    for row in read(Path(directory)/"local_scoring_attempts.jsonl"):
        records=starts if row["state"]=="STARTED" else finished
        if row["attempt_id"] in records:
            duplicates.append(row["attempt_id"])
        records[row["attempt_id"]]=row
    orphans=sorted(set(finished)-set(starts))
    return {"model":"FacebookAI/roberta-large","status":"MISMATCH" if duplicates or orphans else "IN_FLIGHT_OR_INTERRUPTED" if set(starts)-set(finished) else "RECONCILED","forward_calls":len(starts),"completed_calls":len(finished),"returned_calls":sum(r["status"]=="RETURNED" for r in finished.values()),"failed_calls":sum(r["status"]=="FAILED" for r in finished.values()),"interrupted_calls":sum(r["status"]=="INTERRUPTED" for r in finished.values()),"duplicate_attempt_ids":duplicates,"orphan_finished_attempt_ids":orphans,"unknown_latency_calls":sum(r.get("latency_ms") is None for r in finished.values()),"input_token_positions":sum(r["input_token_positions"] for r in starts.values()),"unpadded_input_tokens":sum(r.get("unpadded_input_tokens") or 0 for r in starts.values()),"encoder_latency_ms":sum(r.get("latency_ms") or 0 for r in finished.values()),"latency_status":"KNOWN_LOWER_BOUND" if any(r.get("latency_ms") is None for r in finished.values()) else "EXACT","provider_billable_tokens":0,"generated_tokens":0,"count_source":"actual_encoder_tensors","attribution":"submitted encoder tensor positions, including interrupted attempts; shared within each BERTScore batch, not assigned to individual questions"}


def management_rows(rows):
    management = []
    for row in rows:
        if row["benchmark_track"] != "CANONICAL":
            continue
        recall = (row.get("evidence_recall_at_k") or {}).get("10")
        supplied = row.get("packed_fraction_evidence_recalled")
        issues=[]
        if row["status"]!="COMPLETE":
            issues.append("Evaluation pending or block incomplete" if row["status"]=="NOT_EXECUTED" else row["status"]+": "+str((row.get("error") or {}).get("type","see technical trace")) if isinstance(row.get("error"),dict) else row["status"])
        if row.get("question_status") not in {"VALID","NORMALIZED"}:
            issues.append("Evidence annotation: "+str(row.get("question_status")))
        if row.get("provenance_status")=="INCOMPLETE":
            issues.append("Incomplete provenance; exact evidence metrics unavailable")
        if row.get("packed_provenance_status")=="UNAVAILABLE_NO_GENERATION_CONTEXT":
            issues.append("No final generation context; supplied-evidence metric unavailable")
        if row.get("unknown_usage_calls"):
            issues.append("Unknown provider usage; known totals are lower bounds")
        if row.get("estimated_usage_calls"):
            issues.append("Token total includes labelled estimates")
        if row.get("semantic_preservation_status")=="AUDIT_ERROR":
            issues.append("Semantic audit unavailable")
        expected_answer=row["gold_answer"] if row["category_id"]!=5 else "Unanswerable from the dialogue (adversarial)"
        retrieval_context = row.get("retrieval_context") or {
            "postgresql": [], "neo4j": [], "redis": [], "frontier_calls": [],
            "answer_mode": row.get("answer_mode"), "trace_id": row.get("trace_id"),
        }
        retrieval_context = {**retrieval_context, "trace_archive": row.get("trace_archive")}
        management.append(dict(zip(MANAGEMENT,[row["conversation_id"],row["category_name"],row["question"],expected_answer,row["prediction"],retrieval_context,100*row["official_locomo_f1"],100*recall if recall is not None else None,100*supplied if supplied is not None else None,row.get("semantic_preservation_status"),row.get("terminal_stage"),row.get("total_tokens"),row["total_latency_ms"]/1000 if row.get("total_latency_ms") is not None else None,row["status"],"; ".join(issues)],strict=True)))
    return management


def generate(directory, provisional=True):
    directory = Path(directory)
    manifest = json.loads((directory/"run_manifest.json").read_text())
    paired_source = json.loads((directory/"paired_source.json").read_text()) if (directory/"paired_source.json").exists() else None
    rows = denominator_rows(manifest, completed_rows(directory))
    bootstrap_samples=200 if provisional else 2000
    agg = aggregates(rows,bootstrap_samples=bootstrap_samples)
    paired_agg=paired_aggregates(rows,bootstrap_samples=bootstrap_samples)
    accounting_result = accounting(directory,verify_archives=not provisional)
    local_accounting=local_scoring_accounting(directory)
    atomic(directory/"local_scoring_accounting.json",local_accounting)
    atomic(directory/"accounting.json", accounting_result)
    native = [r for r in rows if r["benchmark_track"] != "SEARCH_COMPARISON"]
    fields=list(dict.fromkeys([*TECHNICAL,*(key for row in rows for key in row)]))
    csv_write(directory/"per_question.csv", native,fields)
    csv_write(directory/"search_comparison.csv", [r for r in rows if r["benchmark_track"] == "SEARCH_COMPARISON"],fields)
    csv_write(directory/"aggregate.csv", agg)
    csv_write(directory/"paired_comparison.csv", paired(rows))
    csv_write(directory/"paired_aggregate.csv",paired_agg)
    ingested=ingestion_rows(directory)
    csv_write(directory/"ingestion.csv", ingested)
    activity_rows=[{"run_id":manifest["run_id"],**{("workflow_run_id" if k=="run_id" else k):v for k,v in history.items() if k!="activities"},**activity} for history in read(directory/"temporal_history_trace.jsonl") for activity in history["activities"]]
    csv_write(directory/"activity_timings.csv",activity_rows)
    audit_rows=read(directory/"semantic_audit.jsonl")
    csv_write(directory/"semantic_audit.csv",audit_rows,list(dict.fromkeys(["audit_id","qa_id","status","semantic_preservation_status","total_tokens","known_total_tokens","unknown_usage_calls",*(key for row in audit_rows for key in row)])))
    embedding_events,cache_events=[],[]
    for path in [*directory.glob("embedding_trace.*.jsonl"),directory/"trace.jsonl",directory/"ingestion_trace.jsonl",*directory.glob("ingestion_trace.*.jsonl")]:
        for trace in iter_read(path,live=True):
            for event in trace.get("events",[]):
                if event["kind"]=="embedding":
                    embedding_events.append({"count":event["count"],"latency_ms":event["latency_ms"],"component":trace["component"]})
                elif event["kind"]=="embedding_cache" and event.get("function")=="get":
                    cache_events.append({"result":True if event.get("result") is not None else None,"component":trace["component"]})
    component_embeddings={}
    for component in {e["component"] for e in [*embedding_events,*cache_events]}:
        calls=[e for e in embedding_events if e["component"]==component]
        caches=[e for e in cache_events if e["component"]==component]
        component_embeddings[component]={"embedding_requests":sum(e["count"] for e in calls),"embedding_latency_ms":sum(e["latency_ms"] for e in calls),"cache_hits":sum(e["result"] is not None for e in caches),"cache_misses":sum(e["result"] is None for e in caches),"cache_hit_ratio":sum(e["result"] is not None for e in caches)/len(caches) if caches else None}
    atomic(directory/"lifecycle_metrics.json",{"ingestion":ingestion_aggregates(ingested),"embedding_requests":sum(e["count"] for e in embedding_events),"embedding_latency_ms":sum(e["latency_ms"] for e in embedding_events),"embedding_cache_hits":sum(e.get("result") is not None for e in cache_events),"embedding_cache_misses":sum(e.get("result") is None for e in cache_events),"cache_hit_ratio":sum(e.get("result") is not None for e in cache_events)/len(cache_events) if cache_events else None,"embeddings_by_component":component_embeddings,"model_accounting_by_component":accounting_result["components"],"semantic_audit_count":len(read(directory/"semantic_audit.jsonl")),"scoring_provider_tokens":0,"scoring_local_embeddings_separate":True})
    management = management_rows(rows)
    csv_write(directory/"management.csv",management,MANAGEMENT)
    from .descriptive import generate_details
    descriptive = generate_details(directory, rows=rows, management=management)
    executed = sum(r["status"] != "NOT_EXECUTED" for r in rows)
    verified = list((directory/"verified_blocks").glob("*.json")) if (directory/"verified_blocks").exists() else []
    audits_ready = all(r.get("semantic_preservation_status") in {"PRESERVED","PARTIAL","LOST","CONTRADICTED","NOT_EVALUABLE","AUDIT_ERROR"} for r in rows if r["benchmark_track"] == "CANONICAL")
    secondary_ready = all(r.get("secondary_metric_status") in {"COMPLETE","NOT_APPLICABLE_ADVERSARIAL","OPERATIONAL_FAILURE"} for r in native)
    selected=select(load(),manifest["profile"])
    expected_blocks={f"{c.id}-{e.cutoff}-{e.variant in {'completed','annotation_limited'}}.json" for c in load() for e in evaluations(c,selected)}
    verified_names=set()
    for path in verified:
        proof=json.loads(path.read_text())
        if proof.get("status")=="VERIFIED" and proof.get("before")==proof.get("after"):
            verified_names.add(path.name)
    complete=executed==len(rows) and accounting_result["status"]=="RECONCILED" and local_accounting["status"]=="RECONCILED" and expected_blocks<=verified_names and (directory/"validation.json").exists() and (directory/"stack_verified.json").exists() and audits_ready and secondary_ready
    errors=sum(r["status"] not in {"COMPLETE","NOT_EXECUTED"} for r in rows)
    audit_errors=sum(r.get("semantic_preservation_status")=="AUDIT_ERROR" for r in rows if r["benchmark_track"]=="CANONICAL")
    status="PROVISIONAL" if provisional else ("COMPLETE_WITH_ERRORS" if errors or audit_errors else "COMPLETE") if complete else "INCOMPLETE"
    if not provisional and manifest.get("report_schema_version", 1) >= 2 and descriptive["observed_execution_status"] != "CAPTURE_COMPLETE":
        status = "INCOMPLETE"
    summary = {"status":status,"run_id":manifest["run_id"],"expected":len(rows),"executed":executed,"remaining":len(rows)-executed,"question_errors":errors,"semantic_audit_errors":audit_errors,"accounting":accounting_result["status"],"verified_blocks":len(verified_names),"expected_verified_blocks":len(expected_blocks),"bootstrap_samples":bootstrap_samples,"limitations":LIMITATIONS,
               "report_schema_version": descriptive["report_schema_version"],
               "descriptive_report_status": descriptive["observed_execution_status"]}
    if paired_source:
        summary["ingestion_scope"] = paired_source["scope"]
        summary["ingestion_source_run"] = paired_source["source_run"]
        summary["ingestion_accounting"] = paired_source["ingestion_accounting"]
        summary["consolidation_observation"] = paired_source.get("consolidation_observation")
    atomic(directory/"summary.json",summary)
    overall = [r for r in agg if r["dimension"] == "overall"]
    text = f"# LoCoMo lifecycle benchmark — {status}\n\nRun `{manifest['run_id']}`. Executed **{executed}/{len(rows)}** expected evaluations. Missing and failed questions remain in the denominator.\n\n"
    if paired_source:
        text += "Frozen retrieval replay. Ingestion measurements and consumption remain in source run `" + paired_source["source_run"] + "`; they are not repeated or included in this run’s token totals.\n\n"
        if paired_source.get("consolidation_observation", {}).get("status") == "NO_OVERVIEWS_OBSERVED":
            text += "No canonical overviews were present in the reused checkpoints. This replay does not measure consolidation quality.\n\n"
    text += "| Track | Completed / expected | Errors | Official score | Recall@10 |\n|---|---:|---:|---:|---:|\n"
    for r in overall:
        text += f"| {r['track']} | {r['completed']}/{r['expected']} | {r['errors']} | {100*r['official_score']:.2f}%" if r['official_score'] is not None else f"| {r['track']} | {r['completed']}/{r['expected']} | {r['errors']} | N/A"
        text += f" | {100*r['evidence_recall_at_10']:.2f}% |\n" if r['evidence_recall_at_10'] is not None else " | N/A |\n"
    recall_denominators="; ".join(f"{r['track']}: {r['evidence_metric_denominator']}/{r['expected']}" for r in overall)
    recall_note="Recall@10 evaluable rows: "+recall_denominators+". Unknown provenance and non-positive annotations are excluded from exact recall, with explicit statuses."
    text+="\n"+recall_note+"\n"
    text += "\n## Category results\n\n| Track | Category | Completed / expected | Errors | Official score | 95% conversation bootstrap CI | Recall@10 | p95 seconds |\n|---|---|---:|---:|---:|---:|---:|---:|\n"
    category_rows=[r for r in agg if r["dimension"]=="category"]
    def percent(value):
        return f"{100*value:.2f}%" if value is not None else "N/A"
    for r in category_rows:
        text+=f"| {r['track']} | {r['group']} | {r['completed']}/{r['expected']} | {r['errors']} | {percent(r['official_score'])} | {percent(r['official_ci_low'])} – {percent(r['official_ci_high'])} | {percent(r['evidence_recall_at_10'])} | {r['latency_p95_ms']/1000:.2f} |\n" if r['latency_p95_ms'] is not None else f"| {r['track']} | {r['group']} | {r['completed']}/{r['expected']} | {r['errors']} | {percent(r['official_score'])} | {percent(r['official_ci_low'])} – {percent(r['official_ci_high'])} | {percent(r['evidence_recall_at_10'])} | N/A |\n"
    text+="\n## Paired search comparisons\n\nDeltas are percentage points versus BM25 on the same questions with evaluable exact provenance.\n\n| Method | Evaluable / expected pairs | Recall@10 delta | 95% conversation bootstrap CI |\n|---|---:|---:|---:|\n"
    for r in paired_agg:
        text+=f"| {r['method']} | {r['evaluable_pairs']}/{r['expected_pairs']} | {percent(r['recall_at_10_delta'])} | {percent(r['ci_low'])} – {percent(r['ci_high'])} |\n"
    text+=f"\nBootstrap seed 42; {bootstrap_samples} samples. Provisional reports use 200 samples; final reports use 2,000. Full coverage with operational errors is labelled COMPLETE_WITH_ERRORS, with failed scores retained in the denominator.\n"
    text += "\n[Management CSV](management.csv) · [Per-question CSV](per_question.csv) · [Search comparisons](search_comparison.csv) · [Ingestion](ingestion.csv) · [Worker activities](activity_timings.csv) · [Semantic audits](semantic_audit.csv) · [Aggregates](aggregate.csv) · [Paired comparisons](paired_comparison.csv) · [Accounting](accounting.json) · [Manifest](run_manifest.json) · [Data dictionary](data_dictionary.md)\n\n"
    text += "## Accounting\n\n" + json.dumps(accounting_result,indent=2) + "\n\n## Local evaluation encoder accounting\n\n" + json.dumps(local_accounting,indent=2)+"\n\n## Limits\n\n" + "\n".join("- "+v for v in LIMITATIONS) + "\n"
    (directory/"summary.md").write_text(text)
    links = " ".join(f'<a href="{name}">{html.escape(name)}</a>' for name in ["management.csv","per_question.csv","search_comparison.csv","ingestion.csv","activity_timings.csv","semantic_audit.csv","aggregate.csv","accounting.json","run_manifest.json","data_dictionary.md"])
    cells = "".join("<tr>"+"".join(f"<td>{html.escape(str(r.get(k,'')))}</td>" for k in ["track","completed","expected","errors","official_score","evidence_recall_at_10"])+"</tr>" for r in overall)
    category_cells="".join("<tr>"+"".join(f"<td>{html.escape(str(r.get(k,'')))}</td>" for k in ["track","group","completed","expected","errors","official_score","official_ci_low","official_ci_high","evidence_recall_at_10","latency_p95_ms"])+"</tr>" for r in category_rows)
    pair_cells="".join("<tr>"+"".join(f"<td>{html.escape(str(r.get(k,'')))}</td>" for k in ["method","expected_pairs","evaluable_pairs","recall_at_10_delta","ci_low","ci_high"])+"</tr>" for r in paired_agg)
    (directory/"summary.html").write_text('<!doctype html><meta charset="utf-8"><title>Engram LoCoMo benchmark</title><style>body{font:16px system-ui;max-width:1100px;margin:40px auto}td,th{padding:10px;border:1px solid #ddd}table{border-collapse:collapse}a{margin-right:10px}</style>'+f"<h1>Engram benchmark: {status}</h1><p>{executed}/{len(rows)} evaluations executed.</p><p>{links}</p><table><tr><th>Track</th><th>Completed</th><th>Expected</th><th>Errors</th><th>Official score (0–1)</th><th>Recall@10 (0–1)</th></tr>{cells}</table><h2>Limitations</h2><ul>"+"".join("<li>"+html.escape(v)+"</li>" for v in LIMITATIONS)+"</ul>")
    with (directory/"summary.html").open("a") as stream:
        if paired_source:
            stream.write("<p>Frozen retrieval replay. Ingestion measurements remain in source run <code>" + html.escape(paired_source["source_run"]) + "</code>; ingestion consumption is not repeated in these totals.</p>")
            if paired_source.get("consolidation_observation", {}).get("status") == "NO_OVERVIEWS_OBSERVED":
                stream.write("<p>No canonical overviews were present in the reused checkpoints. This replay does not measure consolidation quality.</p>")
        stream.write("<p>"+html.escape(recall_note)+"</p>")
        stream.write(f"<h2>Category results</h2><table><tr><th>Track</th><th>Category</th><th>Completed</th><th>Expected</th><th>Errors</th><th>Official score</th><th>CI low</th><th>CI high</th><th>Recall@10</th><th>p95 ms</th></tr>{category_cells}</table><h2>Paired search comparisons</h2><p>Recall deltas versus BM25 on matching evaluable questions; scores use 0–1.</p><table><tr><th>Method</th><th>Expected pairs</th><th>Evaluable pairs</th><th>Recall delta</th><th>CI low</th><th>CI high</th></tr>{pair_cells}</table><p>Seed 42; {bootstrap_samples} conversation bootstrap samples.</p>")
    fields = list(dict.fromkeys(k for row in rows for k in row))
    (directory/"data_dictionary.md").write_text("# Data dictionary\n\nUnavailable scalars are empty CSV cells. Arrays/maps are JSON strings. Scores use 0–1 in technical CSVs and percentages in management.csv. `NOT_EXECUTED` is incomplete work, not a measured answer. Query tokens include every observed attempt, planning, answerability and generation; cached input and reasoning are subsets. Known totals do not claim to include unknown billed failures. Dialogue IDs are deduplicated in actual returned order. Precision@K uses denominator K even for short lists. Session context ordering is chronological; canonical discovery/ranking is relevance-based.\n\n"+"\n".join(f"- `{k}`" for k in fields)+"\n")
    descriptions={"activity_stage_timings_ms":"Native activity schedule-to-terminal durations from read-only Temporal histories, including queueing/backoff. See activity_timings.csv for observed last-attempt duration and retries; earlier retry durations may be folded by the server.","stage_timings_ms":"Observed model-operation latency within an ingestion stage. Excludes non-model work; distinct from full Temporal activity durations.","extracted_triplets":"Raw native canonical extract-v1 triplet count, with legacy fallback only when a legacy record exists. Missing artifacts remain unavailable.","normalized_extracted_triplets":"Typed canonical typed-v1 triplet count after native normalization; may differ from raw extraction.","new_memory_count":"Canonical memory_nodes whose origin_event_id is this accepted event. Distinct from versions and claim counts.","packed_provenance_status":"EXACT or INCOMPLETE for observed supplied model context. UNAVAILABLE_NO_GENERATION_CONTEXT excludes earlier failed sufficiency contexts.","generation_input_tokens":"Provider input tokens from answer-producing operations, including a terminal session sufficiency call that also produces the answer.","generation_output_tokens":"Provider output tokens from those same answer-producing operations; reasoning remains a subset.","total_tokens":"Recorded input+output over every query submission, including labelled estimates when provider usage is absent. Empty when any attempt has unknown usage; known_total_tokens excludes estimates.","known_total_tokens":"Verified provider input+output subtotal, including earlier interrupted submissions of this evaluation. Excludes estimates and unknown billing; lifecycle totals also retain attempts without result rows.","evidence_recall_at_k":"Fraction of valid positive gold dialogue IDs in verified delivery order at 1/3/5/10/20; unavailable when provenance is incomplete.","packed_fraction_evidence_recalled":"Fraction of required gold dialogue IDs included in the supplied model context, not a claim of actual answer attribution.","routing_correct":"Source-location compatibility based on actual required session reads or equivalent canonical evidence; not learned-classifier accuracy.","semantic_preservation_status":"Separate validated model audit of sourced canonical artifacts versus gold dialogue; not official answer accuracy.","provenance_coverage":"Fraction of returned source blocks/rows with exact original-dialogue mappings; summaries with session-only lineage count as incomplete.","retrieval_lower_bound":"Coverage of exactly mapped returned evidence even if other delivered material has incomplete lineage; labelled diagnostic lower bound.","official_locomo_f1":"Unchanged official LoCoMo scorer; missing/operationally failed answers remain zero in complete denominators.","total_latency_ms":"Native engine elapsed time after dependency setup; excludes external service/container startup.","system_prompt_tokens":"Native tokenizer estimate over actual provider instructions, including native format suffix; not model-returned section accounting.","raw_question_tokens":"Native cl100k_base tokenizer estimate; actual model totals use provider usage.","terminal_stage":"Last entered native cascade stage, distinct from executed stores and answer-support attribution.","verification_rejection_count":"Candidates removed per observed native verification call, including deduplication. Reasons remain unavailable without native reason codes.","packing_omitted_count":"Rows omitted per observed packing call; repeated calls remain counted. Dialogue-level coverage deduplicates delivered IDs separately.","resubmission_count":"Earlier evaluation attempts retained during resume, distinct from SDK retries within one model operation.","read_source_levels":"Successful observed source-store reads at L1/L2. Empty tool invocations do not count as source reads.","estimated_total_tokens":"Explicit tokenizer estimates, excluded from verified provider subtotals. Unknown billed failures are not estimated.","packing_latency_ms":"Time in native packing functions; may be nested within retrieval/build durations and must not be added blindly to total latency."}
    descriptions["retrieval_context"] = ("Source-labelled PostgreSQL, Neo4j and Redis reads with "
        "bounded previews, operation read counts, distinct-result counts and raw-result hashes, separate from "
        "the exact context passed to each frontier call. The management CSV adds the lossless trace "
        "archive path; a returned candidate is not necessarily supplied to the model.")
    with (directory/"data_dictionary.md").open("a") as stream:
        stream.write("\n## Key field meanings\n\n"+"\n".join(f"- `{k}`: {v}" for k,v in descriptions.items())+"\n")
        stream.write("\n## Descriptive reports\n\nquestion_report.html links full per-evaluation SDK inputs, raw outputs, store returns and evidence. "
                     "model_calls.csv contains one row per attempt; management_compact.csv provides bounded spreadsheet previews. "
                     "Known Query Tokens excludes estimates and unknown billing. Answer-producing tokens include retries of observed answer-producing operations. "
                     "Final Answer Known Tokens requires verified answer linkage; Core/Frontier roles remain separate even for identical model names. "
                     "See report_manifest.json for capture and usage reconciliation status.\n")
    for filename in ("summary.md", "summary.html"):
        with (directory / filename).open("a") as stream:
            stream.write("\n\n[Descriptive question report](question_report.html) · [Spreadsheet CSV](management_compact.csv) · [Model calls](model_calls.csv)\n" if filename.endswith(".md") else
                         '<p><a href="question_report.html">Descriptive question report</a> · <a href="management_compact.csv">Spreadsheet CSV</a> · <a href="model_calls.csv">Model calls</a></p>')
    (directory/"level_map.md").write_text("# Native level map\n\nL0: eligible parametric answer. L1: current session cache/PostgreSQL originals. L2: summaries as hints plus past PostgreSQL originals. L3_FACTS: canonical PostgreSQL facts. L3_VECTOR: lexical/vector discovery plus PostgreSQL hydration. L3_GRAPH: bounded Neo4j paths plus PostgreSQL verification. Config L4 enables the complete cascade. Internal RetrievalRoute L1…L4 are separate capability names; router_selected_level retains that internal name.\n")
    return summary
