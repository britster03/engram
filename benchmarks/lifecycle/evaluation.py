"""Turn native traces into auditable per-question measurements."""
from __future__ import annotations

import json
import re
import time
from collections import defaultdict

from .dataset import CATEGORIES
from .ledger import safe, digest
from .metrics import evidence_metrics, compatible, TOP_K, unique
from .observer import summarize_usage, tokens
from .retrieval_report import question_retrieval
from .scoring import score


def fusion_participation(trace):
    """Count channels in the actual fusion input, excluding late results."""
    contributions={"bm25":[],"vector":[]}
    for event in trace.events:
        if event["kind"]!="fusion" or not event.get("inputs"):
            continue
        eligible=set((event.get("result") or {}).keys())
        channels=event["inputs"][0]
        for name in contributions:
            contributions[name].extend(str(row["memory_id"]) for row in channels.get(name,[]) if str(row.get("memory_id")) in eligible)
    return {name:unique(ids) for name,ids in contributions.items()}


def path_bundle(path):
    if any(not c.get("claim_id") for c in path["claims"]):
        return None
    return "path:"+":".join(sorted(str(c["claim_id"]) for c in path["claims"]))


def read_source_levels(trace):
    return unique(e.get("stage") for e in trace.events if e["kind"]=="store" and "result" in e and e.get("stage") in {"L1","L2"} and e.get("store") in {"postgres_conversations","postgres_canonical","redis_sessions"})


def context_evidence(provenance, trace, metadata, *, retrieval_only=False):
    retrieved, source_levels, covered, total = [], [], 0, 0
    contexts = []
    canonical = []
    source_rows=defaultdict(set)
    source_dialogues=defaultdict(set)
    def identity(row):
        return (str(row.get("memory_id")),str(row.get("claim_id")),str(row.get("version_id")))
    for event in trace.events:
        if event["kind"] == "tool":
            result = event.get("result")
            if isinstance(result, dict) and "text" in result:
                ids, exact, count = provenance.context(result)
                contexts.append((result, ids, exact, event["level"]))
                retrieved.extend(ids)
                total += count
                covered += count if exact else 0
            elif isinstance(result, list) and result and isinstance(result[0], list):
                ids, n, count = provenance.rows(result[0])
                canonical.extend(ids)
                retrieved.extend(ids)
                covered += n
                total += count
                source_rows[event["level"]].update(identity(row) for row in result[0])
                source_dialogues[event["level"]].update(ids)
            else:
                continue
            if result.get("text") if isinstance(result,dict) else result[0]:
                source_levels.append(event["level"])
        elif event["kind"] == "comparison_discovery":
            ids, n, count = provenance.rows(event.get("rows", []))
            canonical.extend(ids)
            retrieved.extend(ids)
            covered += n
            total += count
            if event.get("rows"):
                source_levels.append("L3_VECTOR")
                source_rows["L3_VECTOR"].update(identity(row) for row in event["rows"])
                source_dialogues["L3_VECTOR"].update(ids)
    packed, packed_covered, packed_total, supplied = [], 0, 0, []
    models = [e for e in trace.events if e["kind"] == "model_context" and e.get("phase") in {None,"generation"}]
    if metadata.get("answer_mode") in {"deterministic-canonical","evidence-terminal"}:
        # A prior NEED_MORE attempt may have used a generation context, but
        # the terminal product answer was produced without that model call.
        models=[]
    final_msc = models[-1].get("text", "") if models else ""
    builds=[e for e in trace.events if e["kind"]=="context" and e.get("result")==final_msc]
    original=(builds[-1].get("arguments",{}).get("session_context") or "") if builds else ""
    retained=""
    suffix_unmapped=False
    marker="## Session context (not canonical memory)\n"
    if original and final_msc.startswith(marker):
        retained=final_msc[len(marker):].split("\n\n## Retrieval assessment\n",1)[0]
        retained=re.sub(r"^\[… truncated to \d+ tokens …\]\n","",retained)
        if not original.endswith(retained):
            retained=""
            suffix_unmapped=True
    native_cutoff=len(original)-len(retained)
    position=0
    for context, ids, exact, level in contexts:
        text=context["text"]
        start=original.find(text,position) if text and original else -1
        if start>=0:
            position=start+len(text)
        if text and text in final_msc:
            packed.extend(ids)
            packed_total += 1
            packed_covered += exact
            supplied.append(level)
        elif text and retained:
            if start<0:
                continue
            if start+len(text)<=native_cutoff:
                continue
            kept,exact,count=provenance.context_tail(context,max(0,native_cutoff-start))
            packed.extend(kept)
            packed_total+=count
            packed_covered+=count if exact else 0
            supplied.append(level)
    packing = [e for e in trace.events if e["kind"] == "packing" and "result" in e]
    if packing and (models or retrieval_only) and (metadata.get("answer_mode") not in {"current-session", "past-session", "parametric"}):
        ids, n, count = provenance.rows(packing[-1]["result"])
        packed.extend(ids)
        packed_total += count
        packed_covered += n
        packed_keys={identity(row) for row in packing[-1]["result"]}
        supplied.extend(level for level,keys in source_rows.items() if keys.intersection(packed_keys))
    packed_status="UNAVAILABLE_NO_GENERATION_CONTEXT" if not models and not retrieval_only else "EXACT" if packed_covered==packed_total and not suffix_unmapped else "INCOMPLETE"
    quality_packs = [e for e in trace.events if e["kind"] == "quality_packing" and "result" in e]
    from engram.retrieval.sufficiency import POLICY_VERSION
    envelope_policies = {"requirement-support-v1", "requirement-support-v2", POLICY_VERSION}
    exact_render = metadata.get("answer_mode") == "deterministic-canonical" and metadata.get("quality_policy") in envelope_policies
    if (models or exact_render) and metadata.get("quality_policy") in envelope_policies:
        packed, packed_total, packed_covered, supplied = [], 0, 0, []
        try:
            if exact_render:
                items = quality_packs[-1]["result"][0] if quality_packs else []
                used = {c["evidence_id"] for c in metadata.get("declared_support", [])}
                items = [i for i in items if i["id"] in used]
                if not items:
                    raise ValueError("unobserved deterministic evidence")
            else:
                items, _ = json.JSONDecoder().raw_decode(final_msc)
            if not isinstance(items, list):
                raise ValueError("invalid evidence envelope")
            for item in items:
                source = item.get("source") or {}
                packed_total += 1
                if item.get("source_kind") == "conversation_record":
                    dia = provenance.events.get(source.get("event_id"))
                    if not dia and item.get("role") == "user":
                        dia = provenance.turn_ids.get((source.get("session_id"), source.get("turn_idx")))
                    if dia:
                        packed.append(dia)
                        packed_covered += 1
                        # An original loaded through a canonical discovery hit
                        # retains that observed channel's source lineage.
                        supplied.extend(level for level, ids in source_dialogues.items() if dia in ids)
                    supplied.extend(level for context, _, _, level in contexts
                                    if any(s.get("event_id") == source.get("event_id") if source.get("event_id") else
                                           (s.get("session_id"), s.get("turn_idx")) == (source.get("session_id"), source.get("turn_idx"))
                                           for s in context.get("sources", [])))
                else:
                    ids, exact = provenance.row(source)
                    packed.extend(ids)
                    packed_covered += exact
                    supplied.extend(level for level, keys in source_rows.items()
                                    if identity(source) in keys)
            packed_status = "EXACT" if packed_total == packed_covered else "INCOMPLETE"
        except (ValueError, TypeError, KeyError):
            packed_status = "INCOMPLETE"
    return {"retrieved_ids": unique(retrieved), "packed_evidence_ids": unique(packed), "canonical_evidence_ids": unique(canonical), "retrieval_source_levels": unique(source_levels), "supplied_context_levels": unique(supplied), "provenance_coverage": covered/total if total else 1.0, "provenance_status": "EXACT" if covered == total else "INCOMPLETE", "packed_provenance_status":packed_status}


def make_row(evaluation, trace, provenance, result=None, *, run_id, git_sha, config_hash, error=None, comparison=None):
    q = evaluation.question
    metadata = result.retrieval_metadata.to_dict() if result else {}
    prediction = result.answer if result else ""
    status = "ERROR" if error else "COMPLETE"
    if metadata.get("stop_reason") in {"dependency-unavailable", "query-deadline"}:
        status = "DEPENDENCY_ERROR"
    if metadata.get("answer_mode") in {"unavailable", "provider-unavailable"} or any(e["kind"] == "provider_error" and e.get("phase") == "generation" for e in trace.events) and "temporarily unavailable" in prediction:
        status = "GENERATION_ERROR"
    info = context_evidence(provenance, trace, metadata,retrieval_only=comparison is not None)
    if comparison is not None:
        rows, packed, diagnostics = comparison
        ids, n, count = provenance.rows(rows)
        packed_ids, pn, pc = provenance.rows(packed)
        info.update(retrieved_ids=ids, packed_evidence_ids=packed_ids, canonical_evidence_ids=ids, provenance_coverage=n/count if count else 1.0, provenance_status="EXACT" if n == count else "INCOMPLETE", packed_provenance_status="EXACT" if pn == pc else "INCOMPLETE")
    else:
        diagnostics = {}
        for event in trace.events:
            if event["kind"] == "tool" and event.get("function") == "semantic_memory_search":
                diagnostics = event.get("diagnostics", {})
    if comparison is not None and any(value not in {"ok","empty","unsupported"} for value in diagnostics.get("retrievers",{}).values()):
        status = "RETRIEVAL_ERROR"
    retrieval_complete=status in {"COMPLETE","GENERATION_ERROR"}
    positive = q.positive and retrieval_complete
    before = evidence_metrics(q.evidence, info["retrieved_ids"], evaluable=positive and info["provenance_status"] == "EXACT")
    after = evidence_metrics(q.evidence, info["packed_evidence_ids"], evaluable=positive and info["packed_provenance_status"] == "EXACT")
    lower_before = evidence_metrics(q.evidence, info["retrieved_ids"], evaluable=positive)
    lower_after = evidence_metrics(q.evidence, info["packed_evidence_ids"], evaluable=positive and info["packed_provenance_status"]!="UNAVAILABLE_NO_GENERATION_CONTEXT")
    usage = summarize_usage(trace.calls)
    phases = defaultdict(float)
    for event in trace.events:
        if event["kind"] in {"provider_result", "provider_error"}:
            phases[event.get("phase", "unknown")] += event.get("latency_ms", 0)
    tool_latency = sum(e.get("latency_ms", 0) for e in trace.events if e["kind"] in {"tool", "comparison_discovery"})
    fusion = [e for e in trace.events if e["kind"] == "fusion"]
    contributions=fusion_participation(trace)
    components = [name for name, ids in contributions.items() if ids]
    plans = [e["result"] for e in trace.events if e["kind"] == "planning" and "result" in e]
    row = {"run_id": run_id, "git_sha": git_sha, "config_hash": config_hash, "benchmark_track": evaluation.track, "evaluation_id": evaluation.key, "trace_id": trace.identity, "conversation_id": q.conversation_id, "qa_id": q.qa_id, "category_id": q.category, "category_name": CATEGORIES[q.category], "question": q.question, "gold_answer": q.answer, "prediction": prediction if evaluation.track != "SEARCH_COMPARISON" else None, "gold_evidence_ids": q.evidence if q.positive else None, "raw_annotations": q.raw_evidence, "annotation_notes": q.annotation_notes, "gold_evidence_sessions": q.evidence_sessions, "question_status": q.annotation_status, "evaluation_cutoff_session": evaluation.cutoff, "checkpoint_variant": evaluation.variant, "search_method": evaluation.method, "expected_level": evaluation.expected_level, "router_selected_level": plans[0]["primary_route"] if plans else None, "initial_plan": plans[0] if plans else None, "executed_levels": unique(trace.executed), "entered_levels": unique(e["metadata"].get("cascade_depth_reached") for e in trace.events if e["kind"] == "cascade"), "terminal_stage": metadata.get("cascade_depth_reached", trace.stage), "classifier_accuracy": None, "routing_correct": compatible(evaluation.expected_level, trace.executed, q.evidence, info["canonical_evidence_ids"]) if q.positive and evaluation.track == "SESSION_ROUTING" else None, "answer_support_levels": None, "answer_support_observability": "UNAVAILABLE", "answerability": metadata.get("answerability_state"), "answer_mode": metadata.get("answer_mode"), "retrieval_triggered": bool(trace.executed), "search_mode": "hybrid" if len(components) == 2 else components[0] if components else "no_discovery_contribution", "hybrid_search": len(components) == 2, "search_components": components, "channel_contributions": contributions, "channel_statuses": diagnostics.get("retrievers", {}), "fusion_method": "rrf" if fusion else None, "reranker_used": "native_heuristic" if any(e["kind"] == "ranking" for e in trace.events) else "none", "learned_reranker": False, "top_k": TOP_K, **info, "evidence_metrics_status": q.annotation_status if not q.positive else "OPERATIONAL_FAILURE" if status != "COMPLETE" else info["provenance_status"], "evidence_recall_at_k": before.get("recall"), "precision_at_k": before.get("precision"), "ndcg_at_k": before.get("ndcg"), "mrr": before.get("mrr"), "any_evidence_recalled": before.get("any"), "all_evidence_recalled": before.get("all"), "fraction_evidence_recalled": before.get("fraction"), "packed_evidence_recall_at_k": after.get("recall"), "packed_precision_at_k": after.get("precision"), "packed_ndcg_at_k": after.get("ndcg"), "packed_mrr": after.get("mrr"), "packed_fraction_evidence_recalled": after.get("fraction"), "retrieval_lower_bound": lower_before, "packed_lower_bound": lower_after, "raw_question_tokens": tokens(q.question), **usage, "routing_latency_ms": phases["tool_planning"]+sum(e.get("latency_ms",0) for e in trace.events if e["kind"]=="planning"), "answerability_latency_ms": phases["answerability"], "retrieval_latency_ms": tool_latency, "generation_latency_ms": phases["generation"], "total_latency_ms": trace.to_dict()["total_ms"], "model_name": unique(c.get("model") for c in trace.calls), "fallback_used": bool(metadata.get("degraded_sources")), "degraded_sources": metadata.get("degraded_sources", []), "status": status, "error": error, "semantic_preservation_status": "PENDING", "stores_queried": unique(e["store"] for e in trace.events if e["kind"] == "store"), "nodes_examined": None, "actual_answer_graph_depth": None}
    graph_calls = [e for e in trace.events if e["kind"] == "store" and e.get("function") == "run_template" and "result" in e]
    paths = [r for e in graph_calls for r in e["result"] if r.get("claims")]
    graph_tools = [e for e in trace.events if e["kind"] == "tool" and e.get("function") == "graph_search" and e.get("result")]
    graph_rows = [r for e in graph_tools for r in e["result"][0]]
    verified_bundles = {b for r in graph_rows for b in r.get("bundle_ids",[])}
    packing = [e for e in trace.events if e["kind"] == "packing" and "result" in e]
    packed_bundles = unique(b for r in (packing[-1]["result"] if packing else []) for b in r.get("bundle_ids", []))
    row.update(requested_graph_depth=max((e.get("inputs", [None, {}])[1].get("hops", 0) for e in graph_calls if len(e.get("inputs", [])) > 1), default=None), returned_path_depths=[len(p["claims"]) for p in paths], returned_paths=len(paths), verified_paths=sum(path_bundle(p) in verified_bundles for p in paths), retained_graph_bundles=packed_bundles, gold_matching_path_depths=[len(p["claims"]) for p in paths if path_bundle(p) in verified_bundles and any(set(provenance.row(claim)[0]).intersection(q.evidence) for claim in p["claims"])] if q.positive else None, ordering_sources={"L1":"chronological session context or PostgreSQL relevance-ranked originals","L2":"PostgreSQL relevance-ranked originals","canonical":"native relevance ranking"},current_session_context_tokens=max((tokens(e["result"].get("text", "")) for e in trace.events if e["kind"] == "tool" and e.get("function") == "current_session_search" and isinstance(e.get("result"),dict)),default=0),historical_context_tokens=max((tokens(e["result"].get("text", "")) for e in trace.events if e["kind"] == "tool" and e.get("function") == "past_session_search" and isinstance(e.get("result"),dict)),default=0),retrieved_context_tokens=metadata.get("total_context_tokens"),reasoning_tokens=usage["total_reasoning_tokens"])
    row.update(evidence_recall_at_k={str(k): before[f"recall_at_{k}"] for k in TOP_K}, precision_at_k={str(k): before[f"precision_at_{k}"] for k in TOP_K}, ndcg_at_k={str(k): before[f"ndcg_at_{k}"] for k in TOP_K}, any_evidence_recalled=before["any_evidence_recalled"], all_evidence_recalled=before["all_evidence_recalled"], fraction_evidence_recalled=before["evidence_fraction"], packed_evidence_recall_at_k={str(k): after[f"recall_at_{k}"] for k in TOP_K}, packed_precision_at_k={str(k): after[f"precision_at_{k}"] for k in TOP_K}, packed_ndcg_at_k={str(k): after[f"ndcg_at_{k}"] for k in TOP_K}, packed_fraction_evidence_recalled=after["evidence_fraction"])
    row["retrieval_status"]="COMPLETE" if retrieval_complete else "FAILED_OR_UNOBSERVED"
    for key in ("quality_policy", "sufficiency_decisions", "evidence_diagnostics", "declared_support", "avoided_model_calls", "stop_reason"):
        row[key] = metadata.get(key)
    row["declared_support_observability"] = "DETERMINISTIC_RENDER" if metadata.get("answer_mode") == "deterministic-canonical" else "MODEL_DECLARATION" if metadata.get("declared_support") else "UNAVAILABLE"
    for phase in ("decomposition", "sufficiency", "output_support"):
        row[phase + "_latency_ms"] = phases[phase]
    row["read_source_levels"]=read_source_levels(trace)
    if q.positive and evaluation.track=="SESSION_ROUTING":
        row["routing_correct"]=compatible(evaluation.expected_level,row["read_source_levels"],q.evidence,info["canonical_evidence_ids"])
    row["evidence_metrics_status"]=q.annotation_status if not q.positive else info["provenance_status"] if retrieval_complete else "OPERATIONAL_FAILURE"
    row["ordering_sources"]=[{"level":e.get("level"),"function":e.get("function"),"ordering":"chronological" if e.get("function")=="current_session_search" else "relevance"} for e in trace.events if e["kind"]=="tool" and e.get("result")]
    verifications=[e for e in trace.events if e["kind"]=="verification" and "result" in e and e.get("inputs") and isinstance(e["inputs"][0],list)]
    packs=[e for e in trace.events if e["kind"]=="packing" and "result" in e and len(e.get("inputs",[]))>1 and isinstance(e["inputs"][1],list)]
    row.update(verification_input_count=sum(len(e["inputs"][0]) for e in verifications),verification_kept_count=sum(len(e["result"]) for e in verifications),verification_rejection_count=sum(len(e["inputs"][0])-len(e["result"]) for e in verifications),verification_rejection_reason_status="UNAVAILABLE_NATIVE_REASON_CODES",packing_input_count=sum(len(e["inputs"][1]) for e in packs),packing_retained_count=sum(len(e["result"]) for e in packs),packing_omitted_count=sum(len(e["inputs"][1])-len(e["result"]) for e in packs),verification_latency_ms=sum(e.get("latency_ms",0) for e in verifications),packing_latency_ms=sum(e.get("latency_ms",0) for e in packs),ranking_latency_ms=sum(e.get("latency_ms",0) for e in trace.events if e["kind"]=="ranking"))
    rejected = [e["result"] for e in trace.events if e["kind"] == "verification_rejection"]
    row["verification_rejection_reasons"] = rejected
    if len(rejected) == row["verification_rejection_count"]:
        row["verification_rejection_reason_status"] = "COMPLETE"
    quality_packs = [e for e in trace.events if e["kind"] == "quality_packing" and "result" in e]
    for event in quality_packs:
        row["packing_input_count"] += len(event["inputs"][0])
        row["packing_retained_count"] += len(event["result"][0])
        row["packing_omitted_count"] += len(event["result"][1])
        row["packing_latency_ms"] += event.get("latency_ms", 0)
    if quality_packs:
        row["retained_graph_bundles"] = unique(b for item in quality_packs[-1]["result"][0] for b in item.get("bundles", []))
    if evaluation.track != "SEARCH_COMPARISON":
        row.update(score(prediction, q.answer, q.category, operational_status=status))
        row["native_abstention"] = any(word in prediction.lower() for word in ["not mentioned", "no information available", "insufficient", "cannot determine", "not enough evidence"])
        if evaluation.track == "CANONICAL":
            row["retrieval_context"] = question_retrieval(
                trace, answer_mode=metadata.get("answer_mode")
            )
    if metadata.get("quality_policy") and evaluation.track != "SEARCH_COMPARISON":
        state, reason = metadata.get("answerability_state"), metadata.get("stop_reason")
        action = ("UNAVAILABLE" if status != "COMPLETE" else
                  "CLARIFY" if reason == "ambiguous-entity-reference" else
                  "CONFLICT" if state == "CONFLICTING_EVIDENCE" else
                  "PARTIAL" if state == "PARTIALLY_ANSWERABLE" else
                  "ANSWER" if state == "ANSWERABLE" else "ABSTAIN")
        row["terminal_action"] = action
        row["native_abstention"] = action == "ABSTAIN"
        row["abstention_diagnostic_source"] = "NATIVE_CONTROLLER_STATE"
    elif evaluation.track != "SEARCH_COMPARISON":
        row["abstention_diagnostic_source"] = "LEGACY_PHRASE_HEURISTIC"
    return safe(row)
