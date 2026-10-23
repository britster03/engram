"""Separate semantic-preservation judgments; never influence native answers."""
from __future__ import annotations

import json
from pathlib import Path

from .ledger import append, digest, safe
from . import ledger
from .observer import Trace, capture, instrument_provider, summarize_usage, error_info


def sourced_artifacts(pg,event_ids):
    supporting={str(r["claim_id"]) for r in pg.get("memory_evidence",[]) if r.get("source_event_id") in event_ids and r.get("claim_id")}
    names={str(r["id"]):r.get("canonical_name") for r in pg.get("memory_nodes",[])}
    artifacts=[{"claim_id":str(r["id"]),"source_event_id":r.get("source_event_id"),"subject_id":str(r.get("subject_id")),"subject_name":names.get(str(r.get("subject_id"))),"predicate":r.get("predicate"),"object_value":r.get("object_value"),"object_entity_id":str(r.get("object_entity_id")) if r.get("object_entity_id") else None,"object_entity_name":names.get(str(r.get("object_entity_id"))),"supporting_event_ids":[e["source_event_id"] for e in pg.get("memory_evidence",[]) if str(e.get("claim_id"))==str(r["id"]) and e.get("source_event_id") in event_ids]} for r in pg.get("memory_claims",[]) if r.get("source_event_id") in event_ids or str(r["id"]) in supporting]
    artifacts += [{"version_id":str(r["id"]),"source_event_id":r.get("source_event_id"),"body":r.get("body"),"abstract":r.get("abstract")} for r in pg.get("memory_versions",[]) if r.get("source_event_id") in event_ids]
    return artifacts


def audit_question(state, evaluation, provenance, directory):
    from engram.config import CoreModelConfig
    from engram.models.providers import build_providers
    q = evaluation.question
    source = [{"dia_id": t.dia_id, "content": t.content} for t in provenance.conversation.turns if t.dia_id in q.evidence]
    if not q.positive:
        return {"qa_id": q.qa_id, "status": q.annotation_status, "semantic_preservation_status": "NOT_EVALUABLE", "total_tokens": 0}
    event_ids = {e for e, dia in provenance.events.items() if dia in q.evidence}
    pg = provenance.data["postgres"]
    artifacts=sourced_artifacts(pg,event_ids)
    identity = digest({"qa_id": q.qa_id, "source": source, "artifacts": artifacts})
    fields = state.cfg.frontier_llm.model_dump()
    cfg = CoreModelConfig(**{k: v for k, v in fields.items() if k in CoreModelConfig.model_fields})
    provider, _ = build_providers(cfg, state.cfg.frontier_llm)
    instrument_provider(provider)
    trace = Trace(f"audit:{identity}", component="semantic_audit")
    payload = {"question": q.question, "expected_answer": q.answer, "original_dialogue": source, "derived_artifacts": artifacts}
    result = {"audit_id": identity, "qa_id": q.qa_id, "artifact_hash": digest(artifacts), "source_event_ids": sorted(event_ids), "source_dialogue_ids": q.evidence, "model": cfg.model_path}
    with capture(trace):
        try:
            response = provider.complete(system_prompt='Evaluate whether the derived artifacts preserve the facts needed to answer the question from the original dialogue. Do not assess the generated answer. Return JSON with status PRESERVED, PARTIAL, LOST, or CONTRADICTED; explanation; citations as a list of {dia_id, quote}; and artifact_ids as a list of claim_id/version_id. Citations must be exact nonempty substrings of original_dialogue content. Use LOST if no artifacts preserve the required fact.', user_prompt=json.dumps(payload, ensure_ascii=False))
            output = response.output
            citations = output.get("citations", [])
            by_id = {s["dia_id"]: s["content"] for s in source}
            artifact_ids = {str(a.get("claim_id") or a.get("version_id")) for a in artifacts}
            if output.get("status") not in {"PRESERVED", "PARTIAL", "LOST", "CONTRADICTED"} or not citations or any(not c.get("quote") or c.get("dia_id") not in by_id or c["quote"] not in by_id[c["dia_id"]] for c in citations) or any(str(a) not in artifact_ids for a in output.get("artifact_ids", [])) or (output.get("status")!="LOST" and not output.get("artifact_ids")):
                raise ValueError("Invalid semantic-audit citations or status")
            result.update(status="COMPLETE", semantic_preservation_status=output["status"], judgment=output)
        except Exception as error:
            result.update(status="ERROR", semantic_preservation_status="AUDIT_ERROR", error=error_info(error))
    result.update(summarize_usage(trace.calls))
    append(Path(directory)/"semantic_audit.jsonl", result)
    archive=getattr(ledger,"append_trace",None)
    if archive is None:
        # A running pre-archive driver can finish its block before reload.
        append(Path(directory)/"trace.jsonl",trace.to_dict())
    else:
        archive(directory,trace)
    return result
