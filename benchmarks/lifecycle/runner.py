"""Persistent isolated driver; the host orchestrator controls writer quiescence."""
from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path

from .adapter import (
    Provenance,
    close_session,
    compare,
    ingest_session,
    readiness,
    readonly_context,
    session_id,
    use_tenant,
)
from .dataset import evaluations, load, select
from .evaluation import make_row
from .infrastructure import assert_isolated
from .ledger import Progress, append, append_trace, atomic, read, safe
from .observer import Trace, capture, error_info, install
from .snapshot import fingerprint, restore, save


def evaluate(state, conversation, request, directory, progress):
    from engram.retrieval.orchestrator import run_query
    from engram.retrieval.planner import AdaptivePlanner
    selected = select(load(), request["profile"])
    block = [e for e in evaluations(conversation, selected) if e.cutoff == request["session"] and (e.variant in {"completed", "annotation_limited"}) == request["final"]]
    path = directory/"checkpoints"/conversation.id/("completed.json.gz" if request["final"] else f"session-{request['session']:03d}.json.gz")
    data = restore(state, path)
    tid = use_tenant(state, conversation.id)
    provenance = Provenance(data, read(directory/"provenance.jsonl"), conversation)
    before = fingerprint(state, tid)
    # The restored session and original ledger cannot contain later sessions.
    allowed = {session_id(conversation.id, s) for s in conversation.sessions if s <= request["session"]}
    if any(r.get("session_id") not in allowed for r in data["postgres"].get("events", []) if r.get("session_id")):
        raise RuntimeError("Future-session leakage in checkpoint")
    existing = read(directory/"result_rows.jsonl")
    keys = [r["evaluation_id"] for r in existing]
    if len(keys) != len(set(keys)):
        raise RuntimeError("Duplicate evaluation rows")
    done = set(keys)
    manifest = json.loads((directory/"run_manifest.json").read_text())
    for evaluation in block:
        if evaluation.key in done:
            continue
        trace = Trace(f"{evaluation.key}:{uuid.uuid4().hex}",
                      evaluation_id=evaluation.key, qa_id=evaluation.question.qa_id)
        append(directory/"evaluation_attempts.jsonl", {"evaluation_id": evaluation.key, "trace_id": trace.identity, "state": "STARTED"})
        result, comparison, error = None, None, None
        progress.update(phase="evaluation",conversation=conversation.id,checkpoint=evaluation.cutoff,current_question=evaluation.question.qa_id,completed=len(done),expected=manifest["expected_evaluations"],remaining=manifest["expected_evaluations"]-len(done))
        engine_started=None
        with capture(trace):
            try:
                with readonly_context(state, tid) as ctx:
                    engine_started=time.perf_counter()
                    if evaluation.track == "SEARCH_COMPARISON":
                        plan = AdaptivePlanner(max_candidates=ctx.cfg.retrieval.max_l1_vector_results).plan(evaluation.question.question, core=None)
                        comparison = compare(ctx, plan, evaluation.method, tid, session_id(conversation.id, evaluation.cutoff))
                    else:
                        result = run_query(ctx, session_id=session_id(conversation.id, evaluation.cutoff), query=evaluation.question.question, on_step=trace.step)
            except Exception as failure:
                error = error_info(failure)
                trace.emit("query_error", error=error)
        engine_ms=(time.perf_counter()-engine_started)*1000 if engine_started is not None else None
        trace.emit("native_query_timing",engine_ms=engine_ms)
        after = fingerprint(state, tid)
        if after != before:
            atomic(directory/"state_mutation.json", {"evaluation_id": evaluation.key, "before": before, "after": after})
            raise RuntimeError("Semantic state changed during query")
        if result and result.retrieval_metadata.answer_mode in {"current-session", "past-session", "parametric"}:
            operations = [e.get("operation_id") for e in trace.events if e["kind"] == "provider_result" and e.get("method") == "answer" and (e.get("result") or {}).get("verdict") == "ANSWER"]
            if operations:
                for call in trace.calls:
                    if call.get("operation_id") == operations[-1]:
                        call["caller_phase"] = call["phase"]
                        call["phase"] = "generation"
                        call["operation_role"] = "answerability_and_generation"
                for event in trace.events:
                    if event.get("operation_id") == operations[-1]:
                        event["caller_phase"] = event.get("phase")
                        event["phase"] = "generation"
                        event["operation_role"] = "answerability_and_generation"
        row = make_row(evaluation, trace, provenance, result, run_id=directory.name, git_sha=manifest["git_sha"], config_hash=manifest["config_hash"], error=error, comparison=comparison)
        row["instrumented_question_wall_ms"]=row["total_latency_ms"]
        row["total_latency_ms"]=engine_ms
        trace.emit("semantic_fingerprint", before=before, after=after, verified=True)
        archived=append_trace(directory, trace)
        row.update(trace_archive=archived["trace_archive"],trace_archive_sha256=archived["trace_archive_sha256"])
        append(directory/"result_rows.jsonl", row)
        append(directory/"evaluation_attempts.jsonl", {"evaluation_id": evaluation.key, "trace_id": trace.identity, "state": "FINISHED", "status": row["status"]})
        done.add(evaluation.key)
        existing.append(row)
        progress.update(phase="evaluation", conversation=conversation.id, checkpoint=evaluation.cutoff, completed=len(done), expected=manifest["expected_evaluations"], remaining=manifest["expected_evaluations"]-len(done), errors=sum(r["status"] != "COMPLETE" for r in existing), known_tokens=sum(r["known_total_tokens"] for r in existing), query_unknown_usage_calls=sum(r["unknown_usage_calls"] for r in existing), current_question=evaluation.question.qa_id)
        if len(done) % 10 == 0 and time.time()-progress.state.get("last_report_at",0)>=300:
            from .report import generate
            report=generate(directory, provisional=True)
            if report["accounting"] == "MISMATCH":
                raise RuntimeError("Provider accounting mismatch")
            progress.update(last_report_at=time.time())
    if request["final"]:
        from .audit import audit_question
        audits = {r["qa_id"] for r in read(directory/"semantic_audit.jsonl")}
        for evaluation in block:
            if evaluation.track == "CANONICAL" and evaluation.question.qa_id not in audits:
                progress.update(phase="semantic_audit", current_question=evaluation.question.qa_id)
                audit_question(state, evaluation, provenance, directory)
    after=fingerprint(state,tid)
    if after!=before:
        atomic(directory/"state_mutation.json",{"conversation":conversation.id,"checkpoint":request["session"],"phase":"block_finalization","before":before,"after":after})
        raise RuntimeError("Semantic state changed during evaluation block")
    atomic(directory/"verified_blocks"/f"{conversation.id}-{request['session']}-{request['final']}.json", {"before": before, "after": after, "evaluations": len(block), "status": "VERIFIED"})


def action(state, request, directory, progress):
    conversations = {c.id: c for c in load()}
    conversation = conversations.get(request.get("conversation"))
    if conversation:
        tid = use_tenant(state, conversation.id)
    operation = request["action"]
    if operation == "resume_info":
        sessions={s:state.control_plane.get_session_state(session_id(conversation.id,s),tenant_id=tid) for s in conversation.sessions}
        present={s:p for s,p in sessions.items() if p is not None}
        latest=max(present,default=0)
        committed=[s for s,p in present.items() if p.get("status")=="COMMITTED"]
        if any(s<latest and s not in committed for s in present):
            raise RuntimeError("Uncommitted older session in resumed ingestion prefix")
        return {"latest_session":latest,"committed_sessions":committed}
    elif operation == "ingest":
        ingest_session(state, conversation, request["session"], directory, progress)
    elif operation == "close":
        close_session(state, conversation, request["session"])
    elif operation == "wait":
        deadline = time.monotonic() + request.get("timeout", 1800)
        while True:
            status = readiness(state, tid)
            if status["ready"]:
                from .activities import collect as collect_activity_history
                temporal=collect_activity_history(state,tid,directory)
                status["temporal_workflows"]=temporal
                status["ready"]=temporal["ready"]
                status["fatal"]=status["fatal"] or temporal["fatal"]
            atomic(directory/"readiness.json", status)
            progress.update(phase="readiness", conversation=conversation.id, checkpoint=request["session"], backlog=status)
            if status["ready"]:
                # Wait for worker journal flush and a stable canonical state.
                first = fingerprint(state, tid)
                time.sleep(2)
                if readiness(state, tid)["ready"] and fingerprint(state, tid) == first:
                    from datetime import datetime, timezone
                    append(directory/"readiness_trace.jsonl",{"conversation":conversation.id,"session":request["session"],"status":"READY","at":datetime.now(timezone.utc).isoformat(),"observed_state":status})
                    return status
            if status["fatal"] or time.monotonic() > deadline:
                save(state,tid,directory/"diagnostics"/conversation.id/"incomplete.json.gz",conversation_id=conversation.id,cutoff=request["session"],final=False,readiness=status,verified=False)
                raise RuntimeError("Ingestion or projection readiness failed")
            time.sleep(2)
    elif operation == "snapshot":
        if not readiness(state, tid)["ready"]:
            raise RuntimeError("Checkpoint ingestion is incomplete")
        path = directory/"checkpoints"/conversation.id/("completed.json.gz" if request.get("final") else f"session-{request['session']:03d}.json.gz")
        save(state, tid, path, conversation_id=conversation.id, cutoff=request["session"], current_session_id=session_id(conversation.id, request["session"]), final=request.get("final", False), original_timestamp=next(t.raw_timestamp for t in conversation.turns if t.session == request["session"]),verified=True,readiness=readiness(state,tid))
        import psycopg
        with psycopg.connect(state.cfg.event_ledger.dsn) as conn:
            size = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        import redis
        cache=redis.Redis.from_url(state.cfg.session_cache.redis_url)
        atomic(directory/"storage.json",{"postgres_database_bytes":size,"redis_memory":cache.info("memory"),"redis_key_count":cache.dbsize(),"scope":"whole isolated stack at latest snapshot; not allocated to individual tenants"})
    elif operation == "evaluate":
        evaluate(state, conversation, request, directory, progress)
    elif operation == "report":
        from .report import generate
        return generate(directory, provisional=request.get("provisional", True))
    elif operation == "phase":
        updates={"phase":request["phase"],"current_question":None}
        if "status" in request:
            updates["status"]=request["status"]
        progress.update(**updates)
    elif operation == "stop":
        return "STOP"
    else:
        raise ValueError(operation)
    return {"status": "COMPLETE"}


def serve(directory):
    assert_isolated()
    install()
    from engram.deps import get_state
    state = get_state()
    directory = Path(directory)
    import importlib.metadata
    atomic(directory/"native_runtime.json", {"models":{"core":safe(state.cfg.core_model),"frontier":safe(state.cfg.frontier_llm)},"retrieval":safe(state.cfg.retrieval),"temporal":safe(state.cfg.temporal),"session":safe(state.cfg.session),"gating":safe(state.cfg.gating),"dependencies":{d.metadata["Name"]:d.version for d in importlib.metadata.distributions()},"provider_attempt_observation":True})
    queue = directory/"commands"
    queue.mkdir(exist_ok=True)
    atomic(queue/"driver_ready.json", {"status": "READY"})
    progress = Progress(directory, phase="ready")
    meta=json.loads((directory/"run_manifest.json").read_text()) if (directory/"run_manifest.json").exists() else {}
    selected=select(load(),meta.get("profile","pilot"))
    progress.update(expected_ingestion_turns=sum(len(c.turns) for c in load() if any(q.qa_id in selected for q in c.questions)))
    try:
        while True:
            requests = sorted(queue.glob("*.request.json"))
            for path in requests:
                response_path = path.with_name(path.name.replace(".request.", ".response."))
                if response_path.exists():
                    continue
                request = json.loads(path.read_text())
                try:
                    result = action(state, request, directory, progress)
                    atomic(response_path, {"status": "COMPLETE", "result": result})
                    if result == "STOP":
                        progress.close("STOPPED")
                        return
                except Exception as error:
                    atomic(response_path, {"status": "INCOMPLETE", "error": error_info(error)})
                    append(directory/"operational_failures.jsonl", {"request": request, "error": error_info(error)})
                    progress.update(status="INCOMPLETE")
            time.sleep(.5)
    finally:
        progress.close("STOPPED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", default="/results")
    serve(parser.parse_args().directory)
