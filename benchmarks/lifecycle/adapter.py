"""Native dependency adapters. All measurement changes stay in this package."""
from __future__ import annotations

import json
import functools
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from . import snapshot
from .ledger import append, safe, digest, read
from .observer import CURRENT, Trace, capture, error_info, tokens


class Observed:
    def __init__(self, wrapped, store):
        self.wrapped, self.store = wrapped, store

    def __getattr__(self, name):
        original = getattr(self.wrapped, name)
        if not callable(original):
            return original
        @functools.wraps(original)
        def call(*args, **kwargs):
            trace = CURRENT.get()
            start = time.perf_counter()
            try:
                result = original(*args, **kwargs)
                if trace is not None:
                    arguments = {k: {"dimensions": len(v), "sha256": digest(v)} if k in {"embedding", "query_embedding"} else v for k, v in kwargs.items()}
                    trace.emit("store", store=self.store, function=name, inputs=args, arguments=arguments, result=result, latency_ms=(time.perf_counter()-start)*1000)
                return result
            except BaseException as error:
                if trace is not None:
                    trace.emit("store", store=self.store, function=name, error=error_info(error), latency_ms=(time.perf_counter()-start)*1000)
                raise
        return call


@contextmanager
def readonly_context(state, tenant_id):
    from engram.deps import make_orchestrator_context
    from engram.storage.postgres import _Connection
    from engram.storage.control_plane import ControlPlaneQueries
    from engram.storage.memory_repository import MemoryRepository, _BoundTransactionStore

    class ReadStore(ControlPlaneQueries):
        def __init__(self, conn):
            self.conn = conn
        def get_conn(self):
            return self.conn
        @contextmanager
        def transaction(self):
            yield self.conn

    class ReadRepository(MemoryRepository):
        def enqueue_overview_regeneration(self, *args, **kwargs):
            trace = CURRENT.get()
            if trace:
                trace.emit("suppressed_write", function="enqueue_overview_regeneration", inputs=args, arguments=kwargs)
            return None

    with psycopg.connect(state.cfg.event_ledger.dsn, row_factory=dict_row) as raw:
        with raw.transaction():
            raw.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            conn = _Connection(raw)
            ctx = make_orchestrator_context(state)
            ctx.control_plane = Observed(ReadStore(conn), "postgres_conversations")
            ctx.memory_repository = Observed(ReadRepository(_BoundTransactionStore(conn), default_tenant_id=tenant_id), "postgres_canonical")
            ctx.neo4j = Observed(state.neo4j, "neo4j")
            ctx.session_cache = Observed(state.session_cache, "redis_sessions")
            ctx.embed = Observed(state.embed, "bge_embeddings")
            yield ctx


def session_id(conversation_id, session):
    return f"locomo-{conversation_id}-s{session:03d}"


def tenant_id(conversation_id):
    return f"locomo-{conversation_id}"


def use_tenant(state, conversation_id):
    from engram.tenancy import set_current_tenant
    tid = tenant_id(conversation_id)
    tenant = state.tenant_registry.get(tid)
    if tenant is None:
        tenant, _ = state.tenant_registry.create(tid)
    set_current_tenant(tenant)
    return tid


def manager_for(state):
    from engram.deps import make_session_manager
    return make_session_manager(state)


def ingest_session(state, conversation, session, directory, progress):
    from engram.session.manager import SessionState, Turn, compact_session
    from engram.api.schemas import IngestRequest, TurnPair, TurnContent
    from engram.api.app import ingest
    tid = use_tenant(state, conversation.id)
    manager = manager_for(state)
    sid = session_id(conversation.id, session)
    payload = state.control_plane.get_session_state(sid, tenant_id=tid)
    current = SessionState.from_payload(payload) if payload else SessionState(sid)
    turns = [t for t in conversation.turns if t.session == session]
    existing = {t.turn_idx for t in current.turns}
    persisted_through=max(existing,default=-1)
    path = Path(directory)/"provenance.jsonl"
    accepted_index={r["dia_id"]:r for r in read(path) if r["conversation_id"]==conversation.id}
    for turn in turns:
        idx = turn.index * 2
        trace = Trace(f"ingest:{conversation.id}:{turn.dia_id}", component="ingestion")
        with capture(trace):
            # Match the native session caller: extraction receives the recent
            # dialogue, and the gate receives native compaction when present.
            # Build it before accepting, without prematurely persisting a turn.
            request_state = replace(current, turns=[*current.turns, Turn("user",turn.content,turn.timestamp,idx)] if idx not in existing else [t for t in current.turns if t.turn_idx <= idx])
            req = IngestRequest(session_id=sid, source="locomo", session_context=request_state.render(max_turns=10),session_summary=current.compacted, turn_pair=TurnPair(user=TurnContent(content=turn.content, timestamp=turn.timestamp, turn_idx=idx), assistant=TurnContent(content="", timestamp=turn.timestamp, turn_idx=idx+1)))
            start = time.perf_counter()
            recorded=accepted_index.get(turn.dia_id)
            event=state.control_plane.get_event(recorded["event_id"],tenant_id=tid) if recorded else None
            if event:
                payload=event["payload"]
                if isinstance(payload,str):
                    payload=json.loads(payload)
                if recorded["content_sha256"]!=digest(turn.content) or event["session_id"]!=sid or payload["turn_pair"]["user"]["content"]!=turn.content:
                    raise RuntimeError("Accepted ingestion record does not match original dialogue")
                accepted={"event_id":recorded["event_id"],"resumed":True}
            else:
                accepted = json.loads(ingest(req).body)
            trace.emit("accepted", **accepted, dia_id=turn.dia_id, session_id=sid, tenant_id=tid, latency_ms=(time.perf_counter()-start)*1000)
            if not event:
                append(path, {"conversation_id": conversation.id, "dia_id": turn.dia_id, "session": session, "session_id": sid, "turn_idx": idx, "event_id": accepted["event_id"], "content_sha256": digest(turn.content), "timestamp": turn.timestamp, "raw_timestamp": turn.raw_timestamp})
            # Earlier accepted turns can be absent because native compaction
            # retained only the tail. Re-add only an acceptance not persisted
            # before the crash; rebuilding compacted turns changes history.
            if idx not in existing and (not event or idx>persisted_through):
                current.turns.append(Turn("user", turn.content, turn.timestamp, idx))
                existing.add(idx)
                persisted_through=max(persisted_through,idx)
                manager._persist(current)
                if manager.needs_compaction(current):
                    trace.stage="SESSION_COMPACTION"
                    compact_session(manager, current, state.core, state.control_plane, tenant_id=tid)
            append(Path(directory)/"ingestion_trace.jsonl", trace.to_dict())
        index={r["event_id"]:r for r in read(path)}
        progress.update(phase="ingestion", conversation=conversation.id, checkpoint=session, completed=len(index),expected=progress.state.get("expected_ingestion_turns",len(conversation.turns)),remaining=progress.state.get("expected_ingestion_turns",len(conversation.turns))-len(index),session_completed=turn.index+1,session_expected=len(turns))
    return current


def close_session(state, conversation, session):
    from engram.session.manager import SessionState, commit_session
    tid = use_tenant(state, conversation.id)
    payload = state.control_plane.get_session_state(session_id(conversation.id, session), tenant_id=tid)
    current = SessionState.from_payload(payload)
    if current.status == "COMMITTED":
        return
    with capture(Trace(f"commit:{conversation.id}:{session}", component="ingestion",stage="SESSION_COMMIT")) as trace:
        commit_session(manager_for(state), current, state.core, control_plane=state.control_plane, neo4j=state.neo4j, embed=state.embed, memory_repository=state.memory_repository, tenant_id=tid)
    append(Path(__import__('os').environ['LOCOMO_TRACE_DIR'])/"ingestion_trace.jsonl", trace.to_dict())


def pending_provider_recovery(conn, tid):
    """Read-only waiting state; the native worker alone authorizes recovery.

    Health and the 30-second delay are enforced by native maintenance. The
    benchmark retains FAILED in its event counts and waits within its existing
    readiness timeout; it must never turn this into successful ingestion.
    """
    transient = r"^(APITimeoutError|APIConnectionError|RateLimitError|InternalServerError|CircuitOpenError):"
    return [r["event_id"] for r in conn.execute(
        "SELECT e.event_id FROM events e WHERE e.tenant_id=%s AND e.status='FAILED' "
        "AND e.event_type='INGEST' AND e.error_message ~ %s "
        "AND NOT EXISTS (SELECT 1 FROM workflow_dispatches d WHERE d.tenant_id=e.tenant_id "
        "AND d.aggregate_id=e.event_id AND d.workflow_type='INGEST' "
        "AND d.payload->>'recovery_policy'='provider-recovery-v1') ORDER BY e.event_id",
        (tid, transient),
    ).fetchall()]


def readiness_dispatches(conn, tid, awaiting_recovery=()):
    """A recovered workflow supersedes its failed generation, not its history.

    Projection failures remain blocking: a newer publication must not hide a
    failed canonical projection dispatch.
    """
    return conn.execute(
        "WITH latest AS (SELECT DISTINCT ON (workflow_type,aggregate_id) "
        "CASE WHEN workflow_type='INGEST' AND aggregate_id=ANY(%s) AND status IN ('DEAD','FAILED') "
        "THEN 'RECOVERY_WAITING' ELSE status END AS status "
        "FROM workflow_dispatches WHERE tenant_id=%s AND workflow_type<>'PROJECTION' "
        "ORDER BY workflow_type,aggregate_id,generation DESC), "
        "relevant AS (SELECT status FROM latest UNION ALL SELECT status FROM workflow_dispatches "
        "WHERE tenant_id=%s AND workflow_type='PROJECTION') "
        "SELECT status,count(*) AS n FROM relevant GROUP BY status", (list(awaiting_recovery),tid,tid)
    ).fetchall()


def readiness(state, tid):
    with psycopg.connect(state.cfg.event_ledger.dsn, row_factory=dict_row) as conn:
        events = conn.execute("SELECT status,count(*) AS n FROM events WHERE tenant_id=%s GROUP BY status", (tid,)).fetchall()
        history = conn.execute("SELECT status,count(*) AS n FROM workflow_dispatches WHERE tenant_id=%s GROUP BY status", (tid,)).fetchall()
        awaiting_recovery = pending_provider_recovery(conn, tid) if state.cfg.canonical_memory.recover_transient_failures else []
        dispatch = readiness_dispatches(conn, tid, awaiting_recovery)
        tasks = conn.execute("SELECT status,count(*) AS n FROM consolidation_tasks WHERE tenant_id=%s GROUP BY status", (tid,)).fetchall()
        nodes = conn.execute("SELECT id::text,revision FROM memory_nodes WHERE tenant_id=%s AND status NOT IN ('DELETED','TOMBSTONED')", (tid,)).fetchall()
        specs = conn.execute("SELECT payload,aggregate_revision FROM workflow_dispatches WHERE tenant_id=%s AND workflow_type='PROJECTION'",(tid,)).fetchall()
    with state.neo4j.reader().session() as session:
        indexes = [dict(r) for r in session.run("SHOW INDEXES YIELD name,state RETURN name,state")]
        revisions = {r["id"]: r["revision"] for r in session.run("MATCH (n {tenant_id:$tid}) RETURN n.memory_id AS id,n.projected_revision AS revision", tid=tid)}
    expected = {}
    for spec in specs:
        payload=spec["payload"]
        if payload.get("memory_id") and payload.get("operation") == "UPSERT_NODE":
            identity=payload["memory_id"]
            expected[identity]=max(expected.get(identity,0),int(payload.get("revision") or spec["aggregate_revision"] or 0))
    mismatched = [identity for identity,revision in expected.items() if revisions.get(identity,-1)<revision]
    # Ancestor invalidation can increment an EPISODE's canonical revision
    # without requesting another node publication. Require the publication
    # actually requested in the native outbox, and record canonical lag as a
    # retrieval limitation rather than inventing additional background work.
    lag=[{"memory_id":r["id"],"canonical_revision":r["revision"],"projected_revision":revisions.get(r["id"]),"requested_revision":expected.get(r["id"])} for r in nodes if revisions.get(r["id"],-1)<r["revision"]]
    result = {"events": {r["status"]: r["n"] for r in events}, "dispatches": {r["status"]: r["n"] for r in dispatch}, "historical_dispatches": {r["status"]: r["n"] for r in history}, "tasks": {r["status"]: r["n"] for r in tasks}, "indexes": indexes, "projection_mismatches": mismatched,"canonical_projection_lag":lag}
    result["ready"] = all(r["status"] in {"COMPLETE", "GATED_SKIP"} for r in events) and all(r["status"] == "COMPLETE" for r in dispatch) and all(r["status"] in {"COMPLETE", "COMPLETED"} for r in tasks) and not mismatched and all(r["state"] == "ONLINE" for r in indexes)
    result["pending_provider_recovery"] = awaiting_recovery
    permanent_events = any(r["status"] == "DEAD" or (r["status"] == "FAILED" and r["n"] > len(awaiting_recovery)) for r in events)
    result["fatal"] = permanent_events or any(r["status"] in {"FAILED", "DEAD"} for r in [*dispatch, *tasks])
    if state.cfg.retrieval.quality_mode == "enforce" or state.cfg.canonical_memory.evidence_mode == "enforce":
        result["ready"] = result["ready"] and not lag
        result["readiness_policy"] = "canonical-revision-alignment"
    return result


class Provenance:
    def __init__(self, data, index, conversation):
        self.data, self.conversation = data, conversation
        self.events = {r["event_id"]: r["dia_id"] for r in index if r["conversation_id"] == conversation.id}
        pg = data["postgres"]
        self.claims = {str(r["id"]): r for r in pg.get("memory_claims", [])}
        self.versions = {str(r["id"]): r for r in pg.get("memory_versions", [])}
        self.memories = {str(r["id"]): r for r in pg.get("memory_nodes", [])}
        self.evidence = {str(r["id"]): r for r in pg.get("memory_evidence", [])}
        self.sessions = {r["session_id"]:r["payload"] if isinstance(r["payload"],dict) else json.loads(r["payload"]) for r in pg.get("session_states",[])}
        self.turn_ids={(session_id(conversation.id,t.session),t.index*2):t.dia_id for t in conversation.turns}

    def row(self, row):
        if not isinstance(row, dict):
            row = safe(row)
        claim = self.claims.get(str(row.get("claim_id")))
        if claim is not None:
            source_events = [claim.get("source_event_id"), *[e.get("source_event_id") for e in self.evidence.values() if str(e.get("claim_id")) == str(claim["id"])]]
            ids = list(dict.fromkeys(self.events[event] for event in source_events if event in self.events))
            return ids, bool(ids) and all(event in self.events for event in source_events if event is not None)
        memory = self.memories.get(str(row.get("memory_id")))
        version = self.versions.get(str(row.get("version_id") or (memory or {}).get("current_version_id")))
        if version and (row.get("text") or row.get("body") or row.get("abstract")) in {version.get("body"), version.get("abstract")}:
            dia = self.events.get(version.get("source_event_id"))
            return ([dia] if dia else []), bool(dia)
        return [], False

    def rows(self, rows):
        ids, covered = [], 0
        for row in rows:
            found, exact = self.row(row)
            covered += exact
            ids.extend(found)
        return list(dict.fromkeys(ids)), covered, len(rows)

    def context(self, context):
        ids, exact, count = [], True, 0
        text = context.get("text", "")
        for source in context.get("sources", []):
            count += 1
            if source.get("source_type") == "session_turn":
                dia = self.turn_ids.get((source.get("session_id"), source.get("turn_idx")))
                if dia and source.get("role") == "user":
                    ids.append(dia)
                else:
                    exact = False
            elif source.get("event_id"):
                dia = self.events.get(source["event_id"])
                if dia:
                    ids.append(dia)
                else:
                    exact = False
            elif source.get("session_id"):
                from engram.session.manager import SessionState
                payload=self.sessions.get(source["session_id"])
                if not payload:
                    exact=False
                    continue
                state=SessionState.from_payload(payload)
                rendered=state.render(max_turns=50)
                if not rendered.endswith(text):
                    exact=False
                    continue
                cutoff=len(rendered)-len(text)
                position=len("(compacted session summary)\n"+state.compacted)+1 if state.compacted else 0
                if state.compacted and cutoff<position:
                    exact=False
                for turn in state.turns[-50:]:
                    block=f"{turn.role.upper()}: {turn.content}"
                    if turn.role=="user" and position<cutoff<position+len(block):
                        exact=False
                    if position>=cutoff and turn.role == "user":
                        identity=self.turn_ids.get((source["session_id"],turn.turn_idx))
                        if identity:
                            ids.append(identity)
                        else:
                            exact=False
                    position+=len(block)+1
            else:
                exact = False
        return list(dict.fromkeys(ids)), exact, count

    def context_tail(self, context, cutoff):
        """Map an observed text suffix; partial records stay unevaluable."""
        text=context["text"][cutoff:]
        sources=context.get("sources",[])
        if all(s.get("session_id") and not s.get("event_id") for s in sources):
            return self.context({**context,"text":text})
        complete_sources=[]
        partial=0
        offset=0
        by_id={s.get("event_id"):s for s in sources}
        for line in context["text"].splitlines(keepends=True):
            end=offset+len(line)
            if end>cutoff:
                if offset<cutoff:
                    partial+=1
                else:
                    try:
                        record=json.loads(line)
                    except (ValueError,TypeError):
                        partial+=1
                    else:
                        source=by_id.get(record.get("event_id")) if isinstance(record,dict) else None
                        if source:
                            complete_sources.append(source)
                        else:
                            partial+=1
            offset=end
        ids,exact,count=self.context({"text":text,"sources":complete_sources})
        return ids,exact and not partial,count+partial


def compare(ctx, plan, method, tid, sid):
    """Discovery ablations share native hydration, verification and packing."""
    from engram.retrieval.tools import MemoryTools
    from engram.retrieval.orchestrator import _hybrid_retrieve
    from engram.retrieval.context import ContextBuilder
    # Disable one discovery capability at the dependency boundary, preserving
    # native eligible fusion and ranking; no broad repository fallback.
    class Channel:
        def __init__(self, neo, enabled):
            self.neo, self.enabled = neo, enabled
        def __getattr__(self, name):
            if name in {"bm25_search", "vector_search"} and name not in self.enabled:
                raise AttributeError(name)
            return getattr(self.neo, name)
    trace = CURRENT.get()
    diagnostics, rows = {}, []
    if method != "kg":
        enabled = {"bm25_search"} if method == "bm25" else {"vector_search"} if method == "vector" else {"bm25_search", "vector_search"}
        trace.stage = "L3_VECTOR"
        trace.executed.append(trace.stage)
        started=time.perf_counter()
        rows, discovered = _hybrid_retrieve(replace(ctx, neo4j=Channel(ctx.neo4j, enabled)), ctx.repository(), plan, tenant_id=tid, diagnostics=diagnostics)
        trace.emit("comparison_discovery", diagnostics=diagnostics, discovered=discovered, rows=rows,latency_ms=(time.perf_counter()-started)*1000)
    if method in {"kg", "hybrid_kg"}:
        trace.stage = "L3_GRAPH"
        graph, _ = MemoryTools(ctx, tenant_id=tid, session_id=sid).graph_search(plan)
        seen = {(r.memory_id, r.claim_id) for r in rows}
        # Shortest bundles first; claims shared by two bundles remain atomic.
        sizes = {b: sum(b in r.bundle_ids for r in graph) for r in graph for b in r.bundle_ids}
        graph.sort(key=lambda r: (min((sizes[b] for b in r.bundle_ids), default=1), str(r.claim_id)))
        for row in graph:
            key = (row.memory_id, row.claim_id)
            if key in seen:
                for prior in rows:
                    if (prior.memory_id, prior.claim_id) == key:
                        prior.bundle_ids = list(dict.fromkeys([*prior.bundle_ids, *row.bundle_ids]))
                        break
            else:
                rows.append(row)
                seen.add(key)
    if ctx.cfg.retrieval.quality_mode == "enforce":
        from engram.retrieval.orchestrator import RetrievalMetadata
        from engram.retrieval.sufficiency import SufficiencyController, canonical_items, POLICY_VERSION
        controller = SufficiencyController(ctx, RetrievalMetadata(), plan, tid)
        # Comparisons remain retrieval-only: use the native packing budget,
        # eligibility and requirement priorities, without a semantic call.
        controller.assessments = ctx.cfg.retrieval.sufficiency_max_assessments
        controller.assess(canonical_items(rows, tid), "COMPARISON")
        selected = {item.id for item in controller.items}
        packed = [row for row in rows if "canonical:" + str(row.identity) in selected]
        diagnostics["packing_policy"] = POLICY_VERSION
        return rows, packed, diagnostics
    from engram.retrieval.domain import RetrievalIntent
    budget = ctx.cfg.retrieval.full_doc_budget_tokens if plan.intent in {RetrievalIntent.HISTORY, RetrievalIntent.EVIDENCE} else ctx.cfg.retrieval.overview_budget_tokens
    packed = ContextBuilder(context_window=budget, session_share=.15, evidence_share=.70).pack_evidence(plan, rows, budget=int(budget * .70))
    return rows, packed, diagnostics
