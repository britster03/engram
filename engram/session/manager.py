"""Session manager (§9).

Owns session lifecycle: ACTIVE → WINDOWED → COMMITTING → COMMITTED. Serialises
session state to the SessionCache (Redis in prod, memory in dev).
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

from engram import frontmatter, tokens
from engram import uri as uri_mod
from engram.config import SessionConfig
from engram.models.core import CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.storage.filesystem import FilesystemStore
from engram.storage.memory_kg import InMemoryKnowledgeGraph
from engram.storage.memory_repository import MemoryRepository, _BoundTransactionStore
from engram.storage.neo4j_store import Neo4jStore
from engram.storage.postgres import PostgresStore
from engram.storage.redis_cache import SessionCache
from engram.tenancy import DEFAULT_TENANT_ID, current_tenant_id
from engram.uri import pair_id as pair_id_fn

log = logging.getLogger(__name__)
_memory_compaction_lock = threading.RLock()


@dataclass
class Turn:
    role: str  # "user" | "assistant"
    content: str
    timestamp: str  # ISO 8601
    turn_idx: int
    ingestible: bool = True


@dataclass
class SessionState:
    session_id: str
    status: str = "ACTIVE"  # ACTIVE | WINDOWED | COMMITTING | COMMITTED
    turns: list[Turn] = field(default_factory=list)
    compacted_turns_idx_upper_bound: int = 0  # turns[:N] have been compacted
    compacted: str | None = None  # compacted block from §8.3
    key_facts: list[str] = field(default_factory=list)
    key_entities: list[str] = field(default_factory=list)
    compaction_metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_payload(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_payload(cls, data: dict[str, Any]) -> SessionState:
        turns = [Turn(**t) for t in data.get("turns", [])]
        s = cls(
            session_id=data["session_id"],
            status=data.get("status", "ACTIVE"),
            turns=turns,
            compacted_turns_idx_upper_bound=data.get("compacted_turns_idx_upper_bound", 0),
            compacted=data.get("compacted"),
            key_facts=data.get("key_facts", []),
            key_entities=data.get("key_entities", []),
            compaction_metadata=data.get("compaction_metadata", {}),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
        )
        return s

    def render(self, *, max_turns: int | None = 20) -> str:
        """Serialise the session to a string suitable for prompt inclusion.

        Compacted block (if any) goes first, then the most recent `max_turns`
        uncompacted turns in chronological order.
        """
        parts: list[str] = []
        if self.compacted:
            parts.append(f"(compacted session summary)\n{self.compacted}")
        tail = self.turns[self.compacted_turns_idx_upper_bound:]
        if max_turns is not None:
            tail = tail[-max_turns:] if max_turns > 0 else []
        for t in tail:
            parts.append(f"{t.role.upper()}: {t.content}")
        return "\n".join(parts)


class SessionManager:
    def __init__(
        self,
        cache: SessionCache,
        *,
        window_threshold_ratio: float | None = None,
        max_turns_before_window: int | None = None,
        session_ttl_minutes: int,
        control_plane: PostgresStore | None = None,
        session_config: SessionConfig | None = None,
        core_output_tokens: int = 2048,
    ) -> None:
        self.cache = cache
        self.control_plane = control_plane
        self.window_threshold_ratio = window_threshold_ratio
        self.max_turns_before_window = max_turns_before_window
        self.session_config = session_config or SessionConfig()
        self.core_output_tokens = core_output_tokens
        self.session_ttl_seconds = session_ttl_minutes * 60

    def create(self) -> SessionState:
        session = SessionState(session_id=f"sess-{uuid.uuid4().hex[:12]}")
        self._persist(session)
        return session

    def get(self, session_id: str) -> SessionState | None:
        data = self.cache.get(session_id)
        if data is None:
            if self.control_plane is None:
                return None
            snapshot = self.control_plane.get_session_state(
                session_id, tenant_id=current_tenant_id()
            )
            if snapshot is not None:
                self.cache.set(session_id, snapshot, ttl_seconds=self.session_ttl_seconds)
                return SessionState.from_payload(snapshot)
            events = self.control_plane.list_session_events(
                session_id, tenant_id=current_tenant_id(), limit=1000
            )
            if not events:
                return None
            state = SessionState(session_id=session_id)
            seen: set[int] = set()
            for event in events:
                payload = event.get("payload") or {}
                pair = payload.get("turn_pair") or {}
                timestamp = str(event.get("created_at") or state.created_at)
                for role in ("user", "assistant"):
                    item = pair.get(role) or {}
                    idx = item.get("turn_idx")
                    if not isinstance(idx, int) or idx in seen or not item.get("content"):
                        continue
                    seen.add(idx)
                    state.turns.append(
                        Turn(
                            role=role,
                            content=str(item["content"]),
                            timestamp=timestamp,
                            turn_idx=idx,
                            ingestible=event.get("event_type") == "INGEST"
                            and not payload.get("derived_response", False),
                        )
                    )
            state.turns.sort(key=lambda turn: turn.turn_idx)
            if not state.turns:
                return None
            state.created_at = state.turns[0].timestamp
            self._persist(state)
            return state
        return SessionState.from_payload(data)

    def delete(self, session_id: str) -> None:
        self.cache.delete(session_id)

    def append_turn_pair(
        self,
        session_id: str,
        user: str,
        assistant: str,
        *,
        ingestible: bool = True,
    ) -> tuple[SessionState, bool]:
        """Append a (user, assistant) pair. Returns (state, needs_compaction)."""
        if self.control_plane is not None:
            tenant = current_tenant_id()
            with self.control_plane.transaction() as conn:
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
                    (f"session:{tenant}:{session_id}",),
                ).fetchone()
                payload = self.control_plane.get_session_state(session_id, tenant_id=tenant)
                state = (
                    SessionState.from_payload(payload)
                    if payload
                    else self.get(session_id) or SessionState(session_id=session_id)
                )
                state, needs_compact = self._append_to_state(state, user, assistant, ingestible)
                self.control_plane.save_session_state(
                    session_id, state.to_payload(), tenant_id=tenant
                )
                user_turn, assistant_turn = state.turns[-2:]
                self.control_plane.record_event(
                    pair_id=pair_id_fn(session_id, user_turn.turn_idx, assistant_turn.turn_idx),
                    session_id=session_id,
                    tenant_id=tenant,
                    source="session" if ingestible else "retrieval_session",
                    event_type="INGEST" if ingestible else "QUERY",
                    initial_status="RECEIVED" if ingestible else "COMPLETE",
                    payload={
                        "session_id": session_id,
                        "derived_response": not ingestible,
                        "turn_pair": {
                            "user": {"content": user, "turn_idx": user_turn.turn_idx},
                            "assistant": {
                                "content": assistant,
                                "turn_idx": assistant_turn.turn_idx,
                            },
                        },
                        "session_context": state.render(max_turns=10),
                    },
                )
                self.schedule_compaction(state)
            # Publish the cache only after the state and its source record commit.
            self.cache.delete(session_id)
            return state, needs_compact
        state = self.get(session_id) or SessionState(session_id=session_id)
        state, needs_compact = self._append_to_state(state, user, assistant, ingestible)
        self._persist(state)
        return state, needs_compact

    def _append_to_state(
        self,
        state: SessionState,
        user: str,
        assistant: str,
        ingestible: bool,
    ) -> tuple[SessionState, bool]:
        if state.status not in {"ACTIVE", "WINDOWED"}:
            raise ValueError("session is not active")
        now = datetime.now(timezone.utc).isoformat()
        next_idx = state.turns[-1].turn_idx + 1 if state.turns else 0
        state.turns.append(
            Turn(
                role="user",
                content=user,
                timestamp=now,
                turn_idx=next_idx,
                ingestible=ingestible,
            )
        )
        state.turns.append(
            Turn(
                role="assistant",
                content=assistant,
                timestamp=now,
                turn_idx=next_idx + 1,
                ingestible=ingestible,
            )
        )
        needs_compact = self._needs_compaction(state)
        state.status = "WINDOWED" if needs_compact else "ACTIVE"
        return state, needs_compact

    def needs_compaction(self, state: SessionState) -> bool:
        return self._needs_compaction(state)

    def _needs_compaction(self, state: SessionState) -> bool:
        return self.context_tokens(state) >= self.session_config.compaction_trigger_tokens

    def context_tokens(self, state: SessionState) -> int:
        return tokens.count_tokens_exact(state.render(max_turns=None))

    def schedule_compaction(self, state: SessionState) -> bool:
        """Called inside acceptance transactions; task and snapshot commit together."""
        if state.status not in {"ACTIVE", "WINDOWED"}:
            return False
        needed = self.needs_compaction(state)
        state.status = "WINDOWED" if needed else "ACTIVE"
        if needed and self.control_plane is not None:
            self.control_plane.enqueue_task(node_id=state.session_id,
                task_type="SESSION_COMPACT", tenant_id=current_tenant_id(), mark_dirty=True)
        return needed

    def _persist(self, state: SessionState) -> None:
        if self.control_plane is not None:
            tenant = current_tenant_id()
            with self.control_plane.transaction() as conn:
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
                    (f"session:{tenant}:{state.session_id}",),
                )
                payload = self.control_plane.get_session_state(state.session_id, tenant_id=tenant)
                if payload:
                    latest = SessionState.from_payload(payload)
                    # A close/compaction result may have been computed before
                    # another accepted turn. Never replace that newer history.
                    by_index = {turn.turn_idx: turn for turn in state.turns}
                    by_index.update({turn.turn_idx: turn for turn in latest.turns})
                    state.turns = [by_index[index] for index in sorted(by_index)]
                    if latest.status in {"COMMITTING", "COMMITTED"} and state.status in {
                        "ACTIVE",
                        "WINDOWED",
                    }:
                        state.status = latest.status
                    if (
                        latest.compacted_turns_idx_upper_bound
                        > state.compacted_turns_idx_upper_bound
                    ):
                        state.compacted = latest.compacted
                        state.compacted_turns_idx_upper_bound = (
                            latest.compacted_turns_idx_upper_bound
                        )
                        state.key_facts = latest.key_facts
                        state.key_entities = latest.key_entities
                        state.compaction_metadata = latest.compaction_metadata
                self.control_plane.save_session_state(
                    state.session_id, state.to_payload(), tenant_id=tenant
                )
            self.cache.delete(state.session_id)
            return
        self.cache.set(state.session_id, state.to_payload(), ttl_seconds=self.session_ttl_seconds)


# ----------------------------------------------------------------------
# Compaction (§8.3)
# ----------------------------------------------------------------------


def compact_session(
    manager: SessionManager,
    state: SessionState,
    core: CoreModelProvider,
    control_plane: PostgresStore | None = None,
    *,
    tenant_id: str | None = None,
) -> SessionState:
    """Compact the oldest uncompacted turns via the Core Model.

    Per §8.3.2, the compaction is a lossy reorganisation that replaces the
    oldest turns in the session cache with a compacted summary. The original
    turn pairs are then re-enqueued into the ingest pipeline (with
    source='session_compact') so long-term memory is extracted from the raw
    turns, not the lossy compaction. Caller passes `control_plane` to enable that
    re-ingest; omitting it keeps the in-cache compaction only (useful for
    unit tests).
    """
    from engram.metrics import session_compaction_outcomes, session_compaction_tokens
    from engram.session.compaction import summarize_turns

    tid = tenant_id or current_tenant_id()
    if state.status not in {"ACTIVE", "WINDOWED"}:
        return state
    lower = state.compacted_turns_idx_upper_bound
    available = state.turns[lower:]
    active = False
    # A native chat accepts the user before generating its assistant response.
    if manager.control_plane is not None:
        active = manager.control_plane.get_conn().execute(
            "SELECT 1 FROM chat_runs WHERE tenant_id = ? AND session_id = ? "
            "AND status = 'RUNNING' AND deadline > CURRENT_TIMESTAMP LIMIT 1",
            (tid, state.session_id),
        ).fetchone()
        if active and available and available[-1].role == "user":
            available = available[:-1]
    if not available and not state.compacted:
        return state
    if not available and active:
        return state
    before = manager.context_tokens(state)
    cfg = manager.session_config
    target = int(cfg.compaction_trigger_tokens * cfg.compaction_target_ratio)
    upper = lower
    # Remove oldest whole units until the fresh tail plus a summary reserve fits.
    while upper < lower + len(available):
        upper += 1
        if (state.turns[upper - 1].role == "user" and upper < lower + len(available)
                and state.turns[upper].role == "assistant"):
            upper += 1
        tail = SessionState(state.session_id, turns=state.turns[upper:])
        if manager.context_tokens(tail) + cfg.compaction_summary_tokens + 32 <= target:
            break
    selected = state.turns[lower:upper]
    previous = {"compacted": state.compacted or "", "key_facts": state.key_facts,
                "key_entities": state.key_entities}
    expected_summary = previous
    source_hash = _source_hash(state.turns[:upper])
    started = time.perf_counter()
    try:
        output, calls = summarize_turns(core, selected, session_id=state.session_id,
            cfg=cfg, core_output_tokens=manager.core_output_tokens, previous=previous)
        if not output.get("compacted"):
            return state
        # A stale output can never replace a newer summary or closed session.
        with _memory_compaction_lock:
            store = manager.control_plane
            if store is not None:
                with store.transaction() as conn:
                    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
                                 (f"session:{tid}:{state.session_id}",)).fetchone()
                    payload = store.get_session_state(state.session_id, tenant_id=tid)
                    latest = SessionState.from_payload(payload) if payload else state
                    applied = _apply_compaction(manager, latest, lower, upper,
                        source_hash, expected_summary, output, before, calls)
                    if applied:
                        _reenqueue_for_ingest(control_plane or store, state.session_id, selected, tenant_id=tid)
                        store.save_session_state(state.session_id, latest.to_payload(), tenant_id=tid)
                        manager.schedule_compaction(latest)
            else:
                payload = manager.cache.get(state.session_id, tenant_id=tid)
                latest = SessionState.from_payload(payload) if payload else state
                applied = _apply_compaction(manager, latest, lower, upper,
                    source_hash, expected_summary, output, before, calls)
                if applied:
                    if control_plane is not None:
                        _reenqueue_for_ingest(control_plane, state.session_id, selected, tenant_id=tid)
                    manager.cache.set(latest.session_id, latest.to_payload(),
                                      ttl_seconds=manager.session_ttl_seconds, tenant_id=tid)
            if store is not None:
                manager.cache.delete(state.session_id, tenant_id=tid)
        session_compaction_outcomes.labels("applied" if applied else "stale").inc()
        if applied:
            session_compaction_tokens.labels("before").observe(before)
            session_compaction_tokens.labels("after").observe(manager.context_tokens(latest))
        state.__dict__.update(latest.__dict__)
        return state
    except Exception:
        session_compaction_outcomes.labels("failed").inc()
        raise
    finally:
        from engram.metrics import session_compaction_latency
        session_compaction_latency.observe(time.perf_counter() - started)


def _source_hash(turns):
    return hashlib.sha256(json.dumps([asdict(t) for t in turns], sort_keys=True,
                                    ensure_ascii=False).encode()).hexdigest()


def _apply_compaction(manager, latest, lower, upper, source_hash, previous, output, before, calls):
    if (latest.status not in {"ACTIVE", "WINDOWED"}
            or latest.compacted_turns_idx_upper_bound != lower
            or {"compacted": latest.compacted or "", "key_facts": latest.key_facts,
                "key_entities": latest.key_entities} != previous
            or _source_hash(latest.turns[:upper]) != source_hash):
        return False
    latest.compacted = output["compacted"]
    latest.key_facts = output["key_facts"]
    latest.key_entities = output["key_entities"]
    latest.compacted_turns_idx_upper_bound = upper
    latest.status = "WINDOWED" if manager.needs_compaction(latest) else "ACTIVE"
    latest.compaction_metadata = {
        "encoding": tokens.ENCODING_NAME, "source_hash": source_hash,
        "covered_turns": [lower, upper], "covered_indices": [t.turn_idx for t in latest.turns[lower:upper]],
        "tokens_before": before, "tokens_after": manager.context_tokens(latest),
        "model_calls": calls, "summary_policy": "rolling-v1",
    }
    log.info("session compaction published", extra={"compaction": latest.compaction_metadata})
    return True


def commit_session(
    manager: SessionManager,
    state: SessionState,
    core: CoreModelProvider,
    *,
    control_plane: PostgresStore | None = None,
    fs: FilesystemStore | None = None,
    neo4j: Neo4jStore | InMemoryKnowledgeGraph | None = None,
    embed: EmbeddingService | None = None,
    memory_repository: MemoryRepository | None = None,
    tenant_id: str | None = None,
) -> SessionState:
    """Commit a session and write its durable SESSION_SUMMARY node.

    The commit path is ledger-only for raw turns: it records deterministic
    ingest events and lets the durable worker process them. In V2 the summary
    is committed to PostgreSQL before the cache entry may be removed; Neo4j is
    updated later by the projection workflow. The filesystem branch exists
    only for legacy development/test configurations.
    """
    tid = tenant_id or current_tenant_id() or DEFAULT_TENANT_ID
    state.status = "COMMITTING"
    manager._persist(state)

    ingestible_turns = [turn for turn in state.turns if turn.ingestible]
    if control_plane is not None:
        _reenqueue_for_ingest(control_plane, state.session_id, state.turns, tenant_id=tid)

    if ingestible_turns:
        summary, key_facts, key_entities = _summarize_for_commit(
            core,
            state,
            manager=manager,
            turns=ingestible_turns,
        )
        if summary:
            state.compacted = summary
        state.key_facts = key_facts
        state.key_entities = key_entities

        # Only explicitly ingestible turns may create canonical long-term
        # memory. Retrieval-generated answers stay available as session
        # history, but cannot reinforce themselves as new evidence.
        canonical_state = replace(
            state,
            turns=ingestible_turns,
            compacted=None,
            key_facts=key_facts,
            key_entities=key_entities,
        )
        durable_summary = summary or canonical_state.render(max_turns=50)
        if memory_repository is not None:
            _commit_canonical_session_summary(
                repository=memory_repository,
                state=canonical_state,
                summary=durable_summary,
                tenant_id=tid,
            )
        elif fs is not None and neo4j is not None and embed is not None:
            _write_session_summary_node(
                state=canonical_state,
                fs=fs,
                neo4j=neo4j,
                embed=embed,
                summary=durable_summary,
                tenant_id=tid,
            )

    state.status = "COMMITTED"
    manager._persist(state)
    return state


def _commit_canonical_session_summary(
    *,
    repository: MemoryRepository,
    state: SessionState,
    summary: str,
    tenant_id: str,
) -> str:
    """Commit one idempotent SESSION_SUMMARY and its projection outbox rows."""

    mutation_key = f"session:{state.session_id}:summary"
    body = summary.strip()
    if state.key_facts:
        body += "\n\nKey facts:\n" + "\n".join(f"- {fact}" for fact in state.key_facts)
    with repository.transaction(tenant_id=tenant_id) as conn:
        canonical = MemoryRepository(
            _BoundTransactionStore(conn, projection_task_queue=repository.projection_task_queue),
            default_tenant_id=tenant_id,
        )
        node = canonical.create_memory(
            memory_type="SESSION_SUMMARY",
            canonical_name=f"Session {state.session_id}",
            metadata={
                "source_session_id": state.session_id,
                "key_entities": list(state.key_entities),
            },
            tenant_id=tenant_id,
            mutation_key=mutation_key,
            mutation_operation="COMMIT_SESSION_SUMMARY",
        )
        version = canonical.append_version(
            node.id,
            body=body,
            abstract=summary.strip()[:500],
            provenance={"extractor": "session_commit", "extractor_version": "v2"},
            metadata={"source_session_id": state.session_id},
            tenant_id=tenant_id,
            mutation_key=f"{mutation_key}:version",
        )
        canonical.record_evidence(
            memory_id=node.id,
            source_session_id=state.session_id,
            extractor="session_commit",
            extractor_version="v2",
            confidence=0.9,
            source_text_hash=hashlib.sha256(body.encode("utf-8")).hexdigest(),
            metadata={"version_id": str(version.id)},
            idempotency_key=f"{mutation_key}:evidence",
            tenant_id=tenant_id,
        )
    return node.canonical_uri


def _summarize_for_commit(
    core: CoreModelProvider,
    state: SessionState,
    *,
    manager: SessionManager,
    turns: list[Turn] | None = None,
) -> tuple[str, list[str], list[str]]:
    selected_turns = state.turns if turns is None else turns
    if not selected_turns:
        return "", [], []
    from engram.session.compaction import summarize_turns
    out, _ = summarize_turns(core, selected_turns, session_id=state.session_id,
        cfg=manager.session_config, core_output_tokens=manager.core_output_tokens)
    return out["compacted"], out["key_facts"], out["key_entities"]


def _write_session_summary_node(
    *,
    state: SessionState,
    fs: FilesystemStore,
    neo4j: Neo4jStore | InMemoryKnowledgeGraph,
    embed: EmbeddingService,
    summary: str,
    tenant_id: str,
) -> str:
    now = datetime.now(timezone.utc).isoformat()
    uri = f"mem://user/session_summaries/{state.session_id}.md"
    fm = {
        "id": str(uuid.uuid4()),
        "node_type": "SESSION_SUMMARY",
        "status": "ACTIVE",
        "created_at": now,
        "source_session_id": state.session_id,
        "schema_version": 1,
        "provenance": {
            "extractor": "session_commit",
            "confidence": 0.9,
        },
    }
    body = summary.strip() + "\n"
    if state.key_facts:
        body += "\nKey facts:\n" + "\n".join(f"- {fact}" for fact in state.key_facts) + "\n"
    mf = frontmatter.MemoryFile(frontmatter=fm, body=body)
    fs.write_atomic(uri, mf.serialize())
    neo4j.merge_node(
        source_uri=uri,
        parent_uri=uri_mod.parent_uri(uri),
        tenant_id=tenant_id,
        properties={
            "id": fm["id"],
            "node_type": "SESSION_SUMMARY",
            "status": "ACTIVE",
            "l0_abstract": summary[:500],
            "l0_embedding": embed.embed(summary[:2000] or state.session_id),
            "retrieval_weight": 1.0,
            "created_at": now,
            "last_accessed_at": now,
            "access_count": 0,
            "schema_version": 1,
            "source_session_id": state.session_id,
        },
    )
    return uri


def _reenqueue_for_ingest(
    control_plane: PostgresStore,
    session_id: str,
    turns: list[Turn],
    *,
    tenant_id: str | None = None,
) -> None:
    tid = tenant_id or current_tenant_id() or DEFAULT_TENANT_ID
    # Group consecutive user→assistant pairs. Users/assistants always alternate
    # in an ACTIVE session so pairing by index is safe.
    pairs: list[tuple[Turn, Turn]] = []
    i = 0
    while i < len(turns) - 1:
        if (
            turns[i].role == "user"
            and turns[i + 1].role == "assistant"
            and turns[i].ingestible
            and turns[i + 1].ingestible
        ):
            pairs.append((turns[i], turns[i + 1]))
            i += 2
        else:
            i += 1  # skip unpaired turn
    for user, assistant in pairs:
        pid = pair_id_fn(session_id, user.turn_idx, assistant.turn_idx)
        control_plane.record_event(
            pair_id=pid,
            session_id=session_id,
            source="session_compact",
            event_type="INGEST",
            payload={
                "session_id": session_id,
                "turn_pair": {
                    "user": {"content": user.content, "turn_idx": user.turn_idx},
                    "assistant": {"content": assistant.content, "turn_idx": assistant.turn_idx},
                },
            },
            tenant_id=tid,
        )
