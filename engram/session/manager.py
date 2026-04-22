"""Session manager (§9).

Owns session lifecycle: ACTIVE → WINDOWED → COMMITTING → COMMITTED. Serialises
session state to the SessionCache (Redis in prod, memory in dev).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from engram import prompts
from engram.models.core import CoreModelProvider
from engram.storage.redis_cache import SessionCache
from engram.storage.sqlite import SqliteStore
from engram.uri import pair_id as pair_id_fn

log = logging.getLogger(__name__)


@dataclass
class Turn:
    role: str            # "user" | "assistant"
    content: str
    timestamp: str       # ISO 8601
    turn_idx: int


@dataclass
class SessionState:
    session_id: str
    status: str = "ACTIVE"                      # ACTIVE | WINDOWED | COMMITTING | COMMITTED
    turns: list[Turn] = field(default_factory=list)
    compacted_turns_idx_upper_bound: int = 0    # turns[:N] have been compacted
    compacted: str | None = None                # compacted block from §8.3
    key_facts: list[str] = field(default_factory=list)
    key_entities: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_payload(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_payload(cls, data: dict[str, Any]) -> "SessionState":
        turns = [Turn(**t) for t in data.get("turns", [])]
        s = cls(
            session_id=data["session_id"],
            status=data.get("status", "ACTIVE"),
            turns=turns,
            compacted_turns_idx_upper_bound=data.get("compacted_turns_idx_upper_bound", 0),
            compacted=data.get("compacted"),
            key_facts=data.get("key_facts", []),
            key_entities=data.get("key_entities", []),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
        )
        return s

    def render(self, *, max_turns: int = 20) -> str:
        """Serialise the session to a string suitable for prompt inclusion.

        Compacted block (if any) goes first, then the most recent `max_turns`
        uncompacted turns in chronological order.
        """
        parts: list[str] = []
        if self.compacted:
            parts.append(f"(compacted session summary)\n{self.compacted}")
        tail = self.turns[-max_turns:]
        for t in tail:
            parts.append(f"{t.role.upper()}: {t.content}")
        return "\n".join(parts)


class SessionManager:
    def __init__(
        self,
        cache: SessionCache,
        *,
        window_threshold_ratio: float,
        max_turns_before_window: int,
        session_ttl_minutes: int,
    ) -> None:
        self.cache = cache
        self.window_threshold_ratio = window_threshold_ratio
        self.max_turns_before_window = max_turns_before_window
        self.session_ttl_seconds = session_ttl_minutes * 60

    def create(self) -> SessionState:
        session = SessionState(session_id=f"sess-{uuid.uuid4().hex[:12]}")
        self._persist(session)
        return session

    def get(self, session_id: str) -> SessionState | None:
        data = self.cache.get(session_id)
        if data is None:
            return None
        return SessionState.from_payload(data)

    def delete(self, session_id: str) -> None:
        self.cache.delete(session_id)

    def append_turn_pair(
        self, session_id: str, user: str, assistant: str
    ) -> tuple[SessionState, bool]:
        """Append a (user, assistant) pair. Returns (state, needs_compaction)."""
        state = self.get(session_id) or SessionState(session_id=session_id)
        now = datetime.now(timezone.utc).isoformat()
        next_idx = state.turns[-1].turn_idx + 1 if state.turns else 0
        state.turns.append(Turn(role="user", content=user, timestamp=now, turn_idx=next_idx))
        state.turns.append(
            Turn(role="assistant", content=assistant, timestamp=now, turn_idx=next_idx + 1)
        )
        needs_compact = self._needs_compaction(state)
        if needs_compact and state.status == "ACTIVE":
            state.status = "WINDOWED"
        self._persist(state)
        return state, needs_compact

    def needs_compaction(self, state: SessionState) -> bool:
        return self._needs_compaction(state)

    def _needs_compaction(self, state: SessionState) -> bool:
        uncompacted = state.turns[state.compacted_turns_idx_upper_bound :]
        if len(uncompacted) >= self.max_turns_before_window:
            return True
        # Crude token-threshold heuristic: average 40 tokens/turn, window ratio
        # of 0.4 of a ~200k context ≈ 2000 turns; kick in earlier with a
        # simpler proxy.
        total_chars = sum(len(t.content) for t in uncompacted)
        return total_chars > 12000  # ~3k tokens

    def _persist(self, state: SessionState) -> None:
        self.cache.set(
            state.session_id, state.to_payload(), ttl_seconds=self.session_ttl_seconds
        )


# ----------------------------------------------------------------------
# Compaction (§8.3)
# ----------------------------------------------------------------------

def compact_session(
    manager: SessionManager,
    state: SessionState,
    core: CoreModelProvider,
    sqlite: SqliteStore | None = None,
) -> SessionState:
    """Compact the oldest uncompacted turns via the Core Model.

    Per §8.3.2, the compaction is a lossy reorganisation that replaces the
    oldest turns in the session cache with a compacted summary. The original
    turn pairs are then re-enqueued into the ingest pipeline (with
    source='session_compact') so long-term memory is extracted from the raw
    turns, not the lossy compaction. Caller passes `sqlite` to enable that
    re-ingest; omitting it keeps the in-cache compaction only (useful for
    unit tests).
    """
    uncompacted = state.turns[state.compacted_turns_idx_upper_bound :]
    if not uncompacted:
        return state
    # Compact the oldest half so the tail remains usable as fresh context.
    split = max(1, len(uncompacted) // 2)
    to_compact = uncompacted[:split]

    turn_history = [
        {"role": t.role, "content": t.content, "turn_idx": t.turn_idx} for t in to_compact
    ]
    prompt = prompts.render(
        "session_compact",
        turn_history=turn_history,
        compaction_budget=2000,
    )
    result = core.complete(
        system_prompt=prompt,
        user_prompt="Return the compaction JSON.",
    )
    out = result.output if isinstance(result.output, dict) else {}
    compact_block = out.get("compacted") or ""
    key_facts = out.get("key_facts") or []
    key_entities = out.get("key_entities") or []
    state.compacted = (
        (state.compacted + "\n" + compact_block) if state.compacted else compact_block
    )
    state.key_facts = list(dict.fromkeys([*state.key_facts, *key_facts]))
    state.key_entities = list(dict.fromkeys([*state.key_entities, *key_entities]))
    state.compacted_turns_idx_upper_bound += len(to_compact)
    manager._persist(state)

    # §8.3.2 step 21: re-enqueue the uncompacted turn pairs as ingest events
    # so the authoritative ingest pipeline sees the raw turns (not the lossy
    # compaction). Each pair_id is deterministic, so resubmits are idempotent.
    if sqlite is not None:
        _reenqueue_for_ingest(sqlite, state.session_id, to_compact)
    return state


def _reenqueue_for_ingest(
    sqlite: SqliteStore, session_id: str, turns: list[Turn]
) -> None:
    # Group consecutive user→assistant pairs. Users/assistants always alternate
    # in an ACTIVE session so pairing by index is safe.
    pairs: list[tuple[Turn, Turn]] = []
    i = 0
    while i < len(turns) - 1:
        if turns[i].role == "user" and turns[i + 1].role == "assistant":
            pairs.append((turns[i], turns[i + 1]))
            i += 2
        else:
            i += 1  # skip unpaired turn
    for user, assistant in pairs:
        pid = pair_id_fn(session_id, user.turn_idx, assistant.turn_idx)
        sqlite.record_event(
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
        )
