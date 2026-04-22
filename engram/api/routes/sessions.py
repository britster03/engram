"""Session endpoints (§11.1).

POST   /api/v1/sessions              → create
GET    /api/v1/sessions/{id}         → state
DELETE /api/v1/sessions/{id}         → end
POST   /api/v1/sessions/{id}/message → append turn pair + enqueue ingest
POST   /api/v1/sessions/{id}/compact → force compaction
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from engram.api.auth import AuthDep
from engram.deps import AppState, get_state, make_ingest_context
from engram.ingest.worker import process_event
from engram.session.manager import SessionManager, compact_session
from engram.uri import pair_id as pair_id_fn

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sessions", tags=["sessions"], dependencies=[AuthDep])


class CreateResponse(BaseModel):
    session_id: str
    status: str


class SessionResponse(BaseModel):
    session_id: str
    status: str
    turn_count: int
    created_at: str
    compacted_turns: int
    key_facts: list[str]


class MessageRequest(BaseModel):
    user: str
    assistant: str


class MessageResponse(BaseModel):
    event_id: str
    pair_id: str
    needs_compaction: bool


class CompactResponse(BaseModel):
    session_id: str
    status: str
    compacted_turns: int


def _manager(state: AppState) -> SessionManager:
    return SessionManager(
        state.session_cache,
        window_threshold_ratio=state.cfg.session.window_threshold_ratio,
        max_turns_before_window=state.cfg.session.max_turns_before_window,
        session_ttl_minutes=state.cfg.session.timeout_minutes,
    )


@router.post("", response_model=CreateResponse, status_code=201)
def create_session() -> CreateResponse:
    state = get_state()
    mgr = _manager(state)
    session = mgr.create()
    return CreateResponse(session_id=session.session_id, status=session.status)


@router.get("/{session_id}", response_model=SessionResponse)
def get_session(session_id: str) -> SessionResponse:
    state = get_state()
    mgr = _manager(state)
    sess = mgr.get(session_id)
    if sess is None:
        raise HTTPException(status_code=404, detail="session not found")
    return SessionResponse(
        session_id=sess.session_id,
        status=sess.status,
        turn_count=len(sess.turns),
        created_at=sess.created_at,
        compacted_turns=sess.compacted_turns_idx_upper_bound,
        key_facts=sess.key_facts,
    )


@router.delete("/{session_id}")
def end_session(session_id: str) -> dict[str, Any]:
    state = get_state()
    mgr = _manager(state)
    sess = mgr.get(session_id)
    if sess is None:
        raise HTTPException(status_code=404, detail="session not found")
    sess.status = "COMMITTED"
    mgr._persist(sess)
    mgr.delete(session_id)
    return {"session_id": session_id, "status": "COMMITTED"}


@router.post("/{session_id}/message", response_model=MessageResponse, status_code=202)
def add_message(
    session_id: str, req: MessageRequest, background: BackgroundTasks
) -> MessageResponse:
    state = get_state()
    mgr = _manager(state)
    sess, needs_compaction = mgr.append_turn_pair(session_id, req.user, req.assistant)

    user_turn = sess.turns[-2]
    asst_turn = sess.turns[-1]
    pid = pair_id_fn(session_id, user_turn.turn_idx, asst_turn.turn_idx)
    event_id, is_new = state.sqlite.record_event(
        pair_id=pid,
        session_id=session_id,
        source="session",
        event_type="INGEST",
        payload={
            "session_id": session_id,
            "turn_pair": {
                "user": {"content": req.user, "turn_idx": user_turn.turn_idx},
                "assistant": {"content": req.assistant, "turn_idx": asst_turn.turn_idx},
            },
            "session_context": sess.render(max_turns=10),
        },
    )
    if is_new:
        background.add_task(_drive_event, state, event_id)

    # Auto-trigger compaction when the session has grown past the threshold
    # (§7.4 "on session compaction"). Runs in the background so the API stays
    # responsive; failures are logged and the session falls back to its raw
    # form (the session cache keeps older turns readable for one more round).
    if needs_compaction:
        background.add_task(_run_compaction, state, session_id)

    return MessageResponse(event_id=event_id, pair_id=pid, needs_compaction=needs_compaction)


@router.post("/{session_id}/compact", response_model=CompactResponse)
def compact_now(session_id: str) -> CompactResponse:
    state = get_state()
    mgr = _manager(state)
    sess = mgr.get(session_id)
    if sess is None:
        raise HTTPException(status_code=404, detail="session not found")
    sess = compact_session(mgr, sess, state.core, sqlite=state.sqlite)
    return CompactResponse(
        session_id=sess.session_id,
        status=sess.status,
        compacted_turns=sess.compacted_turns_idx_upper_bound,
    )


def _drive_event(state: AppState, event_id: str) -> None:
    try:
        ctx = make_ingest_context(state)
        process_event(ctx, event_id)
    except Exception:
        log.exception("session ingest worker failed for %s", event_id)


def _run_compaction(state: AppState, session_id: str) -> None:
    try:
        mgr = _manager(state)
        sess = mgr.get(session_id)
        if sess is None:
            return
        if not mgr.needs_compaction(sess):
            return
        # Pass sqlite so §8.3.2 re-ingest of the uncompacted turns kicks in.
        compact_session(mgr, sess, state.core, sqlite=state.sqlite)
    except Exception:
        log.exception("auto-compaction failed for session %s", session_id)
