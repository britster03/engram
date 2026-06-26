"""Chat completions endpoint (§11.3).

POST /api/v1/chat/completions → buffered or SSE streaming.
Auto-creates/maintains sessions and fires background ingest on completion.
"""

from __future__ import annotations

import json as _json
import logging
import queue
import threading
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from starlette.responses import StreamingResponse

from engram.api.auth import AuthDep
from engram.api.schemas import ChatCompletionRequest, ChatCompletionResponse
from engram.deps import AppState, get_state, make_orchestrator_context
from engram.retrieval.orchestrator import run_query
from engram.session.manager import SessionManager, SessionState, compact_session
from engram.tenancy import Tenant, TenantQuotas, current_tenant_id, set_current_tenant
from engram.uri import pair_id as pair_id_fn

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["chat"], dependencies=[AuthDep])

_SENTINEL = object()


def _manager(state: AppState) -> SessionManager:
    return SessionManager(
        state.session_cache,
        window_threshold_ratio=state.cfg.session.window_threshold_ratio,
        max_turns_before_window=state.cfg.session.max_turns_before_window,
        session_ttl_minutes=state.cfg.session.timeout_minutes,
    )


def _bind_tenant(tenant_id: str) -> None:
    set_current_tenant(
        Tenant(
            tenant_id=tenant_id,
            display_name=tenant_id,
            api_key_hashes=[],
            quotas=TenantQuotas(),
            status="ACTIVE",
        )
    )


def _run_compaction(state: AppState, session_id: str, tenant_id: str) -> None:
    try:
        _bind_tenant(tenant_id)
        mgr = _manager(state)
        sess = mgr.get(session_id)
        if sess is None:
            return
        if not mgr.needs_compaction(sess):
            return
        compact_session(mgr, sess, state.core, sqlite=state.sqlite, tenant_id=tenant_id)
    except Exception:
        log.exception("auto-compaction failed for session %s", session_id)


def _append_turn_and_record(
    state: AppState,
    session_id: str,
    user_msg: str,
    assistant_msg: str,
    tenant_id: str,
) -> None:
    _bind_tenant(tenant_id)
    mgr = _manager(state)
    sess, needs_compaction = mgr.append_turn_pair(session_id, user_msg, assistant_msg)
    user_turn = sess.turns[-2]
    asst_turn = sess.turns[-1]
    pid = pair_id_fn(session_id, user_turn.turn_idx, asst_turn.turn_idx)
    state.sqlite.record_event(
        pair_id=pid,
        session_id=session_id,
        source="session",
        event_type="INGEST",
        payload={
            "session_id": session_id,
            "turn_pair": {
                "user": {"content": user_msg, "turn_idx": user_turn.turn_idx},
                "assistant": {"content": assistant_msg, "turn_idx": asst_turn.turn_idx},
            },
            "session_context": sess.render(max_turns=10),
        },
        tenant_id=tenant_id,
    )
    # The durable ingest worker is the only component that processes RECEIVED rows.
    # Automatic compaction is intentionally left to /sessions/{id}/compact or
    # session close so chat completion remains a ledger-only write path.
    _ = needs_compaction


def _sse_chat_stream_realtime(
    ctx: Any,
    req: ChatCompletionRequest,
    session: SessionState,
    state: AppState,
    result_box: list | None = None,
    tenant_id: str | None = None,
) -> Any:
    """SSE generator that emits retrieval_step events in real-time as the
    cascade progresses, then streams the answer deltas.

    If *result_box* is provided, the final QueryResult is placed in
    result_box[0] so the caller can schedule post-stream work.
    """
    q: queue.Queue = queue.Queue()

    def on_step(snapshot: dict[str, Any]) -> None:
        q.put(("retrieval_step", snapshot))

    def worker() -> None:
        if tenant_id is not None:
            _bind_tenant(tenant_id)
        try:
            result = run_query(
                ctx,
                session_id=session.session_id,
                query=req.messages[-1].content,
                session_context=req.session_context or session.render(max_turns=20),
                max_depth=req.max_depth,
                max_reentries=req.max_reentries,
                on_step=on_step,
            )
            q.put(("result", result))
        except Exception as err:
            q.put(("error", err))
        finally:
            q.put(_SENTINEL)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    result: Any = None
    try:
        while True:
            try:
                item = q.get(timeout=300)
            except queue.Empty:
                yield f"event: error\ndata: {_json.dumps({'error': 'query timeout'})}\n\n"
                yield "event: done\ndata: {}\n\n"
                return

            if item is _SENTINEL:
                break
            kind, payload = item
            if kind == "retrieval_step":
                yield f"event: retrieval_step\ndata: {_json.dumps(payload)}\n\n"
            elif kind == "result":
                result = payload
            elif kind == "error":
                yield f"event: error\ndata: {_json.dumps({'error': str(payload)})}\n\n"
                yield "event: done\ndata: {}\n\n"
                return
    finally:
        thread.join(timeout=5)

    if result is None:
        yield f"event: error\ndata: {_json.dumps({'error': 'no result from query'})}\n\n"
        yield "event: done\ndata: {}\n\n"
        return

    if result_box is not None:
        result_box.append(result)

    md = result.retrieval_metadata.to_dict()
    yield f"event: metadata\ndata: {_json.dumps(md)}\n\n"

    try:
        answer = result.answer
        for i in range(0, len(answer), 12):
            yield f"event: delta\ndata: {_json.dumps({'text': answer[i:i+12]})}\n\n"
    except Exception as err:
        log.exception("streaming failed")
        yield f"event: error\ndata: {_json.dumps({'error': str(err)})}\n\n"
        yield "event: done\ndata: {}\n\n"
        return

    yield "event: done\ndata: {}\n\n"


@router.post("/chat/completions", response_model=ChatCompletionResponse, status_code=200)
def chat_completions(
    req: ChatCompletionRequest,
    background: BackgroundTasks,
) -> ChatCompletionResponse | StreamingResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    mgr = _manager(state)

    if req.session_id:
        session = mgr.get(req.session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="session not found")
        if session.status == "COMMITTED":
            raise HTTPException(status_code=400, detail="session is committed")
    else:
        session = mgr.create()

    query = req.messages[-1].content
    session_context = req.session_context or session.render(max_turns=20)

    ctx = make_orchestrator_context(state)

    if not req.stream:
        try:
            result = run_query(
                ctx,
                session_id=session.session_id,
                query=query,
                session_context=session_context,
                max_depth=req.max_depth,
                max_reentries=req.max_reentries,
            )
        except Exception as err:
            log.exception("query failed")
            raise HTTPException(status_code=500, detail=f"query failed: {err}") from err

        background.add_task(
            _append_turn_and_record, state, session.session_id, query, result.answer, tenant_id
        )

        return ChatCompletionResponse(
            answer=result.answer,
            session_id=session.session_id,
            retrieval_metadata=result.retrieval_metadata.to_dict(),
            finish_reason="stop",
        )

    _state = state
    _session = session
    _req = req
    _ctx = ctx
    _result_box: list = []
    _tenant_id = tenant_id

    def _stream_and_ingest() -> Any:
        try:
            yield from _sse_chat_stream_realtime(
                _ctx, _req, _session, _state, result_box=_result_box, tenant_id=_tenant_id,
            )
        except Exception:
            return

    from starlette.background import BackgroundTask

    def _ingest() -> None:
        if not _result_box:
            return
        try:
            _append_turn_and_record(
                _state,
                _session.session_id,
                _req.messages[-1].content,
                _result_box[0].answer,
                _tenant_id,
            )
        except Exception:
            log.exception("post-stream ingest failed for session %s", _session.session_id)

    return StreamingResponse(
        _stream_and_ingest(),
        media_type="text/event-stream",
        background=BackgroundTask(_ingest),
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
