"""Chat completions endpoint (§11.3).

POST /api/v1/chat/completions → buffered or SSE streaming.
Auto-creates/maintains sessions and records derived query history without
feeding generated answers back into canonical ingestion.
"""

from __future__ import annotations

import hashlib
import json as _json
import logging
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse, StreamingResponse

from engram.api.auth import AuthDep
from engram.api.schemas import ChatCompletionRequest, ChatCompletionResponse
from engram.deps import AppState, get_state, make_orchestrator_context
from engram.retrieval.orchestrator import run_query
from engram.session.manager import SessionManager, SessionState, compact_session
from engram.storage.chat_runs import (
    ChatConflictError,
    accept_message,
    find_run,
    find_session_run,
    finish_response,
    terminate_run,
)
from engram.tenancy import Tenant, TenantQuotas, current_tenant_id, set_current_tenant

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["chat"], dependencies=[AuthDep])

_SENTINEL = object()


def _manager(state: AppState) -> SessionManager:
    from engram.deps import make_session_manager
    return make_session_manager(state)


def _request_context(req: ChatCompletionRequest, session: SessionState) -> str:
    """Include supplied prior messages in both buffered and streaming requests."""
    parts = [session.render(max_turns=20)]
    if req.session_context:
        parts.append("Caller-supplied conversation context:\n" + req.session_context)
    if len(req.messages) > 1:
        parts.append(
            "Caller-supplied prior messages:\n"
            + "\n".join(
                f"{message.role.upper()}: {message.content}" for message in req.messages[:-1]
            )
        )
    return "\n\n".join(part for part in parts if part)


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
        compact_session(
            mgr, sess, state.core, control_plane=state.control_plane, tenant_id=tenant_id
        )
    except Exception:
        log.exception("auto-compaction failed for session %s", session_id)


def _append_turn_and_record(
    state: AppState,
    session_id: str,
    user_msg: str,
    assistant_msg: str,
    tenant_id: str,
    *,
    run_id: str | None = None,
    response: dict[str, Any] | None = None,
) -> None:
    _bind_tenant(tenant_id)
    mgr = _manager(state)
    if run_id and response is not None:
        finish_response(mgr, run_id, response, tenant=tenant_id)
        return
    # The manager commits the session snapshot and QUERY event atomically.
    # Derived answers have no ingestion dispatch.
    mgr.append_turn_pair(session_id, user_msg, assistant_msg, ingestible=False)


def _response(result: Any, session_id: str, run: dict[str, Any]) -> ChatCompletionResponse:
    return ChatCompletionResponse(
        answer=result.answer,
        session_id=session_id,
        run_id=run["run_id"],
        input_event_id=run["input_event_id"],
        retrieval_metadata=result.retrieval_metadata.to_dict(),
        answerability=result.answerability,
        sources=[item.model_dump(mode="json") for item in result.verified_evidence],
        conflicts=list(result.retrieval_metadata.conflicts),
        verification_rejections=list(result.retrieval_metadata.missing_evidence),
        finish_reason="stop",
    )


def _replay_stream(response: dict[str, Any]):
    yield f"event: metadata\ndata: {_json.dumps(response['retrieval_metadata'])}\n\n"
    yield f"event: delta\ndata: {_json.dumps({'text': response['answer']})}\n\n"
    yield "event: done\ndata: {}\n\n"


def _run_identity(run: dict[str, Any]) -> dict[str, Any]:
    return {
        key: run[key]
        for key in (
            "run_id",
            "session_id",
            "client_message_id",
            "input_event_id",
            "status",
            "created_at",
            "deadline",
            "error_code",
        )
    }


def _existing_run_response(
    req: ChatCompletionRequest, run: dict[str, Any]
) -> ChatCompletionResponse | StreamingResponse | JSONResponse:
    if run["client_message_id"] == req.client_message_id and run["status"] == "COMPLETED":
        if req.stream:
            return StreamingResponse(
                _replay_stream(run["response"]), media_type="text/event-stream"
            )
        return ChatCompletionResponse.model_validate(run["response"])
    running = run["status"] == "RUNNING"
    return JSONResponse(
        status_code=409,
        content=jsonable_encoder(
            {
                "detail": "a response is already running for this session"
                if running
                else "this message already has a finished response run; your message is saved",
                "code": "session_busy" if running else "message_finalized",
                "run": _run_identity(run),
            }
        ),
    )


def _sse_chat_stream_realtime(
    ctx: Any,
    req: ChatCompletionRequest,
    session: SessionState,
    state: AppState,
    result_box: list | None = None,
    tenant_id: str | None = None,
    run: dict[str, Any] | None = None,
) -> Any:
    """SSE generator that emits retrieval_step events in real-time as the
    cascade progresses, then streams the answer deltas.

    If *result_box* is provided, the final QueryResult is placed in
    result_box[0]. The worker durably commits history before emitting the result.
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
                session_context=_request_context(req, session),
                max_depth=req.max_depth,
                max_reentries=req.max_reentries,
                on_step=on_step,
                deadline_seconds=max(0, expires_at - time.monotonic()),
            )
            _append_turn_and_record(
                state,
                session.session_id,
                req.messages[-1].content,
                result.answer,
                tenant_id or current_tenant_id(),
                run_id=run["run_id"] if run else None,
                response=_response(result, session.session_id, run).model_dump(mode="json")
                if run
                else None,
            )
            q.put(("result", result))
        except Exception as err:
            if run:
                terminate_run(
                    state.control_plane, run["run_id"], tenant=tenant_id or current_tenant_id()
                )
            q.put(("error", err))
        finally:
            q.put(_SENTINEL)

    # Queue activity must not extend the durable run's absolute deadline.
    remaining = (run["deadline"] - datetime.now(timezone.utc)).total_seconds() if run else 300
    expires_at = time.monotonic() + max(0, remaining)
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    if run:
        accepted = _run_identity(run) | {"status": "saved"}
        yield f"event: accepted\ndata: {_json.dumps(jsonable_encoder(accepted))}\n\n"

    result: Any = None
    try:
        while True:
            try:
                remaining = expires_at - time.monotonic()
                if remaining <= 0:
                    raise queue.Empty
                item = q.get(timeout=remaining)
            except queue.Empty:
                if run:
                    terminate_run(
                        state.control_plane,
                        run["run_id"],
                        tenant=tenant_id or current_tenant_id(),
                        error_code="deadline_exceeded",
                    )
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
                log.error("chat generation failed: %s", type(payload).__name__)
                yield f"event: error\ndata: {_json.dumps({'error': 'Answer generation failed; your message is saved.'})}\n\n"
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
            yield f"event: delta\ndata: {_json.dumps({'text': answer[i : i + 12]})}\n\n"
    except Exception as err:
        log.exception("streaming failed")
        yield f"event: error\ndata: {_json.dumps({'error': str(err)})}\n\n"
        yield "event: done\ndata: {}\n\n"
        return

    yield "event: done\ndata: {}\n\n"


@router.post("/chat/completions", response_model=ChatCompletionResponse, status_code=200)
def chat_completions(
    req: ChatCompletionRequest,
) -> ChatCompletionResponse | StreamingResponse | JSONResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    mgr = _manager(state)

    client_id = req.client_message_id or uuid.uuid4().hex
    input_hash = hashlib.sha256(
        _json.dumps(
            {
                "messages": [m.model_dump() for m in req.messages],
                "session_context": req.session_context,
                "max_depth": req.max_depth,
                "max_reentries": req.max_reentries,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    prior = find_run(state.control_plane, tenant_id, client_message_id=client_id)
    if prior:
        if prior["input_hash"] != input_hash or (
            req.session_id and req.session_id != prior["session_id"]
        ):
            raise HTTPException(
                status_code=409, detail="client_message_id was already used for different input"
            )
        # Also replay server-assigned IDs for callers which omitted a key.
        if prior["status"] == "COMPLETED":
            if req.stream:
                return StreamingResponse(
                    _replay_stream(prior["response"]), media_type="text/event-stream"
                )
            return ChatCompletionResponse.model_validate(prior["response"])
        return _existing_run_response(req, prior)

    if req.session_id:
        session = mgr.get(req.session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="session not found")
        if session.status == "COMMITTED":
            raise HTTPException(status_code=400, detail="session is committed")
    else:
        session = mgr.create()

    query = req.messages[-1].content
    session_context = _request_context(req, session)

    try:
        run = accept_message(
            mgr,
            session.session_id,
            query,
            tenant=tenant_id,
            client_message_id=client_id,
            input_hash=input_hash,
            automatic_memory=(
                state.cfg.canonical_memory.enabled
                and state.cfg.canonical_memory.automatic_user_memory_enabled
            ),
        )
    except ChatConflictError as err:
        if err.run:
            return _existing_run_response(req, err.run)
        raise HTTPException(status_code=409, detail=str(err)) from err

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
                deadline_seconds=max(0, (run["deadline"] - datetime.now(timezone.utc)).total_seconds()),
            )
        except Exception as err:
            terminate_run(state.control_plane, run["run_id"], tenant=tenant_id)
            log.warning("chat query failed: %s", type(err).__name__)
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "Answer generation failed; your message is saved.",
                    "run_id": run["run_id"],
                    "session_id": session.session_id,
                },
            ) from err

        # Record the derived turn before returning. QUERY events are durable
        # history but deliberately have no Temporal ingestion dispatch.
        response = _response(result, session.session_id, run)
        try:
            _append_turn_and_record(
                state,
                session.session_id,
                query,
                result.answer,
                tenant_id,
                run_id=run["run_id"],
                response=response.model_dump(mode="json"),
            )
        except Exception as err:
            terminate_run(
                state.control_plane,
                run["run_id"],
                tenant=tenant_id,
                error_code="response_persistence_failed",
            )
            raise HTTPException(
                status_code=503, detail="Response could not be saved; your message is saved."
            ) from err
        return response

    return StreamingResponse(
        _sse_chat_stream_realtime(ctx, req, session, state, tenant_id=tenant_id, run=run),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _run_status(state: AppState, run: dict[str, Any]) -> dict[str, Any]:
    event = state.control_plane.get_event(run["input_event_id"], tenant_id=current_tenant_id())
    return _run_identity(run) | {
        "response": run["response"],
        "memory_status": (event["status"] if event["event_type"] == "INGEST" else "NOT_REQUESTED")
        if event
        else "UNAVAILABLE",
    }


@router.get("/chat/runs/{run_id}")
def chat_run(run_id: str) -> dict[str, Any]:
    state = get_state()
    run = find_run(state.control_plane, current_tenant_id(), run_id=run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="response run not found")
    return _run_status(state, run)


@router.get("/sessions/{session_id}/chat-run")
def session_chat_run(
    session_id: str,
    client_message_id: str | None = Query(default=None, min_length=1, max_length=128),
) -> dict[str, Any]:
    state = get_state()
    tenant = current_tenant_id()
    if state.control_plane.get_session_state(session_id, tenant_id=tenant) is None:
        raise HTTPException(status_code=404, detail="session not found")
    run = find_session_run(
        state.control_plane, tenant, session_id, client_message_id=client_message_id
    )
    return {"run": _run_status(state, run) if run else None}


@router.post("/chat/runs/{run_id}/cancel")
def cancel_chat_run(run_id: str) -> dict[str, Any]:
    state = get_state()
    if find_run(state.control_plane, current_tenant_id(), run_id=run_id) is None:
        raise HTTPException(status_code=404, detail="response run not found")
    terminate_run(
        state.control_plane,
        run_id,
        tenant=current_tenant_id(),
        cancelled=True,
        error_code="cancelled",
    )
    return chat_run(run_id)
