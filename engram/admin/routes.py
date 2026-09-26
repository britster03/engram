import json
import logging
import os
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
)
from fastapi.templating import Jinja2Templates

from engram.admin.auth import (
    SESSION_MAX_AGE,
    create_session_token,
    require_ui_auth,
    verify_api_key,
)
from engram.api import schemas
from engram.deps import get_state
from engram.tenancy import DEFAULT_TENANT_ID, Tenant, TenantQuotas, set_current_tenant
from engram.uri import pair_id as pair_id_fn

log = logging.getLogger(__name__)

admin_router = APIRouter()

ADMIN_DIR = Path(__file__).parent
TEMPLATES_DIR = str(ADMIN_DIR / "templates")
STATIC_DIR = str(ADMIN_DIR / "static")
I18N_DIR = str(ADMIN_DIR / "i18n")

templates = Jinja2Templates(directory=TEMPLATES_DIR)


def _static_version(path: str) -> str:
    try:
        mtime = int((ADMIN_DIR / "static" / path).stat().st_mtime)
        return f"/admin/static/{path}?v={mtime}"
    except Exception:
        return f"/admin/static/{path}"


def _load_locale_json(lang: str) -> str:
    safe = lang if lang in {"en", "ja", "ko", "zh", "zh-TW"} else "en"
    try:
        with open(Path(I18N_DIR) / f"{safe}.json", encoding="utf-8") as f:
            return json.dumps(json.load(f))
    except Exception:
        return "{}"


def _build_context(request: Request, locale: str = "en", extra: dict | None = None) -> dict:
    lang = request.query_params.get("lang", locale if locale else "en")
    accept = request.headers.get("Accept-Language", "")
    if not lang or lang == "en":
        for segment in accept.replace(";", ",").split(","):
            seg = segment.strip()
            if seg.startswith("zh-Hant") or seg == "zh-TW":
                lang = "zh-TW"
                break
            if seg.startswith("zh"):
                lang = "zh"
                break
            if seg.startswith("ja"):
                lang = "ja"
                break
            if seg.startswith("ko"):
                lang = "ko"
                break
    ctx: dict = {
        "current_lang": lang,
        "locale_json": _load_locale_json(lang),
        "static": _static_version,
    }
    if extra:
        ctx.update(extra)
    return ctx


@admin_router.get("/admin/login", response_class=HTMLResponse)
async def admin_login(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "login.html", _build_context(request))


async def _check_ui_auth(request: Request):
    result = await require_ui_auth(request)
    if result is not None:
        return result
    return None


@admin_router.get("/admin/chat", response_class=HTMLResponse)
async def admin_chat(request: Request) -> Response:
    result = await _check_ui_auth(request)
    if isinstance(result, RedirectResponse):
        return result
    return templates.TemplateResponse(request, "chat.html", _build_context(request))


@admin_router.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request) -> Response:
    result = await _check_ui_auth(request)
    if isinstance(result, RedirectResponse):
        return result
    return templates.TemplateResponse(request, "dashboard.html", _build_context(request))


@admin_router.get("/admin/kg", response_class=HTMLResponse)
async def admin_kg(request: Request) -> Response:
    result = await _check_ui_auth(request)
    if isinstance(result, RedirectResponse):
        return result
    return templates.TemplateResponse(request, "kg.html", _build_context(request))


@admin_router.get("/admin")
async def admin_root(request: Request) -> RedirectResponse:
    result = await _check_ui_auth(request)
    if isinstance(result, RedirectResponse):
        return result
    return RedirectResponse(url="/admin/dashboard", status_code=307)


@admin_router.get("/admin/static/{path:path}")
async def admin_static(path: str) -> FileResponse:
    target = Path(STATIC_DIR) / path
    try:
        target.resolve().relative_to(Path(STATIC_DIR).resolve())
    except ValueError as err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND) from err
    if not target.exists() or target.is_dir():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return FileResponse(str(target))


# ---------------------------------------------------------------------------
# Admin API - lightweight wrappers around engram APIs for the dashboard UI
# ---------------------------------------------------------------------------


def _get_state_safe():
    try:
        return get_state()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"app not ready: {exc}") from exc


def _bind_default_tenant():
    state = _get_state_safe()
    tenant = state.tenant_registry.get(DEFAULT_TENANT_ID)
    if tenant is None:
        tenant = Tenant(
            tenant_id=DEFAULT_TENANT_ID,
            display_name="Default tenant",
            api_key_hashes=[],
            quotas=TenantQuotas(),
            status="ACTIVE",
        )
    set_current_tenant(tenant)
    return state, tenant


def _configured_admin_key() -> str | None:
    try:
        from engram.config import get_config

        cfg = get_config()
        return getattr(cfg.api, "admin_key", None) or os.environ.get("ENGRAM_ADMIN_KEY")
    except Exception:
        return os.environ.get("ENGRAM_ADMIN_KEY")


def _serialize_timestamp(value: object) -> str:
    """Return a stable JSON-safe representation for database timestamps."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value or "")


def _event_pair(event: dict) -> dict | None:
    """Read a user/assistant pair from a durable ingest event."""
    payload = event.get("payload") or {}
    pair = payload.get("turn_pair") or payload.get("turn_group")
    if not isinstance(pair, dict):
        return None
    user = pair.get("user")
    assistant = pair.get("assistant")
    if not isinstance(user, dict) or not isinstance(assistant, dict):
        return None
    if not isinstance(user.get("content"), str) or not isinstance(assistant.get("content"), str):
        return None
    return {
        "key": str(event.get("pair_id") or event.get("event_id")),
        "user": user,
        "assistant": assistant,
        "created_at": _serialize_timestamp(event.get("created_at")),
    }


def _cache_pairs(session_id: str, data: dict) -> list[dict]:
    """Convert cached session turns into deterministic, de-duplicable pairs."""
    turns = data.get("turns") or []
    pairs: list[dict] = []
    index = 0
    while index + 1 < len(turns):
        user, assistant = turns[index], turns[index + 1]
        if user.get("role") != "user" or assistant.get("role") != "assistant":
            index += 1
            continue
        user_idx, assistant_idx = user.get("turn_idx"), assistant.get("turn_idx")
        key = (
            pair_id_fn(session_id, user_idx, assistant_idx)
            if isinstance(user_idx, int) and isinstance(assistant_idx, int)
            else f"cache:{index}"
        )
        pairs.append(
            {
                "key": key,
                "user": user,
                "assistant": assistant,
                "created_at": _serialize_timestamp(user.get("timestamp") or data.get("created_at")),
            }
        )
        index += 2
    return pairs


def _history_payload(state, *, session_id: str, tenant_id: str) -> dict | None:
    """Merge cached and durable session turns for the Admin read-only view."""
    cached = state.session_cache.get(session_id, tenant_id=tenant_id)
    ledger_events = state.control_plane.list_session_events(session_id, tenant_id=tenant_id)
    if cached is None and not ledger_events:
        return None

    pairs: dict[str, dict] = {}
    for event in ledger_events:
        pair = _event_pair(event)
        if pair is not None:
            pairs[pair["key"]] = pair
    if cached is not None:
        # Cache contains the latest canonical session state, including a turn
        # that may not have reached the durable ledger yet.
        for pair in _cache_pairs(session_id, cached):
            pairs[pair["key"]] = pair

    ordered_pairs = sorted(
        pairs.values(),
        key=lambda pair: (
            not isinstance(pair["user"].get("turn_idx"), int),
            pair["user"].get("turn_idx", 0),
            pair["created_at"],
            pair["key"],
        ),
    )
    turns: list[dict] = []
    for pair in ordered_pairs:
        for role in ("user", "assistant"):
            turn = pair[role]
            turns.append(
                {
                    "role": role,
                    "content": turn["content"],
                    "timestamp": _serialize_timestamp(turn.get("timestamp") or pair["created_at"]),
                    "turn_idx": turn.get("turn_idx"),
                }
            )

    first_ledger = ledger_events[0]["created_at"] if ledger_events else None
    return {
        "session_id": session_id,
        "status": (cached or {}).get("status", "ARCHIVED"),
        "created_at": _serialize_timestamp((cached or {}).get("created_at") or first_ledger),
        "last_active": _serialize_timestamp(
            turns[-1]["timestamp"]
            if turns
            else (ledger_events[-1]["created_at"] if ledger_events else None)
        ),
        "turn_count": len(turns),
        "turns": turns,
    }


@admin_router.get("/admin/api/stats")
async def admin_stats(request: Request):
    verify_ui_auth(request)
    state, tenant = _bind_default_tenant()
    kg_counts = {"kg_nodes": 0, "kg_edges": 0}
    try:
        graph = state.neo4j.graph(
            depth=4,
            limit=500,
            tenant_id=tenant.tenant_id,
        )
        kg_counts = {
            "kg_nodes": len(graph.get("nodes", [])),
            "kg_edges": len(graph.get("edges", [])),
        }
    except Exception:
        pass
    queue_depth = 0
    with suppress(Exception):
        queue_depth = state.control_plane.queue_depth(tenant_id=tenant.tenant_id)
    return JSONResponse({**kg_counts, "queue_depth": queue_depth})


@admin_router.get("/admin/api/pipeline")
async def admin_pipeline(request: Request):
    verify_ui_auth(request)
    state = _get_state_safe()
    pending_events = 0
    processing_events = 0
    outbox_pending = 0
    try:
        pending_events = state.control_plane.count_events_by_status("RECEIVED")
        processing_events = state.control_plane.count_events_by_status("PROCESSING")
        outbox_pending = state.control_plane.count_outbox_pending()
    except Exception:
        pass
    return JSONResponse(
        {
            "pending_events": pending_events,
            "processing_events": processing_events,
            "outbox_pending": outbox_pending,
        }
    )


@admin_router.get("/admin/api/sessions")
async def admin_sessions(request: Request):
    verify_ui_auth(request)
    state, tenant = _bind_default_tenant()
    try:
        raw_sessions = state.session_cache.list_sessions(tenant_id=tenant.tenant_id)
        sessions_by_id = {}
        for data in raw_sessions:
            sid = data.get("session_id", "")
            if not sid:
                continue
            last_turn = (data.get("turns") or [])[-1] if data.get("turns") else {}
            sessions_by_id[sid] = {
                "id": sid,
                "status": data.get("status", "ACTIVE"),
                "turns": len(data.get("turns", [])),
                "last_active": _serialize_timestamp(
                    last_turn.get("timestamp") or data.get("created_at")
                ),
                "history_available": True,
            }
        for row in state.control_plane.list_session_rollups(tenant_id=tenant.tenant_id):
            sid = row["session_id"]
            if sid in sessions_by_id:
                continue
            sessions_by_id[sid] = {
                "id": sid,
                "status": "ARCHIVED",
                "turns": int(row["pair_count"]) * 2,
                "last_active": _serialize_timestamp(row["last_active"]),
                "history_available": True,
            }
        sessions = list(sessions_by_id.values())
        sessions.sort(key=lambda s: s["last_active"] or "", reverse=True)
        return JSONResponse({"sessions": sessions})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@admin_router.get("/admin/api/sessions/{session_id}/history")
async def admin_session_history(request: Request, session_id: str):
    verify_ui_auth(request)
    state, tenant = _bind_default_tenant()
    payload = _history_payload(
        state,
        session_id=session_id,
        tenant_id=tenant.tenant_id,
    )
    if payload is None:
        raise HTTPException(status_code=404, detail="session history not found")
    return JSONResponse(payload)


@admin_router.post("/admin/api/sessions")
async def admin_create_session(request: Request):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.sessions import create_session

    return create_session()


@admin_router.get("/admin/api/memories")
async def admin_memories(
    request: Request,
    prefix: str = Query(default="mem://"),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = Query(default=None),
):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.memories import list_memories

    return list_memories(prefix=prefix, limit=limit, cursor=cursor)


@admin_router.get("/admin/api/memories/detail")
async def admin_memory_detail(
    request: Request,
    source_uri: str = Query(..., min_length=1),
):
    """Return the canonical PostgreSQL snapshot used by the KG detail panel."""

    verify_ui_auth(request)
    state, tenant = _bind_default_tenant()
    from engram.api.routes.memories import get_memory

    memory = get_memory(source_uri)
    payload = memory.model_dump(mode="json")
    repository = state.memory_repository
    if repository is None:
        return payload
    node = repository.resolve_memory_ref(
        source_uri,
        tenant_id=tenant.tenant_id,
        include_historical=True,
    )
    if node is None:
        return payload
    payload.update(
        {
            "canonical_name": node.canonical_name,
            "origin_event_id": node.origin_event_id,
            "created_at": _serialize_timestamp(node.created_at),
            "updated_at": _serialize_timestamp(node.updated_at),
        }
    )
    if node.origin_event_id:
        event = state.control_plane.get_event(
            node.origin_event_id,
            tenant_id=tenant.tenant_id,
        )
        if event is not None:
            payload["source_event"] = {
                "event_id": event.get("event_id"),
                "session_id": event.get("session_id"),
                "source": event.get("source"),
                "status": event.get("status"),
                "created_at": _serialize_timestamp(event.get("created_at")),
                "turn_pair": _event_pair(event),
            }
    return payload


@admin_router.post("/admin/api/ingest")
async def admin_ingest(request: Request, req: schemas.IngestRequest):
    verify_ui_auth(request)
    state, tenant = _bind_default_tenant()
    depth = state.control_plane.queue_depth(tenant_id=tenant.tenant_id)
    if depth > state.cfg.consolidation.max_backlog:
        raise HTTPException(
            status_code=503,
            detail=(
                f"consolidation queue saturated ({depth} > {state.cfg.consolidation.max_backlog})"
            ),
            headers={"Retry-After": "30"},
        )
    pair = req.effective_pair()
    user_idx = pair.user.turn_idx or 0
    asst_idx = pair.assistant.turn_idx or (user_idx + 1)
    pid = pair_id_fn(req.session_id or "stateless", user_idx, asst_idx)
    event_id, _ = state.control_plane.record_event(
        pair_id=pid,
        session_id=req.session_id,
        source=req.source,
        event_type="INGEST",
        payload=req.model_dump(),
        tenant_id=tenant.tenant_id,
    )
    response = schemas.IngestResponse(event_id=event_id, pair_id=pid, status="RECEIVED")
    return JSONResponse(content=response.model_dump(), status_code=status.HTTP_202_ACCEPTED)


@admin_router.post("/admin/api/ingest/bulk", status_code=status.HTTP_202_ACCEPTED)
async def admin_create_bulk_job(
    request: Request,
    file: Annotated[UploadFile, File()],
    dry_run: Annotated[bool, Form()] = False,
    session_id: Annotated[str | None, Form()] = None,
    project_name: Annotated[str | None, Form()] = None,
    file_format: Annotated[Literal["jsonl", "csv", "zip"] | None, Form()] = None,
):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.bulk_ingest import create_bulk_job

    return await create_bulk_job(
        file=file,
        dry_run=dry_run,
        session_id=session_id,
        project_name=project_name,
        file_format=file_format,
    )


@admin_router.get("/admin/api/ingest/bulk/{job_id}")
async def admin_get_bulk_job(request: Request, job_id: str):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.bulk_ingest import get_bulk_job

    return get_bulk_job(job_id)


@admin_router.post("/admin/api/ingest/bulk/{job_id}/events/{event_id}/retry")
async def admin_retry_bulk_event(request: Request, job_id: str, event_id: str):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.bulk_ingest import retry_failed_bulk_event

    return retry_failed_bulk_event(job_id, event_id)


@admin_router.post("/admin/api/chat/completions")
async def admin_chat_completions(
    request: Request,
    req: schemas.ChatCompletionRequest,
):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.chat import chat_completions

    return chat_completions(req)


@admin_router.get("/admin/api/kg/graph")
async def admin_kg_graph(
    request: Request,
    root_uri: str | None = Query(default=None),
    depth: int = Query(default=1, ge=0, le=4),
    limit: int = Query(default=100, ge=1, le=500),
    type: Literal[
        "ENTITY",
        "EPISODE",
        "EVENT",
        "FACT",
        "DOCUMENT",
        "DIRECTORY",
        "SESSION_SUMMARY",
        "COLLECTION",
        "PROFILE",
        "PREFERENCE",
        "PROJECT",
        "FILE",
        "CLASS",
        "FUNCTION",
        "METHOD",
        "EXTERNAL_MODULE",
    ]
    | None = Query(default=None),
):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.kg import graph

    return graph(root_uri=root_uri, depth=depth, limit=limit, type=type)


@admin_router.get("/admin/api/kg/projects")
async def admin_code_projects(request: Request):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.kg import code_projects

    return code_projects()


@admin_router.get("/admin/api/kg/code-map")
async def admin_code_map(
    request: Request,
    project_uri: str = Query(..., min_length=1),
    depth: int = Query(default=2, ge=0, le=6),
    limit: int = Query(default=200, ge=1, le=500),
):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.kg import code_map

    return code_map(project_uri=project_uri, depth=depth, limit=limit)


@admin_router.get("/admin/api/kg/node-details")
async def admin_code_node_details(
    request: Request,
    source_uri: str = Query(..., min_length=1),
):
    verify_ui_auth(request)
    _bind_default_tenant()
    from engram.api.routes.kg import code_node_details

    return code_node_details(source_uri=source_uri)


@admin_router.post("/admin/api/logout")
async def admin_logout(request: Request):
    resp = RedirectResponse(url="/admin/login", status_code=307)
    resp.delete_cookie("engram_session_token")
    return resp


@admin_router.post("/admin/api/login")
async def admin_login_api(request: Request):
    body = await request.json()
    api_key = body.get("api_key", "")
    admin_key = _configured_admin_key()
    if admin_key and verify_api_key(api_key, admin_key):
        token = create_session_token()
        resp = JSONResponse({"status": "ok"})
        resp.set_cookie(
            "engram_session_token",
            token,
            max_age=SESSION_MAX_AGE,
            httponly=True,
            samesite="lax",
        )
        return resp
    raise HTTPException(status_code=401, detail="Invalid API key")


def verify_ui_auth(request: Request):
    path = request.url.path
    if path in ("/admin/login", "/admin/static/*"):
        return
    token = request.cookies.get("engram_session_token")
    if token:
        from engram.admin.auth import verify_session_token

        if verify_session_token(token):
            return
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        api_key = auth.split(" ", 1)[1].strip()
        admin_key = _configured_admin_key()
        if admin_key and verify_api_key(api_key, admin_key):
            return
    raise HTTPException(status_code=401, detail="Unauthorized")
