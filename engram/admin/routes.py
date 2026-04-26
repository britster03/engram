import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from engram.admin.auth import require_ui_auth, verify_api_key, create_session_token, SESSION_COOKIE_NAME, SESSION_MAX_AGE
from engram.deps import get_state

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
        with open(Path(I18N_DIR) / f"{safe}.json", "r", encoding="utf-8") as f:
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
async def admin_chat(request: Request) -> HTMLResponse:
    result = await _check_ui_auth(request)
    if isinstance(result, RedirectResponse):
        return result
    return templates.TemplateResponse(request, "chat.html", _build_context(request))


@admin_router.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request) -> HTMLResponse:
    result = await _check_ui_auth(request)
    if isinstance(result, RedirectResponse):
        return result
    return templates.TemplateResponse(request, "dashboard.html", _build_context(request))


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
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    if not target.exists() or target.is_dir():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return FileResponse(str(target))


# ---------------------------------------------------------------------------
# Admin API – lightweight wrappers around engram APIs for the dashboard UI
# ---------------------------------------------------------------------------

def _get_state_safe():
    try:
        return get_state()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"app not ready: {exc}")


@admin_router.get("/admin/api/stats")
async def admin_stats(request: Request):
    verify_ui_auth(request)
    state = _get_state_safe()
    kg_counts = {}
    try:
        rows = state.neo4j.run_template(
            "MATCH (n:Node) WITH count(n) AS nodes "
            "OPTIONAL MATCH ()-[r]->() "
            "RETURN nodes, count(r) AS edges",
            {},
            timeout_s=3,
        )
        if rows:
            kg_counts = {
                "kg_nodes": int(rows[0].get("nodes", 0)),
                "kg_edges": int(rows[0].get("edges", 0)),
            }
    except Exception:
        pass
    queue_depth = 0
    try:
        queue_depth = state.sqlite.queue_depth()
    except Exception:
        pass
    return JSONResponse({**kg_counts, "queue_depth": queue_depth})


@admin_router.get("/admin/api/pipeline")
async def admin_pipeline(request: Request):
    verify_ui_auth(request)
    state = _get_state_safe()
    pending_events = 0
    processing_events = 0
    outbox_pending = 0
    try:
        pending_events = state.sqlite.count_events_by_status("RECEIVED")
        processing_events = state.sqlite.count_events_by_status("PROCESSING")
        outbox_pending = state.sqlite.count_outbox_pending()
    except Exception:
        pass
    return JSONResponse({
        "pending_events": pending_events,
        "processing_events": processing_events,
        "outbox_pending": outbox_pending,
    })


@admin_router.get("/admin/api/sessions")
async def admin_sessions(request: Request):
    verify_ui_auth(request)
    state = _get_state_safe()
    try:
        raw_sessions = state.session_cache.list_sessions()
        sessions = []
        for data in raw_sessions:
            sid = data.get("session_id", "")
            sessions.append({
                "id": sid,
                "status": data.get("status", "ACTIVE"),
                "turns": len(data.get("turns", [])),
                "last_active": data.get("created_at", ""),
            })
        sessions.sort(key=lambda s: s["last_active"] or "", reverse=True)
        return JSONResponse({"sessions": sessions})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@admin_router.post("/admin/api/logout")
async def admin_logout(request: Request):
    resp = RedirectResponse(url="/admin/login", status_code=307)
    resp.delete_cookie("engram_session_token")
    return resp


@admin_router.post("/admin/api/login")
async def admin_login_api(request: Request):
    body = await request.json()
    api_key = body.get("api_key", "")
    admin_key = os.environ.get("ENGRAM_ADMIN_KEY")
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
        admin_key = os.environ.get("ENGRAM_ADMIN_KEY")
        if admin_key and verify_api_key(api_key, admin_key):
            return
    raise HTTPException(status_code=401, detail="Unauthorized")
