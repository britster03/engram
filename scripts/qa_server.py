"""
Minimal QA server for the Engram admin UI.

Skips the full Engram lifecycle and dependencies — just serves the
admin pages and the JSON APIs the UI needs, with canned responses.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

import uvicorn
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

app = FastAPI(title="Engram Admin QA", version="0.1.0")

ADMIN_DIR = ROOT / "engram" / "admin"
TEMPLATES_DIR = str(ADMIN_DIR / "templates")
STATIC_DIR = str(ADMIN_DIR / "static")
I18N_DIR = str(ADMIN_DIR / "i18n")

templates = Jinja2Templates(directory=TEMPLATES_DIR)


def _load_locale_json(lang: str) -> str:
    safe = lang if lang in {"en", "ja", "ko", "zh", "zh-TW"} else "en"
    try:
        with open(Path(I18N_DIR) / f"{safe}.json", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return "{}"


def _build_context(request: Request) -> dict:
    lang = "en"
    return {
        "request": request,
        "current_lang": lang,
        "locale_json": _load_locale_json(lang),
        "static": lambda path: f"/admin/static/{path}",
        "logo": "/admin/static/engram_logo.svg",
    }


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login(request: Request):
    return templates.TemplateResponse(request, "login.html", _build_context(request))


@app.get("/admin/chat", response_class=HTMLResponse)
async def admin_chat(request: Request):
    return templates.TemplateResponse(request, "chat.html", _build_context(request))


@app.get("/admin/kg", response_class=HTMLResponse)
async def admin_kg(request: Request):
    return templates.TemplateResponse(request, "kg.html", _build_context(request))


@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html", _build_context(request))


@app.get("/admin")
async def admin_root():
    return RedirectResponse(url="/admin/dashboard", status_code=307)


@app.post("/admin/api/login")
async def admin_login_api(request: Request):
    body = await request.json()
    api_key = body.get("api_key", "")
    if api_key == "test-api-key-for-qa":
        resp = JSONResponse({"status": "ok"})
        resp.set_cookie("engram_session_token", "qa-session", max_age=86400, httponly=True, samesite="lax")
        return resp
    return JSONResponse({"detail": "Invalid API key"}, status_code=401)


@app.post("/admin/api/logout")
async def admin_logout():
    resp = RedirectResponse(url="/admin/login", status_code=307)
    resp.delete_cookie("engram_session_token")
    return resp


@app.get("/admin/api/stats")
async def admin_stats(request: Request):
    return JSONResponse({"kg_nodes": 42, "kg_edges": 128, "queue_depth": 0})


@app.get("/admin/api/pipeline")
async def admin_pipeline(request: Request):
    return JSONResponse({"pending_events": 3, "processing_events": 1, "outbox_pending": 0})


@app.get("/admin/api/sessions")
async def admin_sessions(request: Request):
    return JSONResponse({
        "sessions": [
            {
                "id": "sess-test-001",
                "status": "ACTIVE",
                "turns": 3,
                "last_active": "2026-04-24T12:00:00Z",
            }
        ]
    })


@app.get("/readyz")
def readyz():
    return {"status": "ready", "components": {"sqlite": True, "neo4j": True, "redis": True, "filesystem": True}}


@app.get("/api/v1/health")
def health():
    return JSONResponse({
        "status": "healthy",
        "components": {"sqlite": True, "neo4j": True, "redis": True, "filesystem": True},
    })


@app.get("/api/v1/admin/tenants")
def tenants():
    return JSONResponse({"tenants": [{"tenant_id": "qa", "display_name": "QA Tenant"}]})


@app.get("/api/v1/memories")
def memories(prefix: str = "", limit: int = 50):
    return JSONResponse({
        "items": [
            {
                "source_uri": "mem://user/project-ideas",
                "node_type": "document",
                "l0_abstract": "Idea for a retrieval-augmented memory system.",
                "status": "ACTIVE",
                "created_at": "2026-04-24T11:00:00Z",
            }
        ],
        "next_cursor": None,
    })


@app.post("/admin/api/ingest")
@app.post("/api/v1/ingest")
def ingest():
    return JSONResponse({"event_id": "evt-qa-001", "pair_id": "pair-qa-001", "status": "RECEIVED"}, status_code=202)


@app.post("/admin/api/ingest/bulk")
@app.post("/api/v1/ingest/bulk")
async def ingest_bulk(
    file: Annotated[UploadFile, File()],
    dry_run: Annotated[bool, Form()] = False,
    session_id: Annotated[str | None, Form()] = None,
    file_format: Annotated[str | None, Form()] = None,
):
    raw = await file.read()
    name = file.filename or "upload.jsonl"
    inferred = file_format or ("csv" if name.endswith(".csv") else "zip" if name.endswith(".zip") else "jsonl")
    rejected_rows = []
    accepted_count = 1 if raw else 0
    if not raw:
        rejected_rows.append({"row_number": 1, "reason": "file is empty", "preview": name})
    job = {
        "job_id": "bulk-qa-001",
        "status": "DRY_RUN" if dry_run else "QUEUED",
        "dry_run": dry_run,
        "total_count": accepted_count + len(rejected_rows),
        "accepted_count": accepted_count,
        "rejected_count": len(rejected_rows),
        "rejected_rows": rejected_rows,
        "event_ids": [] if dry_run else ["evt-bulk-qa-001"],
        "source": f"bulk_{inferred}",
        "filename": name,
        "created_at": "2026-06-15T00:00:00Z",
        "completed_at": "2026-06-15T00:00:00Z",
    }
    return JSONResponse(job, status_code=202)


@app.get("/admin/api/ingest/bulk/{job_id}")
@app.get("/api/v1/ingest/bulk/{job_id}")
def ingest_bulk_status(job_id: str):
    return JSONResponse({
        "job_id": job_id,
        "status": "QUEUED",
        "dry_run": False,
        "total_count": 1,
        "accepted_count": 1,
        "rejected_count": 0,
        "rejected_rows": [],
        "event_ids": ["evt-bulk-qa-001"],
        "source": "bulk_jsonl",
        "filename": "turns.jsonl",
        "created_at": "2026-06-15T00:00:00Z",
        "completed_at": "2026-06-15T00:00:00Z",
    })


@app.post("/admin/api/sessions")
@app.post("/api/v1/sessions")
def create_session():
    return JSONResponse({"session_id": "sess-qa-new", "status": "ACTIVE"}, status_code=201)


@app.get("/admin/api/kg/graph")
@app.get("/api/v1/kg/graph")
def kg_graph(
    root_uri: str | None = None,
    depth: int = 1,
    limit: int = 100,
    type: str | None = None,
):
    nodes = [
        {
            "id": "mem://user/project-ideas",
            "source_uri": "mem://user/project-ideas",
            "label": "Project ideas",
            "node_type": "DOCUMENT",
            "status": "ACTIVE",
            "l0_abstract": "Idea for a retrieval-augmented memory system.",
        },
        {
            "id": "mem://user/entities/engram",
            "source_uri": "mem://user/entities/engram",
            "label": "Engram",
            "node_type": "ENTITY",
            "status": "ACTIVE",
            "l0_abstract": "Engram QA fixture entity.",
        },
    ]
    if type:
        nodes = [node for node in nodes if node["node_type"] == type]
    if root_uri:
        nodes = [node for node in nodes if node["source_uri"] == root_uri] or nodes[:1]
    nodes = nodes[: max(1, min(limit, 500))]
    node_ids = {node["id"] for node in nodes}
    edges = [
        {
            "source": "mem://user/project-ideas",
            "target": "mem://user/entities/engram",
            "type": "REFERENCES",
            "label": "mentions",
            "status": "ACTIVE",
        }
    ]
    edges = [edge for edge in edges if edge["source"] in node_ids and edge["target"] in node_ids]
    return JSONResponse({"nodes": nodes, "edges": edges, "limit": limit, "depth": depth})


@app.post("/admin/api/chat/completions")
@app.post("/api/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    session_id = body.get("session_id") or "sess-qa-new"
    if body.get("stream"):
        async def sse():
            yield "event: metadata\ndata: {\"levels_visited\":[\"L1\"],\"latency_ms\":{\"l1_execute\":12},\"nodes_retrieved\":2,\"cascade_depth_reached\":\"L1\",\"reentries\":0}\n\n"
            for chunk in ["Hello. ", "This is a simulated Engram chat response."]:
                yield f"event: delta\ndata: {json.dumps({'text': chunk})}\n\n"
            yield "event: done\ndata: {}\n\n"

        return StreamingResponse(sse(), media_type="text/event-stream")
    return JSONResponse({
        "id": "chatcmpl-qa",
        "object": "chat.completion",
        "created": 1781481600,
        "model": "qa",
        "session_id": session_id,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "Hello. This is a simulated Engram chat response."},
            "finish_reason": "stop",
        }],
    })


@app.post("/api/v1/query")
async def query_stream(request: Request):
    body = await request.json()
    is_stream = body.get("stream") or request.query_params.get("stream")
    if is_stream:
        async def sse():
            yield "event: metadata\ndata: {}\n\n"
            chunks = "Hello! " + "This is a simulated engram response. " + "It uses SSE streaming."
            for c in chunks.split(" "):
                if c:
                    yield f"event: delta\ndata: {json.dumps({'text': c + ' '})}\n\n"
            yield "event: done\ndata: {}\n\n"
        from fastapi.responses import StreamingResponse
        return StreamingResponse(sse(), media_type="text/event-stream")
    return JSONResponse({"answer": "Hello from engram query."})


app.mount("/admin/static", StaticFiles(directory=STATIC_DIR), name="admin_static")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9000, log_level="warning")
