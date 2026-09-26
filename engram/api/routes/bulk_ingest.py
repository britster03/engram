"""Bulk ingest endpoints for JSONL, CSV, and ZIP uploads."""

from __future__ import annotations

import csv
import io
import json
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel, Field
from slugify import slugify

from engram.api import schemas
from engram.api.auth import AuthDep
from engram.codegraph import CODE_SUFFIXES, CodeSourceFile, analyze_project, attach_embeddings
from engram.deps import get_state
from engram.ingest.code_ingest import enqueue_code_archive
from engram.projection_status import projection_status_for_event
from engram.tenancy import current_tenant_id
from engram.uri import pair_id as pair_id_fn

router = APIRouter(prefix="/api/v1/ingest/bulk", tags=["ingest"], dependencies=[AuthDep])

MAX_BULK_ROWS = 1000
MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_ZIP_MEMBER_BYTES = 10 * 1024 * 1024
MAX_ZIP_EXPANDED_BYTES = 50 * 1024 * 1024


class BulkRejectedRow(BaseModel):
    row_number: int
    reason: str
    preview: str | None = None


class BulkEventResponse(BaseModel):
    """User-facing progress for one item accepted from a bulk upload."""

    event_id: str
    label: str
    status: str
    graph_status: str | None = None
    retry_count: int = 0
    error_message: str | None = None
    created_at: str | None = None
    processed_at: str | None = None


class BulkCodeItemResponse(BaseModel):
    """Immediate, deterministic source-code analysis status for one ZIP member."""

    path: str
    status: str
    node_count: int = 0
    relationship_count: int = 0
    error_message: str | None = None
    project_uri: str | None = None


class BulkJobResponse(BaseModel):
    job_id: str
    status: str
    dry_run: bool
    total_count: int
    accepted_count: int
    rejected_count: int
    rejected_rows: list[BulkRejectedRow]
    event_ids: list[str]
    events: list[BulkEventResponse] = Field(default_factory=list)
    source: str
    filename: str | None = None
    created_at: str | None = None
    completed_at: str | None = None
    project_uri: str | None = None
    code_items: list[BulkCodeItemResponse] = Field(default_factory=list)


@dataclass
class _Accepted:
    row_number: int
    request: schemas.IngestRequest


@router.post("", response_model=BulkJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_bulk_job(
    file: Annotated[UploadFile, File()],
    dry_run: Annotated[bool, Form()] = False,
    force_store: Annotated[bool, Form()] = False,
    session_id: Annotated[str | None, Form()] = None,
    project_name: Annotated[str | None, Form()] = None,
    file_format: Annotated[Literal["jsonl", "csv", "zip"] | None, Form()] = None,
) -> BulkJobResponse:
    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="bulk upload limit is 5 MiB")
    fmt = file_format or _infer_format(file.filename or "")
    if fmt is None:
        raise HTTPException(status_code=400, detail="cannot infer format; use jsonl, csv, or zip")

    job_id = f"bulk-{uuid.uuid4().hex[:12]}"
    source = f"bulk_{fmt}"
    code_sources: list[CodeSourceFile] = []
    if fmt == "jsonl":
        accepted, rejected = _parse_jsonl(raw, default_session_id=session_id, source=source)
    elif fmt == "csv":
        accepted, rejected = _parse_csv(raw, default_session_id=session_id, source=source)
    else:
        accepted, rejected, code_sources = _parse_zip(
            raw, job_id=job_id, default_session_id=session_id, source=source
        )
    if len(accepted) + len(rejected) + len(code_sources) > MAX_BULK_ROWS:
        raise HTTPException(
            status_code=400, detail=f"bulk upload is limited to {MAX_BULK_ROWS} rows"
        )

    state = get_state()
    tenant_id = current_tenant_id()
    if code_sources and not (project_name or "").strip():
        raise HTTPException(
            status_code=422,
            detail="project_name is required when a ZIP contains Python, TypeScript, or JavaScript files",
        )

    project_uri: str | None = None
    code_items: list[dict[str, Any]] = []
    code_failed = False
    queue_code_ingest = bool(code_sources and state.cfg.canonical_memory.enabled and not dry_run)
    if code_sources:
        if queue_code_ingest:
            project_uri = f"mem://projects/{slugify((project_name or '').strip(), separator='-')}"
            code_items = [
                {
                    "row_number": index,
                    "path": source_file.path,
                    "status": "QUEUED",
                    "node_count": 0,
                    "relationship_count": 0,
                    "error_message": None,
                }
                for index, source_file in enumerate(code_sources, start=1)
            ]
        else:
            graph = analyze_project((project_name or "").strip(), code_sources, core=state.core)
            project_uri = graph.project_uri
            if graph.ok and not dry_run:
                try:
                    attach_embeddings(graph, state.embed)
                    state.neo4j.replace_code_project(
                        project_uri=project_uri,
                        nodes=graph.storage_nodes(),
                        edges=graph.storage_edges(),
                        tenant_id=tenant_id,
                    )
                except Exception as err:
                    code_failed = True
                    code_items = [
                        {
                            "row_number": index,
                            "path": result.path,
                            "status": "FAILED",
                            "node_count": result.node_count,
                            "relationship_count": result.relationship_count,
                            "error_message": f"Graph indexing failed: {err}",
                        }
                        for index, result in enumerate(graph.file_results, start=1)
                    ]
            if not code_items:
                code_failed = not graph.ok
                status_name = "VALIDATED" if dry_run else ("INDEXED" if graph.ok else "FAILED")
                code_items = [
                    {
                        "row_number": index,
                        "path": result.path,
                        "status": status_name if result.status != "FAILED" else "FAILED",
                        "node_count": result.node_count,
                        "relationship_count": result.relationship_count,
                        "error_message": result.error_message,
                    }
                    for index, result in enumerate(graph.file_results, start=1)
                ]
    event_ids: list[str] = []
    if not dry_run:
        for item in accepted:
            req = item.request
            pair = req.effective_pair()
            user_idx = pair.user.turn_idx or 0
            asst_idx = pair.assistant.turn_idx or (user_idx + 1)
            pid = pair_id_fn(req.session_id or job_id, user_idx, asst_idx)
            event_id, _ = state.control_plane.record_event(
                pair_id=pid,
                session_id=req.session_id,
                source=req.source,
                event_type="INGEST",
                payload={**req.model_dump(), "force_store": force_store},
                tenant_id=tenant_id,
            )
            event_ids.append(event_id)
    if queue_code_ingest:
        event_ids.append(
            enqueue_code_archive(
                state,
                job_id=job_id,
                tenant_id=tenant_id,
                project_name=(project_name or "").strip(),
                filename=file.filename or "project.zip",
                content=raw,
            )
        )

    status_text = (
        "DRY_RUN"
        if dry_run
        else (
            "QUEUED"
            if event_ids or queue_code_ingest
            else ("FAILED" if code_failed else "COMPLETE")
        )
    )
    state.control_plane.save_bulk_job(
        job_id=job_id,
        tenant_id=tenant_id,
        source=source,
        filename=file.filename,
        dry_run=dry_run,
        status=status_text,
        total_count=len(accepted) + len(rejected) + len(code_sources),
        accepted_count=len(accepted) + len(code_sources),
        rejected_count=len(rejected),
        rejected_rows=[r.model_dump() for r in rejected],
        event_ids=event_ids,
    )
    if code_items:
        state.control_plane.save_bulk_code_items(
            job_id=job_id,
            tenant_id=tenant_id,
            project_uri=project_uri,
            items=code_items,
        )
    return _bulk_response(
        {
            "job_id": job_id,
            "tenant_id": tenant_id,
            "source": source,
            "filename": file.filename,
            "dry_run": dry_run,
            "status": status_text,
            "total_count": len(accepted) + len(rejected) + len(code_sources),
            "accepted_count": len(accepted) + len(code_sources),
            "rejected_count": len(rejected),
            "rejected_rows": [r.model_dump() for r in rejected],
            "event_ids": event_ids,
            "project_uri": project_uri,
        },
        state=state,
        tenant_id=tenant_id,
    )


@router.get("/{job_id}", response_model=BulkJobResponse)
def get_bulk_job(job_id: str) -> BulkJobResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    job = state.control_plane.get_bulk_job(job_id, tenant_id=tenant_id)
    if job is None:
        raise HTTPException(status_code=404, detail="bulk job not found")
    _refresh_bulk_job_status(state, job, tenant_id=tenant_id)
    return _bulk_response(job, state=state, tenant_id=tenant_id)


@router.post("/{job_id}/events/{event_id}/retry", response_model=BulkEventResponse)
def retry_failed_bulk_event(job_id: str, event_id: str) -> BulkEventResponse:
    """Requeue exactly one failed upload item; successful items stay untouched."""
    state = get_state()
    tenant_id = current_tenant_id()
    job = state.control_plane.get_bulk_job(job_id, tenant_id=tenant_id)
    if job is None:
        raise HTTPException(status_code=404, detail="bulk job not found")
    if event_id not in set(job.get("event_ids") or []):
        raise HTTPException(status_code=404, detail="event does not belong to this bulk job")
    event = state.control_plane.get_event(event_id, tenant_id=tenant_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    if event.get("status") != "FAILED":
        raise HTTPException(status_code=409, detail="only failed upload items can be retried")

    if hasattr(state.control_plane, "retry_event") and state.cfg.temporal.enabled:
        refreshed = state.control_plane.retry_event(event_id, tenant_id=tenant_id)
        if refreshed is None:
            raise HTTPException(status_code=409, detail="only failed upload items can be retried")
    else:
        with state.control_plane.transaction() as conn:
            conn.execute(
                "UPDATE events SET status = 'RECEIVED', error_message = NULL, "
                "retry_count = retry_count + 1, processed_at = NULL "
                "WHERE event_id = ? AND tenant_id = ? AND status = 'FAILED'",
                (event_id, tenant_id),
            )
    state.control_plane.requeue_bulk_job(job_id, tenant_id=tenant_id)
    refreshed = (
        state.control_plane.get_event(event_id, tenant_id=tenant_id)
        if not (hasattr(state.control_plane, "retry_event") and state.cfg.temporal.enabled)
        else refreshed
    )
    if refreshed is None:  # defensive: the event was checked above
        raise HTTPException(status_code=404, detail="event not found")
    return _bulk_event_response(state, refreshed, tenant_id=tenant_id)


def _infer_format(filename: str) -> Literal["jsonl", "csv", "zip"] | None:
    lower = filename.lower()
    if lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        return "jsonl"
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith(".zip"):
        return "zip"
    return None


def _parse_jsonl(
    raw: bytes,
    *,
    default_session_id: str | None,
    source: str,
) -> tuple[list[_Accepted], list[BulkRejectedRow]]:
    accepted: list[_Accepted] = []
    rejected: list[BulkRejectedRow] = []
    text = raw.decode("utf-8-sig")
    logical_idx = 0
    for row_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError as err:
            rejected.append(_reject(row_number, f"invalid JSON: {err.msg}", line))
            continue
        logical_idx += 1
        _accept_mapping(
            data,
            row_number=row_number,
            logical_idx=logical_idx,
            default_session_id=default_session_id,
            source=source,
            accepted=accepted,
            rejected=rejected,
        )
    return accepted, rejected


def _parse_csv(
    raw: bytes,
    *,
    default_session_id: str | None,
    source: str,
) -> tuple[list[_Accepted], list[BulkRejectedRow]]:
    text = raw.decode("utf-8-sig")
    accepted: list[_Accepted] = []
    rejected: list[BulkRejectedRow] = []
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return [], [_reject(1, "CSV header is required", text[:200])]
    for logical_idx, row in enumerate(reader, start=1):
        _accept_mapping(
            dict(row),
            row_number=logical_idx + 1,
            logical_idx=logical_idx,
            default_session_id=default_session_id,
            source=source,
            accepted=accepted,
            rejected=rejected,
        )
    return accepted, rejected


def _parse_zip(
    raw: bytes,
    *,
    job_id: str,
    default_session_id: str | None,
    source: str,
) -> tuple[list[_Accepted], list[BulkRejectedRow], list[CodeSourceFile]]:
    accepted: list[_Accepted] = []
    rejected: list[BulkRejectedRow] = []
    code_sources: list[CodeSourceFile] = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        return [], [_reject(1, "invalid ZIP archive", None)], []
    members = [info for info in zf.infolist() if not info.is_dir()]
    if len(members) > MAX_BULK_ROWS:
        zf.close()
        return (
            [],
            [_reject(1, f"ZIP archive is limited to {MAX_BULK_ROWS} files", None)],
            [],
        )
    if sum(info.file_size for info in members) > MAX_ZIP_EXPANDED_BYTES:
        zf.close()
        return (
            [],
            [_reject(1, "ZIP expanded content limit is 50 MiB", None)],
            [],
        )
    logical_idx = 0
    for info in members:
        name = info.filename
        suffix = name.lower().rsplit(".", 1)[-1] if "." in name else ""
        if name.startswith("/") or ".." in name.replace("\\", "/").split("/"):
            rejected.append(_reject(logical_idx + 1, "unsafe ZIP member path", name))
            continue
        if info.flag_bits & 0x1:
            rejected.append(
                _reject(logical_idx + 1, "encrypted ZIP members are not supported", name)
            )
            continue
        if info.file_size > MAX_ZIP_MEMBER_BYTES:
            rejected.append(_reject(logical_idx + 1, "ZIP member limit is 10 MiB", name))
            continue
        if not name.lower().endswith((".txt", ".md", ".py", ".ts", ".tsx", ".js", ".jsx")):
            rejected.append(_reject(logical_idx + 1, "unsupported ZIP member type", name))
            continue
        logical_idx += 1
        try:
            content_bytes = zf.read(info)
        except (RuntimeError, zipfile.BadZipFile, OSError):
            rejected.append(_reject(logical_idx, "cannot read ZIP member", name))
            continue
        try:
            content = content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            rejected.append(_reject(logical_idx, "document is not UTF-8 text", name))
            continue
        if not content.strip():
            rejected.append(_reject(logical_idx, "document is empty", name))
            continue
        if f".{suffix}" in CODE_SUFFIXES:
            code_sources.append(CodeSourceFile(path=name, content=content))
            continue
        try:
            req = schemas.IngestRequest(
                session_id=default_session_id or job_id,
                source=source,
                turn_pair=schemas.TurnPair(
                    user=schemas.TurnContent(
                        content=f"Uploaded document: {name}", turn_idx=(logical_idx - 1) * 2
                    ),
                    assistant=schemas.TurnContent(
                        content=content[: schemas.MAX_CONTENT_LENGTH],
                        turn_idx=(logical_idx - 1) * 2 + 1,
                    ),
                ),
            )
            accepted.append(_Accepted(row_number=logical_idx, request=req))
        except Exception as err:
            rejected.append(_reject(logical_idx, str(err), name))
    zf.close()
    return accepted, rejected, code_sources


def _accept_mapping(
    data: Any,
    *,
    row_number: int,
    logical_idx: int,
    default_session_id: str | None,
    source: str,
    accepted: list[_Accepted],
    rejected: list[BulkRejectedRow],
) -> None:
    if not isinstance(data, dict):
        rejected.append(_reject(row_number, "row must be an object", repr(data)))
        return
    try:
        req = _request_from_mapping(
            data,
            logical_idx=logical_idx,
            default_session_id=default_session_id,
            source=source,
        )
        req.effective_pair()
    except Exception as err:
        rejected.append(_reject(row_number, str(err), json.dumps(data, default=str)[:200]))
        return
    accepted.append(_Accepted(row_number=row_number, request=req))


def _request_from_mapping(
    data: dict[str, Any],
    *,
    logical_idx: int,
    default_session_id: str | None,
    source: str,
) -> schemas.IngestRequest:
    if data.get("turn_pair") or data.get("turn_group"):
        payload = {
            **data,
            "session_id": data.get("session_id")
            or default_session_id
            or f"bulk-session-{logical_idx}",
            "source": data.get("source") or source,
        }
        req = schemas.IngestRequest.model_validate(payload)
        pair = req.effective_pair()
        if pair.user.turn_idx is None:
            pair.user.turn_idx = (logical_idx - 1) * 2
        if pair.assistant.turn_idx is None:
            pair.assistant.turn_idx = (logical_idx - 1) * 2 + 1
        return req

    user = data.get("user") or data.get("user_message") or data.get("prompt")
    assistant = data.get("assistant") or data.get("assistant_message") or data.get("response")
    if not user or not assistant:
        raise ValueError("row requires user and assistant fields")
    return schemas.IngestRequest(
        session_id=data.get("session_id") or default_session_id or f"bulk-session-{logical_idx}",
        source=data.get("source") or source,
        turn_pair=schemas.TurnPair(
            user=schemas.TurnContent(content=str(user), turn_idx=(logical_idx - 1) * 2),
            assistant=schemas.TurnContent(
                content=str(assistant), turn_idx=(logical_idx - 1) * 2 + 1
            ),
        ),
    )


def _reject(row_number: int, reason: str, preview: str | None) -> BulkRejectedRow:
    return BulkRejectedRow(
        row_number=row_number,
        reason=reason[:300],
        preview=(preview[:200] if preview else None),
    )


def _refresh_bulk_job_status(state: Any, job: dict[str, Any], *, tenant_id: str) -> None:
    if job.get("dry_run") or job.get("status") not in {"QUEUED", "PROCESSING"}:
        return
    event_ids = list(job.get("event_ids") or [])
    if not event_ids:
        code_items = state.control_plane.get_bulk_code_items(job["job_id"], tenant_id=tenant_id)
        if any(item.get("status") == "FAILED" for item in code_items):
            _set_bulk_job_terminal(state, job, "FAILED", tenant_id=tenant_id)
        elif code_items or int(job.get("accepted_count") or 0) == 0:
            _set_bulk_job_terminal(state, job, "COMPLETE", tenant_id=tenant_id)
        return

    events = [
        state.control_plane.get_event(event_id, tenant_id=tenant_id) for event_id in event_ids
    ]
    if any(event is None for event in events):
        _set_bulk_job_terminal(state, job, "FAILED", tenant_id=tenant_id)
        return

    statuses = {str(event["status"]) for event in events if event is not None}
    if not statuses:
        return
    # The API returns 202 before the worker has claimed an event. Once at
    # least one event is claimed or reaches an intermediate ingest state, the
    # Admin UI should show real progress rather than a misleading QUEUED label.
    if statuses & {"PROCESSING", "GATED_STORE", "INDEXED"}:
        state.control_plane.mark_bulk_job_processing(job["job_id"], tenant_id=tenant_id)
        job["status"] = "PROCESSING"
        return
    if statuses & {"RECEIVED"}:
        return
    code_items = state.control_plane.get_bulk_code_items(job["job_id"], tenant_id=tenant_id)
    code_failed = any(item.get("status") == "FAILED" for item in code_items)
    if statuses <= {"COMPLETE", "GATED_SKIP", "INDEXED"} and not code_failed:
        _set_bulk_job_terminal(state, job, "COMPLETE", tenant_id=tenant_id)
    elif statuses <= {"COMPLETE", "GATED_SKIP", "INDEXED", "FAILED"}:
        _set_bulk_job_terminal(state, job, "FAILED", tenant_id=tenant_id)


def _set_bulk_job_terminal(
    state: Any,
    job: dict[str, Any],
    status_text: str,
    *,
    tenant_id: str,
) -> None:
    state.control_plane.set_bulk_job_status(job["job_id"], status_text, tenant_id=tenant_id)
    updated = state.control_plane.get_bulk_job(job["job_id"], tenant_id=tenant_id)
    if updated is not None:
        job.update(updated)
    else:
        job["status"] = status_text


def _bulk_response(
    data: dict[str, Any],
    *,
    state: Any | None = None,
    tenant_id: str | None = None,
) -> BulkJobResponse:
    events: list[BulkEventResponse] = []
    code_items: list[BulkCodeItemResponse] = []
    project_uri = data.get("project_uri")
    if state is not None and tenant_id is not None:
        for event_id in data.get("event_ids", []):
            event = state.control_plane.get_event(event_id, tenant_id=tenant_id)
            if event is not None:
                events.append(_bulk_event_response(state, event, tenant_id=tenant_id))
        code_rows = state.control_plane.get_bulk_code_items(data["job_id"], tenant_id=tenant_id)
        code_items = [
            BulkCodeItemResponse(
                path=row["path"],
                status=row["status"],
                node_count=int(row.get("node_count") or 0),
                relationship_count=int(row.get("relationship_count") or 0),
                error_message=row.get("error_message"),
                project_uri=row.get("project_uri"),
            )
            for row in code_rows
        ]
        if not project_uri and code_rows:
            project_uri = code_rows[0].get("project_uri")
    return BulkJobResponse(
        job_id=data["job_id"],
        status=data["status"],
        dry_run=bool(data["dry_run"]),
        total_count=int(data["total_count"]),
        accepted_count=int(data["accepted_count"]),
        rejected_count=int(data["rejected_count"]),
        rejected_rows=[
            r if isinstance(r, BulkRejectedRow) else BulkRejectedRow(**r)
            for r in data.get("rejected_rows", [])
        ],
        event_ids=list(data.get("event_ids", [])),
        events=events,
        source=data["source"],
        filename=data.get("filename"),
        created_at=_timestamp_string(data.get("created_at")),
        completed_at=_timestamp_string(data.get("completed_at")),
        project_uri=project_uri,
        code_items=code_items,
    )


def _bulk_event_response(state: Any, event: dict[str, Any], *, tenant_id: str) -> BulkEventResponse:
    return BulkEventResponse(
        event_id=event["event_id"],
        label=_event_label(event),
        status=str(event.get("status") or "RECEIVED"),
        graph_status=projection_status_for_event(
            state, str(event["event_id"]), tenant_id=tenant_id
        ),
        retry_count=int(event.get("retry_count") or 0),
        error_message=event.get("error_message"),
        created_at=_timestamp_string(event.get("created_at")),
        processed_at=_timestamp_string(event.get("processed_at")),
    )


def _timestamp_string(value: object | None) -> str | None:
    """Normalize PostgreSQL datetime values for API output."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _event_label(event: dict[str, Any]) -> str:
    """Give ZIP members and tabular rows a useful label without new persistence."""
    payload = event.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {}
    pair = payload.get("turn_pair") or payload.get("turn_group") or {}
    user = pair.get("user") or {}
    content = str(user.get("content") or "").strip()
    prefix = "Uploaded document: "
    if content.startswith(prefix):
        return content[len(prefix) :].strip() or "Uploaded document"
    if content:
        return content[:100]
    return f"Upload item {event['event_id']}"
