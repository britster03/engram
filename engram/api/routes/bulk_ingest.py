"""Bulk ingest endpoints for JSONL, CSV, and ZIP uploads."""

from __future__ import annotations

import csv
import io
import json
import uuid
import zipfile
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel

from engram.api import schemas
from engram.api.auth import AuthDep
from engram.deps import get_state
from engram.tenancy import current_tenant_id
from engram.uri import pair_id as pair_id_fn

router = APIRouter(prefix="/api/v1/ingest/bulk", tags=["ingest"], dependencies=[AuthDep])

MAX_BULK_ROWS = 1000
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


class BulkRejectedRow(BaseModel):
    row_number: int
    reason: str
    preview: str | None = None


class BulkJobResponse(BaseModel):
    job_id: str
    status: str
    dry_run: bool
    total_count: int
    accepted_count: int
    rejected_count: int
    rejected_rows: list[BulkRejectedRow]
    event_ids: list[str]
    source: str
    filename: str | None = None
    created_at: str | None = None
    completed_at: str | None = None


@dataclass
class _Accepted:
    row_number: int
    request: schemas.IngestRequest


@router.post("", response_model=BulkJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_bulk_job(
    file: Annotated[UploadFile, File()],
    dry_run: Annotated[bool, Form()] = False,
    session_id: Annotated[str | None, Form()] = None,
    file_format: Annotated[
        Literal["jsonl", "csv", "zip"] | None, Form()
    ] = None,
) -> BulkJobResponse:
    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="bulk upload limit is 5 MiB")
    fmt = file_format or _infer_format(file.filename or "")
    if fmt is None:
        raise HTTPException(status_code=400, detail="cannot infer format; use jsonl, csv, or zip")

    job_id = f"bulk-{uuid.uuid4().hex[:12]}"
    source = f"bulk_{fmt}"
    if fmt == "jsonl":
        accepted, rejected = _parse_jsonl(raw, default_session_id=session_id, source=source)
    elif fmt == "csv":
        accepted, rejected = _parse_csv(raw, default_session_id=session_id, source=source)
    else:
        accepted, rejected = _parse_zip(
            raw, job_id=job_id, default_session_id=session_id, source=source
        )
    if len(accepted) + len(rejected) > MAX_BULK_ROWS:
        raise HTTPException(status_code=400, detail=f"bulk upload is limited to {MAX_BULK_ROWS} rows")

    state = get_state()
    tenant_id = current_tenant_id()
    event_ids: list[str] = []
    if not dry_run:
        for item in accepted:
            req = item.request
            pair = req.effective_pair()
            user_idx = pair.user.turn_idx or 0
            asst_idx = pair.assistant.turn_idx or (user_idx + 1)
            pid = pair_id_fn(req.session_id or job_id, user_idx, asst_idx)
            event_id, _ = state.sqlite.record_event(
                pair_id=pid,
                session_id=req.session_id,
                source=req.source,
                event_type="INGEST",
                payload=req.model_dump(),
                tenant_id=tenant_id,
            )
            event_ids.append(event_id)

    status_text = "DRY_RUN" if dry_run else "QUEUED"
    state.sqlite.save_bulk_job(
        job_id=job_id,
        tenant_id=tenant_id,
        source=source,
        filename=file.filename,
        dry_run=dry_run,
        status=status_text,
        total_count=len(accepted) + len(rejected),
        accepted_count=len(accepted),
        rejected_count=len(rejected),
        rejected_rows=[r.model_dump() for r in rejected],
        event_ids=event_ids,
    )
    return _bulk_response(
        {
            "job_id": job_id,
            "tenant_id": tenant_id,
            "source": source,
            "filename": file.filename,
            "dry_run": dry_run,
            "status": status_text,
            "total_count": len(accepted) + len(rejected),
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "rejected_rows": [r.model_dump() for r in rejected],
            "event_ids": event_ids,
        }
    )


@router.get("/{job_id}", response_model=BulkJobResponse)
def get_bulk_job(job_id: str) -> BulkJobResponse:
    state = get_state()
    tenant_id = current_tenant_id()
    job = state.sqlite.get_bulk_job(job_id, tenant_id=tenant_id)
    if job is None:
        raise HTTPException(status_code=404, detail="bulk job not found")
    _refresh_bulk_job_status(state, job, tenant_id=tenant_id)
    return _bulk_response(job)


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
    raw: bytes, *, default_session_id: str | None, source: str,
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
    raw: bytes, *, default_session_id: str | None, source: str,
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
    raw: bytes, *, job_id: str, default_session_id: str | None, source: str,
) -> tuple[list[_Accepted], list[BulkRejectedRow]]:
    accepted: list[_Accepted] = []
    rejected: list[BulkRejectedRow] = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile:
        return [], [_reject(1, "invalid ZIP archive", None)]
    logical_idx = 0
    for info in zf.infolist():
        if info.is_dir():
            continue
        name = info.filename
        if not name.lower().endswith((".txt", ".md")):
            rejected.append(_reject(logical_idx + 1, "unsupported ZIP member type", name))
            continue
        logical_idx += 1
        try:
            content = zf.read(info).decode("utf-8")
        except UnicodeDecodeError:
            rejected.append(_reject(logical_idx, "document is not UTF-8 text", name))
            continue
        if not content.strip():
            rejected.append(_reject(logical_idx, "document is empty", name))
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
                        content=content[:schemas.MAX_CONTENT_LENGTH],
                        turn_idx=(logical_idx - 1) * 2 + 1,
                    ),
                ),
            )
            accepted.append(_Accepted(row_number=logical_idx, request=req))
        except Exception as err:
            rejected.append(_reject(logical_idx, str(err), name))
    return accepted, rejected


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
    data: dict[str, Any], *, logical_idx: int, default_session_id: str | None, source: str,
) -> schemas.IngestRequest:
    if data.get("turn_pair") or data.get("turn_group"):
        payload = {
            **data,
            "session_id": data.get("session_id") or default_session_id or f"bulk-session-{logical_idx}",
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
            assistant=schemas.TurnContent(content=str(assistant), turn_idx=(logical_idx - 1) * 2 + 1),
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
        if int(job.get("accepted_count") or 0) == 0:
            _set_bulk_job_terminal(state, job, "COMPLETE", tenant_id=tenant_id)
        return

    events = [state.sqlite.get_event(event_id, tenant_id=tenant_id) for event_id in event_ids]
    if any(event is None for event in events):
        _set_bulk_job_terminal(state, job, "FAILED", tenant_id=tenant_id)
        return

    statuses = {str(event["status"]) for event in events if event is not None}
    if not statuses or statuses & {"RECEIVED", "PROCESSING"}:
        return
    if statuses <= {"COMPLETE", "GATED_SKIP", "INDEXED"}:
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
    state.sqlite.set_bulk_job_status(job["job_id"], status_text, tenant_id=tenant_id)
    updated = state.sqlite.get_bulk_job(job["job_id"], tenant_id=tenant_id)
    if updated is not None:
        job.update(updated)
    else:
        job["status"] = status_text


def _bulk_response(data: dict[str, Any]) -> BulkJobResponse:
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
        source=data["source"],
        filename=data.get("filename"),
        created_at=data.get("created_at"),
        completed_at=data.get("completed_at"),
    )
