"""Per-endpoint body-size middleware.

Rejects requests whose bodies exceed a configured ceiling before any
parsing happens. Applied globally with per-path overrides so that
`/api/v1/ingest/batch` can accept larger payloads than `/api/v1/query`
without loosening limits everywhere.
"""

from __future__ import annotations

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

DEFAULT_LIMIT = 128 * 1024           # 128 KiB default for any POST/PATCH body
QUERY_LIMIT = 16 * 1024              # 16 KiB for /api/v1/query
INGEST_LIMIT = 256 * 1024            # 256 KiB per single ingest
BATCH_LIMIT = 4 * 1024 * 1024        # 4 MiB for /api/v1/ingest/batch (100 items max)


def _limit_for(path: str) -> int:
    if path.startswith("/api/v1/ingest/batch"):
        return BATCH_LIMIT
    if path.startswith("/api/v1/ingest"):
        return INGEST_LIMIT
    if path.startswith("/api/v1/query"):
        return QUERY_LIMIT
    if path.endswith("/message") and path.startswith("/api/v1/sessions/"):
        return INGEST_LIMIT
    return DEFAULT_LIMIT


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Enforce per-path Content-Length caps.

    Trusts `Content-Length` when present; otherwise reads the body into a
    size-bounded buffer. Returns 413 Payload Too Large on violation.
    """

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        method = request.method.upper()
        if method not in {"POST", "PUT", "PATCH"}:
            return await call_next(request)
        limit = _limit_for(request.url.path)
        cl = request.headers.get("content-length")
        if cl is not None:
            try:
                declared = int(cl)
            except ValueError:
                return JSONResponse(
                    {"detail": "invalid content-length"}, status_code=400
                )
            if declared > limit:
                return JSONResponse(
                    {"detail": f"body exceeds {limit} bytes"},
                    status_code=413,
                )
        # No Content-Length — read and count. Starlette's Request caches the
        # body so downstream handlers don't re-read it.
        body = await request.body()
        if len(body) > limit:
            return JSONResponse(
                {"detail": f"body exceeds {limit} bytes"},
                status_code=413,
            )
        return await call_next(request)
