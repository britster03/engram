"""Request-ID middleware.

Honours an incoming `X-Request-ID` header when present (so upstream proxies
can correlate), otherwise generates one. The ID is:
  - placed into the logging contextvar (every log record tagged)
  - added to the response as `X-Request-ID`
"""

from __future__ import annotations

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from engram.logging_setup import new_request_id, set_request_id


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        rid = request.headers.get("x-request-id") or new_request_id()
        set_request_id(rid)
        try:
            response = await call_next(request)
        finally:
            set_request_id("")
        response.headers["X-Request-ID"] = rid
        return response
