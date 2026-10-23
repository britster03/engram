"""Request-scoped metadata for model-provider calls.

Providers are long-lived singletons, while a model request belongs to one
Engram conversation or background-work item.  ``ContextVar`` keeps that
metadata isolated between concurrent request threads without changing every
provider interface.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from hashlib import sha256
from uuid import uuid4

_session_id: ContextVar[str | None] = ContextVar("engram_model_session_id", default=None)
_HEADER_VALUE = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")
_FALLBACK_SESSION_ID = f"engram-process-{uuid4().hex}"


@contextmanager
def model_request_session(session_id: str | None) -> Iterator[None]:
    """Bind a stable, header-safe model session ID for the current request."""
    value = str(session_id).strip() if session_id else None
    if value and not _HEADER_VALUE.fullmatch(value):
        # Background work often uses a mem:// URI.  Preserve its stability
        # without placing URI characters into an HTTP header.
        value = f"engram-{sha256(value.encode('utf-8')).hexdigest()}"
    token = _session_id.set(value)
    try:
        yield
    finally:
        _session_id.reset(token)


def opencode_headers(api_base: str | None) -> dict[str, str]:
    """Return OpenCode-specific headers for the active model request.

    OpenCode Go requires a stable ``x-opencode-session`` for each
    conversation.  Other OpenAI-compatible providers must not receive this
    vendor-specific header.
    """
    if not api_base or "opencode.ai" not in api_base.lower():
        return {}
    session_id = _session_id.get()
    session_id = session_id or _FALLBACK_SESSION_ID
    return {
        "User-Agent": "engram/0.1.0",
        "x-opencode-session": session_id,
    }
