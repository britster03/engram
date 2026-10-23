"""Request identities independent of conversation session/turn metadata."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any


class IdempotencyConflictError(ValueError):
    """An existing request identity was reused for different input."""


def request_pair_id(key: str | None = None, *, namespace: str = "ingest-request") -> str:
    """Use a stable, namespaced client key, or a fresh identity for a new request."""
    identity = key if key is not None else uuid.uuid4().hex
    return hashlib.sha256(f"{namespace}:{identity}".encode()).hexdigest()


def payload_fingerprint(payload: dict[str, Any]) -> str:
    """Fingerprint JSON values independently of dictionary insertion order."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()
