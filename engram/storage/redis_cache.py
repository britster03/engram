"""Session cache backend — Redis in production, in-memory for single-process dev (§9.2)."""

from __future__ import annotations

import json
import time
from typing import Any

import redis

from engram.config import SessionCacheConfig


class SessionCache:
    """Key/value store keyed by session:{session_id} → serialized session state."""

    def __init__(self, cfg: SessionCacheConfig, *, default_ttl_seconds: int) -> None:
        self.cfg = cfg
        self.default_ttl = default_ttl_seconds
        if cfg.backend == "redis":
            self._client: redis.Redis | None = redis.Redis.from_url(
                cfg.redis_url, decode_responses=True
            )
            self._memory: dict[str, tuple[float, str]] | None = None
        else:
            self._client = None
            self._memory = {}

    def ping(self) -> bool:
        if self._client is not None:
            try:
                return bool(self._client.ping())
            except Exception:
                return False
        return True

    def _key(self, session_id: str) -> str:
        return f"session:{session_id}"

    def get(self, session_id: str) -> dict[str, Any] | None:
        if self._client is not None:
            raw = self._client.get(self._key(session_id))
            return json.loads(raw) if raw else None
        assert self._memory is not None
        entry = self._memory.get(session_id)
        if entry is None:
            return None
        expires, raw = entry
        if expires < time.time():
            self._memory.pop(session_id, None)
            return None
        return json.loads(raw)

    def set(
        self, session_id: str, value: dict[str, Any], *, ttl_seconds: int | None = None
    ) -> None:
        raw = json.dumps(value)
        ttl = ttl_seconds or self.default_ttl
        if self._client is not None:
            self._client.setex(self._key(session_id), ttl, raw)
        else:
            assert self._memory is not None
            self._memory[session_id] = (time.time() + ttl, raw)

    def delete(self, session_id: str) -> None:
        if self._client is not None:
            self._client.delete(self._key(session_id))
        else:
            assert self._memory is not None
            self._memory.pop(session_id, None)
