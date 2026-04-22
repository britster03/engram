"""Session cache backend — Redis in production, in-memory for single-process dev (§9.2).

Keys are **tenant-namespaced** so two tenants cannot collide on a shared
session_id. Schema: `session:{tenant_id}:{session_id}`.
"""

from __future__ import annotations

import json
import time
from typing import Any

import redis

from engram.config import SessionCacheConfig
from engram.tenancy import current_tenant_id


class SessionCache:
    """Key/value store keyed by session:{tenant_id}:{session_id} → serialized state."""

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

    def _key(self, session_id: str, *, tenant_id: str | None = None) -> str:
        tid = tenant_id or current_tenant_id()
        return f"session:{tid}:{session_id}"

    def get(
        self, session_id: str, *, tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        key = self._key(session_id, tenant_id=tenant_id)
        if self._client is not None:
            raw = self._client.get(key)
            return json.loads(raw) if raw else None
        assert self._memory is not None
        entry = self._memory.get(key)
        if entry is None:
            return None
        expires, raw = entry
        if expires < time.time():
            self._memory.pop(key, None)
            return None
        return json.loads(raw)

    def set(
        self,
        session_id: str,
        value: dict[str, Any],
        *,
        ttl_seconds: int | None = None,
        tenant_id: str | None = None,
    ) -> None:
        key = self._key(session_id, tenant_id=tenant_id)
        raw = json.dumps(value)
        ttl = ttl_seconds or self.default_ttl
        if self._client is not None:
            self._client.setex(key, ttl, raw)
        else:
            assert self._memory is not None
            self._memory[key] = (time.time() + ttl, raw)

    def delete(
        self, session_id: str, *, tenant_id: str | None = None,
    ) -> None:
        key = self._key(session_id, tenant_id=tenant_id)
        if self._client is not None:
            self._client.delete(key)
        else:
            assert self._memory is not None
            self._memory.pop(key, None)
