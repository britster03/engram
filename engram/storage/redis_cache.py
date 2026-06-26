"""Session cache backend — Redis in production, in-memory for single-process dev (§9.2).

Keys are **tenant-namespaced** so two tenants cannot collide on a shared
session_id. Schema: `session:{tenant_id}:{session_id}`.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, cast

import redis

from engram.config import SessionCacheConfig
from engram.tenancy import current_tenant_id

log = logging.getLogger(__name__)


class SessionCache:
    """Key/value store keyed by session:{tenant_id}:{session_id} → serialized state."""

    def __init__(self, cfg: SessionCacheConfig, *, default_ttl_seconds: int) -> None:
        self.cfg = cfg
        self.default_ttl = default_ttl_seconds
        if cfg.backend == "redis":
            self._client: redis.Redis | None = redis.Redis.from_url(
                cfg.redis_url, decode_responses=True
            )
            self._memory: dict[str, tuple[float, str]] | None = {}
        else:
            self._client = None
            self._memory = {}
        self._redis_failed_logged = False

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
            try:
                raw = self._client.get(key)
                raw = cast(str | None, raw)
                return json.loads(raw) if raw else None
            except Exception as err:
                self._disable_redis(err)
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
            try:
                self._client.setex(key, ttl, raw)
                return
            except Exception as err:
                self._disable_redis(err)
        assert self._memory is not None
        self._memory[key] = (time.time() + ttl, raw)

    def delete(
        self, session_id: str, *, tenant_id: str | None = None
    ) -> None:
        key = self._key(session_id, tenant_id=tenant_id)
        if self._client is not None:
            try:
                self._client.delete(key)
                return
            except Exception as err:
                self._disable_redis(err)
        assert self._memory is not None
        self._memory.pop(key, None)

    def list_sessions(self, *, tenant_id: str | None = None) -> list[dict[str, Any]]:
        """Return all stored sessions for the current tenant as raw dicts."""
        tid = tenant_id or current_tenant_id()
        prefix = f"session:{tid}:"
        results: list[dict[str, Any]] = []
        if self._client is not None:
            try:
                for key in self._client.scan_iter(match=f"{prefix}*"):
                    raw = self._client.get(key)
                    raw = cast(str | None, raw)
                    if raw:
                        results.append(json.loads(raw))
                return results
            except Exception as err:
                self._disable_redis(err)
        assert self._memory is not None
        now = time.time()
        for key, (expires, raw) in list(self._memory.items()):
            if key.startswith(prefix) and expires >= now:
                results.append(json.loads(raw))
        return results

    def _disable_redis(self, err: Exception) -> None:
        if not self._redis_failed_logged:
            log.warning("Redis session cache unavailable (%s); using in-memory fallback", err)
            self._redis_failed_logged = True
        self._client = None
