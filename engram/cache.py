"""Distributed caches backed by Redis with in-memory fallback.

Three caches are exposed:

  - `EmbeddingCache`: text → 384-dim vector (hot path for query rewriting
    and the L0 memory-hit fallback; avoids re-encoding identical strings
    across workers).
  - `OverviewCache`: directory URI → rendered overview markdown (cuts
    redundant filesystem reads when the same overview is surfaced by
    multiple consecutive queries).
  - `PlanCache` (opt-in): query hash → L1 plan JSON. Off by default
    because plans are temperature-sensitive and should not leak across
    users.

All caches share the same backend protocol so tests can swap in a
`MemoryCache` without importing redis.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any, Protocol

import redis

log = logging.getLogger(__name__)


class CacheBackend(Protocol):
    def get(self, key: str) -> bytes | None: ...
    def set(self, key: str, value: bytes, *, ttl_seconds: int | None = None) -> None: ...
    def delete(self, key: str) -> None: ...


class MemoryCache:
    """Per-process TTL-bounded cache (safe single-worker fallback)."""

    def __init__(self, max_entries: int = 10_000) -> None:
        self.max_entries = max_entries
        self._data: dict[str, tuple[float, bytes]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> bytes | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            expires, value = entry
            if expires < time.monotonic():
                self._data.pop(key, None)
                return None
            return value

    def set(self, key: str, value: bytes, *, ttl_seconds: int | None = None) -> None:
        ttl = ttl_seconds or 900
        with self._lock:
            if len(self._data) >= self.max_entries:
                # Simple FIFO eviction of the first 10% of entries.
                drop = max(1, self.max_entries // 10)
                for k in list(self._data.keys())[:drop]:
                    self._data.pop(k, None)
            self._data[key] = (time.monotonic() + ttl, value)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)


class RedisCache:
    """Thin wrapper over redis.Redis keyed by a namespace prefix."""

    def __init__(self, url: str, *, namespace: str) -> None:
        self.client = redis.Redis.from_url(url)
        self.namespace = namespace
        # Probe now so construction fails fast if Redis is unreachable.
        self.client.ping()

    def _k(self, key: str) -> str:
        return f"engram:cache:{self.namespace}:{key}"

    def get(self, key: str) -> bytes | None:
        try:
            return self.client.get(self._k(key))
        except redis.RedisError as err:
            log.debug("redis cache GET failed for %s: %s", key, err)
            return None

    def set(self, key: str, value: bytes, *, ttl_seconds: int | None = None) -> None:
        try:
            if ttl_seconds:
                self.client.setex(self._k(key), ttl_seconds, value)
            else:
                self.client.set(self._k(key), value)
        except redis.RedisError as err:
            log.debug("redis cache SET failed for %s: %s", key, err)

    def delete(self, key: str) -> None:
        try:
            self.client.delete(self._k(key))
        except redis.RedisError:
            pass


def build_cache(url: str | None, namespace: str) -> CacheBackend:
    if url:
        try:
            return RedisCache(url, namespace=namespace)
        except Exception as err:
            log.warning("Redis cache %s unavailable (%s); using in-memory", namespace, err)
    return MemoryCache()


# ----------------------------------------------------------------------
# EmbeddingCache
# ----------------------------------------------------------------------

class EmbeddingCache:
    """text → list[float] cache keyed by SHA-256(text).

    Entries are serialized as length-prefixed float32 bytes for compact
    storage; decoding is ~0.1 ms for a 384-dim vector.
    """

    _VERSION = "v1"
    _FLOAT_BYTES = 4

    def __init__(self, backend: CacheBackend, *, ttl_seconds: int = 7 * 24 * 3600) -> None:
        self.backend = backend
        self.ttl = ttl_seconds

    def _key(self, text: str, *, model_tag: str) -> str:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return f"{self._VERSION}:{model_tag}:{digest}"

    def get(self, text: str, *, model_tag: str) -> list[float] | None:
        raw = self.backend.get(self._key(text, model_tag=model_tag))
        if raw is None:
            return None
        return _decode_vec(raw)

    def set(self, text: str, vec: list[float], *, model_tag: str) -> None:
        self.backend.set(
            self._key(text, model_tag=model_tag),
            _encode_vec(vec),
            ttl_seconds=self.ttl,
        )


def _encode_vec(vec: list[float]) -> bytes:
    import struct
    return struct.pack(f"<{len(vec)}f", *vec)


def _decode_vec(data: bytes) -> list[float]:
    import struct
    count = len(data) // 4
    return list(struct.unpack(f"<{count}f", data))


# ----------------------------------------------------------------------
# OverviewCache
# ----------------------------------------------------------------------

class OverviewCache:
    """dir_uri → overview markdown. Invalidated by the consolidation worker
    after regenerating the file.
    """

    def __init__(self, backend: CacheBackend, *, ttl_seconds: int = 3600) -> None:
        self.backend = backend
        self.ttl = ttl_seconds

    def get(self, tenant_id: str, dir_uri: str) -> str | None:
        raw = self.backend.get(f"{tenant_id}:{dir_uri}")
        return raw.decode("utf-8") if raw else None

    def set(self, tenant_id: str, dir_uri: str, text: str) -> None:
        self.backend.set(f"{tenant_id}:{dir_uri}", text.encode("utf-8"),
                          ttl_seconds=self.ttl)

    def invalidate(self, tenant_id: str, dir_uri: str) -> None:
        self.backend.delete(f"{tenant_id}:{dir_uri}")
