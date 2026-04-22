"""Token-bucket rate limiting middleware (§11.5).

Keyed by bearer token (falling back to client host). Two backends are
wired in: an in-process bucket for single-worker dev, and a Redis-backed
Lua-script atomic bucket for multi-worker production.

The middleware picks the backend based on configuration. If Redis is
configured but unreachable at request time, we log and fall back to the
per-process bucket rather than fail open — requests still succeed, just
with per-worker-local accounting until Redis recovers.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Protocol

import redis
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

log = logging.getLogger(__name__)


# Atomic token-bucket Lua script. Returns (allowed, retry_after_seconds).
_REDIS_SCRIPT = """
local key       = KEYS[1]
local now       = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])  -- tokens per second
local capacity  = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

local delta = math.max(0, now - ts)
tokens = math.min(capacity, tokens + delta * rate)

local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = (1 - tokens) / rate
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, math.ceil(capacity / rate) + 1)
return {allowed, tostring(retry)}
"""


class RateLimiterBackend(Protocol):
    def allow(self, key: str) -> tuple[bool, float]:  # pragma: no cover - interface
        ...


@dataclass
class _Bucket:
    tokens: float
    last_refill: float


class InMemoryLimiter:
    """Single-process token bucket."""

    def __init__(self, capacity: int, refill_per_minute: int) -> None:
        self.capacity = float(capacity)
        self.refill_rate_per_s = refill_per_minute / 60.0
        self._buckets: dict[str, _Bucket] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> tuple[bool, float]:
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _Bucket(tokens=self.capacity, last_refill=now)
                self._buckets[key] = bucket
            else:
                elapsed = now - bucket.last_refill
                bucket.tokens = min(
                    self.capacity, bucket.tokens + elapsed * self.refill_rate_per_s
                )
                bucket.last_refill = now
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True, 0.0
            deficit = 1.0 - bucket.tokens
            retry_after = deficit / self.refill_rate_per_s if self.refill_rate_per_s else 60.0
            return False, retry_after


class RedisLimiter:
    """Token bucket backed by a Redis Lua script for atomic multi-worker accounting."""

    def __init__(
        self, url: str, capacity: int, refill_per_minute: int, *, name: str
    ) -> None:
        self.client = redis.Redis.from_url(url, decode_responses=True)
        self.capacity = float(capacity)
        self.refill_rate_per_s = refill_per_minute / 60.0
        self.name = name
        # Register the script once per client; reuse sha on subsequent calls.
        self._script = self.client.register_script(_REDIS_SCRIPT)

    def allow(self, key: str) -> tuple[bool, float]:
        try:
            allowed, retry = self._script(
                keys=[f"engram:rl:{self.name}:{key}"],
                args=[time.time(), self.refill_rate_per_s, self.capacity],
            )
        except (redis.RedisError, OSError) as err:
            raise RuntimeError(f"redis rate limiter error: {err}") from err
        return bool(int(allowed)), float(retry)


# Keep legacy name for tests
TokenBucketLimiter = InMemoryLimiter


class _FallbackWrapper:
    """Prefer Redis; on Redis failure, fall back to a per-process bucket."""

    def __init__(
        self,
        primary: RedisLimiter,
        fallback: InMemoryLimiter,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._failures = 0
        self._lock = threading.Lock()

    def allow(self, key: str) -> tuple[bool, float]:
        try:
            result = self._primary.allow(key)
            with self._lock:
                self._failures = 0
            return result
        except Exception as err:
            with self._lock:
                self._failures += 1
                if self._failures == 1 or self._failures % 50 == 0:
                    log.warning(
                        "redis rate limiter failed (%s); using in-memory fallback", err
                    )
            return self._fallback.allow(key)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Tenant-aware rate limiter.

    When a bearer token resolves to a tenant, the limit is that tenant's
    `quotas.requests_per_minute` / `quotas.ingest_per_minute`. Unknown
    tokens fall back to the fleet-wide defaults from config.yaml. Unlimited
    is signalled by capacity <= 0.
    """

    def __init__(
        self,
        app,
        *,
        query_per_min: int,
        ingest_per_min: int,
        redis_url: str | None = None,
    ) -> None:
        super().__init__(app)
        self.default_query = query_per_min
        self.default_ingest = ingest_per_min
        self.redis_url = redis_url

        mem_query = InMemoryLimiter(capacity=query_per_min, refill_per_minute=query_per_min)
        mem_ingest = InMemoryLimiter(capacity=ingest_per_min, refill_per_minute=ingest_per_min)
        if redis_url:
            try:
                self._legacy_query = _FallbackWrapper(
                    RedisLimiter(redis_url, query_per_min, query_per_min,
                                 name="query_default"),
                    mem_query,
                )
                self._legacy_ingest = _FallbackWrapper(
                    RedisLimiter(redis_url, ingest_per_min, ingest_per_min,
                                 name="ingest_default"),
                    mem_ingest,
                )
                log.info("rate limiter using Redis backend")
            except Exception as err:
                log.warning("rate limiter: Redis init failed (%s); in-memory only", err)
                self._legacy_query = mem_query
                self._legacy_ingest = mem_ingest
        else:
            self._legacy_query = mem_query
            self._legacy_ingest = mem_ingest

        self._per_tenant: dict[tuple[str, str], Any] = {}
        self._per_tenant_lock = threading.Lock()

    # Public accessors for existing tests
    @property
    def query(self):
        return self._legacy_query

    @property
    def ingest(self):
        return self._legacy_ingest

    def _bucket_for_tenant(
        self, tenant_id: str, dimension: str, capacity: int,
    ):
        key = (tenant_id, dimension)
        with self._per_tenant_lock:
            existing = self._per_tenant.get(key)
            if existing is not None:
                return existing
            mem = InMemoryLimiter(capacity=capacity, refill_per_minute=capacity)
            if self.redis_url:
                try:
                    limiter = _FallbackWrapper(
                        RedisLimiter(
                            self.redis_url, capacity, capacity,
                            name=f"{dimension}_{tenant_id}",
                        ),
                        mem,
                    )
                except Exception:
                    limiter = mem
            else:
                limiter = mem
            self._per_tenant[key] = limiter
            return limiter

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        path = request.url.path
        dimension: str | None = None
        if path.startswith("/api/v1/query"):
            dimension = "query"
        elif path.startswith("/api/v1/ingest") or path.endswith("/message"):
            dimension = "ingest"
        if dimension is None:
            return await call_next(request)

        auth = request.headers.get("authorization", "").split(" ", 1)[-1]
        tenant = _resolve_tenant(auth) if auth else None

        if tenant is not None:
            capacity = (
                tenant.quotas.requests_per_minute
                if dimension == "query"
                else tenant.quotas.ingest_per_minute
            )
            if capacity <= 0:  # unlimited sentinel
                return await call_next(request)
            limiter = self._bucket_for_tenant(tenant.tenant_id, dimension, capacity)
            key = tenant.tenant_id
        else:
            limiter = self._legacy_query if dimension == "query" else self._legacy_ingest
            key = auth or (request.client.host if request.client else "anonymous")

        allowed, retry_after = limiter.allow(key)
        if not allowed:
            return JSONResponse(
                status_code=429,
                content={"detail": "rate limit exceeded"},
                headers={"Retry-After": str(int(retry_after) + 1)},
            )
        return await call_next(request)


def _resolve_tenant(api_key: str):
    """Best-effort tenant lookup. Returns None if registry is unavailable."""
    try:
        from engram.deps import get_state
        state = get_state()
        return state.tenant_registry.resolve_key(api_key)
    except Exception:
        return None
