"""Distributed coordination primitives.

Provides a Redis-based lease so background workers (consolidation,
reconciliation) can run safely in multi-replica deployments: only the
holder of the lease executes the singleton work, everyone else hot-stands
by. When the holder dies the lease expires and another replica picks up.

The lease is **not** the same as a traditional mutex:
  - non-blocking: `acquire()` returns True/False, never waits
  - fenced: every renewal rechecks the value so a partitioned old holder
    can detect it has lost the lease
  - cooperative: workers SHOULD stop their singleton work immediately when
    `is_held()` returns False

Without Redis the implementation becomes a no-op per-process singleton
(always "we are the leader") — this matches the single-replica deployment
where no coordination is needed.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from typing import Protocol

import redis

log = logging.getLogger(__name__)


class LeaseBackend(Protocol):
    def acquire(self) -> bool: ...  # pragma: no cover
    def renew(self) -> bool: ...    # pragma: no cover
    def release(self) -> None: ...  # pragma: no cover
    def is_held(self) -> bool: ...  # pragma: no cover


class NoOpLease:
    """Single-replica fallback: always the leader."""

    def acquire(self) -> bool: return True
    def renew(self) -> bool: return True
    def release(self) -> None: return None
    def is_held(self) -> bool: return True


class RedisLease:
    """Atomic Redis-based lease keyed by `name`, with fencing on the value.

    The lease's value is a random token chosen at acquire time; renewals
    are compare-and-set so a partitioned old holder cannot extend a lease
    that has already been re-acquired by somebody else.
    """

    # Lua: extend if we still own the key
    _RENEW = (
        "if redis.call('get', KEYS[1]) == ARGV[1] "
        "then return redis.call('pexpire', KEYS[1], ARGV[2]) "
        "else return 0 end"
    )
    # Lua: release only if we still own it
    _RELEASE = (
        "if redis.call('get', KEYS[1]) == ARGV[1] "
        "then return redis.call('del', KEYS[1]) "
        "else return 0 end"
    )

    def __init__(
        self,
        redis_url: str,
        name: str,
        *,
        ttl_seconds: float = 30.0,
        node_id: str | None = None,
    ) -> None:
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.key = f"engram:lease:{name}"
        self.ttl_ms = int(ttl_seconds * 1000)
        # Distinct value per process; lets us verify ownership on renew/release.
        self.value = f"{node_id or _default_node_id()}/{uuid.uuid4().hex[:8]}"
        self._renew_script = self.client.register_script(self._RENEW)
        self._release_script = self.client.register_script(self._RELEASE)
        self._held = False

    def acquire(self) -> bool:
        try:
            ok = bool(self.client.set(self.key, self.value, nx=True, px=self.ttl_ms))
        except (redis.RedisError, OSError) as err:
            log.warning("lease acquire failed for %s: %s", self.key, err)
            return False
        self._held = ok
        return ok

    def renew(self) -> bool:
        try:
            ok = int(self._renew_script(keys=[self.key], args=[self.value, self.ttl_ms]))
        except (redis.RedisError, OSError) as err:
            log.warning("lease renew failed for %s: %s", self.key, err)
            self._held = False
            return False
        self._held = bool(ok)
        return self._held

    def release(self) -> None:
        try:
            self._release_script(keys=[self.key], args=[self.value])
        except (redis.RedisError, OSError) as err:
            log.debug("lease release best-effort failed for %s: %s", self.key, err)
        self._held = False

    def is_held(self) -> bool:
        return self._held


def build_lease(redis_url: str | None, name: str, *, ttl_seconds: float = 30.0) -> LeaseBackend:
    """Return a RedisLease or a NoOpLease (single-node fallback)."""
    if not redis_url:
        return NoOpLease()
    try:
        lease = RedisLease(redis_url, name, ttl_seconds=ttl_seconds)
        # Soft probe
        lease.client.ping()
        return lease
    except Exception as err:
        log.warning("Redis lease unavailable for %s (%s); falling back to NoOpLease", name, err)
        return NoOpLease()


# ----------------------------------------------------------------------
# Leader loop: run `fn` only while we hold the lease, renewing periodically.
# ----------------------------------------------------------------------

def run_as_leader(
    lease: LeaseBackend,
    stop: threading.Event,
    fn,                            # callable taking `stop: Event` → None
    *,
    poll_interval_s: float = 1.0,
    renew_interval_s: float = 10.0,
) -> None:
    """Acquire the lease, run `fn(stop)` while holding it, renew periodically.

    If the lease is lost (Redis failure, another process took over), `fn`
    is asked to stop via the shared `stop` Event and we loop back to
    re-acquire. When the outer `stop` is set, everything unwinds cleanly.
    """
    inner_stop = threading.Event()
    work_thread: threading.Thread | None = None

    while not stop.is_set():
        if lease.acquire():
            log.info("lease %s acquired; starting leader work", getattr(lease, "key", "?"))
            inner_stop = threading.Event()
            work_thread = threading.Thread(target=fn, args=(inner_stop,), daemon=True)
            work_thread.start()
            # Renew loop
            while not stop.is_set():
                stop.wait(renew_interval_s)
                if stop.is_set():
                    break
                if not lease.renew():
                    log.warning("lease %s lost; stopping leader work", getattr(lease, "key", "?"))
                    break
            inner_stop.set()
            if work_thread is not None:
                work_thread.join(timeout=10.0)
            lease.release()
        stop.wait(poll_interval_s)


def _default_node_id() -> str:
    return os.environ.get("ENGRAM_NODE_ID") or socket.gethostname() or "engram-node"
