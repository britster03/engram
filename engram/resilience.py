"""Resilience primitives: retries, circuit breakers, timeouts.

Every external call in Engram (LLM, Neo4j, Redis, filesystem) is wrapped in
at most one of these. The goal is to bound latency and blast radius when a
dependency misbehaves: a slow Claude API call should not block the event
loop; a flapping Neo4j should not turn every query into a 30-second wait.

Design choices:
  - tenacity for exponential-backoff retries (standard, pluggable).
  - A lightweight in-process circuit breaker. Once N consecutive failures
    are observed in a rolling window, the breaker opens for `cool_down`
    seconds; subsequent calls short-circuit with CircuitOpenError without
    touching the backend.
  - Every wrapper respects a hard timeout so a hung call cannot monopolise
    a worker slot.

Breakers are identified by a string key so multiple call sites share a
single breaker where appropriate (e.g. one breaker per LLM provider).
"""

from __future__ import annotations

import functools
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, TypeVar

from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

log = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitOpenError(RuntimeError):
    """Raised when a call is short-circuited by an open breaker."""


@dataclass
class _BreakerState:
    failure_threshold: int
    cool_down: float
    failures: int = 0
    opened_at: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record_failure(self) -> None:
        with self.lock:
            self.failures += 1
            if self.failures >= self.failure_threshold:
                self.opened_at = time.monotonic()

    def record_success(self) -> None:
        with self.lock:
            self.failures = 0
            self.opened_at = 0.0

    def is_open(self) -> bool:
        with self.lock:
            if self.opened_at == 0.0:
                return False
            if time.monotonic() - self.opened_at > self.cool_down:
                # Half-open: allow one trial call through.
                self.opened_at = 0.0
                self.failures = max(0, self.failures - 1)
                return False
            return True


_BREAKERS: dict[str, _BreakerState] = {}
_BREAKERS_LOCK = threading.Lock()


def _get_breaker(name: str, failure_threshold: int, cool_down: float) -> _BreakerState:
    with _BREAKERS_LOCK:
        existing = _BREAKERS.get(name)
        if existing is None:
            existing = _BreakerState(failure_threshold=failure_threshold, cool_down=cool_down)
            _BREAKERS[name] = existing
        return existing


def reset_breakers() -> None:
    """Test hook — clears every breaker's state."""
    with _BREAKERS_LOCK:
        _BREAKERS.clear()


def resilient(
    *,
    breaker: str,
    failure_threshold: int = 5,
    cool_down: float = 30.0,
    max_attempts: int = 3,
    initial_delay: float = 0.5,
    max_delay: float = 8.0,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] = Exception,
    log_context: str | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator: apply a circuit breaker + bounded exponential-backoff retry.

    Usage:
        @resilient(breaker="anthropic", max_attempts=3)
        def call_claude(...): ...

    Failures bump the named breaker; after `failure_threshold` failures the
    breaker opens for `cool_down` seconds and subsequent calls raise
    `CircuitOpenError` immediately. Retries never bump the breaker more than
    once per invocation (the tenacity retry loop catches and re-raises).
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            state = _get_breaker(breaker, failure_threshold, cool_down)
            if state.is_open():
                raise CircuitOpenError(
                    f"breaker {breaker!r} is open (last {state.failures} failures)"
                )
            try:
                for attempt in Retrying(
                    stop=stop_after_attempt(max_attempts),
                    wait=(
                        wait_exponential(multiplier=initial_delay, max=max_delay)
                        + wait_random(0, 0.5)
                    ),
                    retry=retry_if_exception_type(retry_on),
                    reraise=True,
                ):
                    with attempt:
                        result = fn(*args, **kwargs)
                state.record_success()
                return result
            except RetryError as err:
                state.record_failure()
                log.warning(
                    "resilient call %s exhausted retries (breaker=%s): %s",
                    log_context or fn.__name__, breaker, err,
                )
                raise
            except Exception:
                state.record_failure()
                raise

        return wrapper

    return decorator


def with_timeout(seconds: float, thread_name: str | None = None):
    """Decorator: enforce a wall-clock timeout by running in a daemon thread.

    Only suitable for synchronous functions that do not hold process-wide
    locks. On timeout the worker thread is abandoned (daemon=True); callers
    receive a TimeoutError immediately.
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            result: list[T] = []
            err: list[BaseException] = []

            def target() -> None:
                try:
                    result.append(fn(*args, **kwargs))
                except BaseException as e:  # noqa: BLE001
                    err.append(e)

            t = threading.Thread(target=target, name=thread_name or fn.__name__, daemon=True)
            t.start()
            t.join(seconds)
            if t.is_alive():
                raise TimeoutError(f"{fn.__name__} exceeded {seconds:.1f}s")
            if err:
                raise err[0]
            return result[0]

        return wrapper

    return decorator


def breaker_snapshot() -> dict[str, dict[str, float]]:
    """Return a read-only snapshot of every breaker's state. For telemetry."""
    out: dict[str, dict[str, float]] = {}
    with _BREAKERS_LOCK:
        for name, state in _BREAKERS.items():
            out[name] = {
                "failures": state.failures,
                "opened_at": state.opened_at,
                "is_open": 1.0 if state.is_open() else 0.0,
            }
    return out
