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
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from engram.models.budget import BudgetExceededError, bounded_sleep

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
    probe_in_flight: bool = False
    generation: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record_failure(self, generation: int | None = None) -> None:
        with self.lock:
            if generation is not None and generation != self.generation:
                return
            self.failures += 1
            if self.probe_in_flight or self.failures >= self.failure_threshold:
                self.opened_at = time.monotonic()
                self.probe_in_flight = False
                self.generation += 1

    def record_success(self, generation: int | None = None) -> None:
        with self.lock:
            if generation is not None and generation != self.generation:
                return
            self.failures = 0
            self.opened_at = 0.0
            self.probe_in_flight = False

    def is_open(self) -> bool:
        """Inspect state without consuming a recovery probe."""
        with self.lock:
            return self.opened_at != 0.0

    def release(self, generation: int) -> None:
        """Local admission/deadline failure says nothing about provider health."""
        with self.lock:
            if generation == self.generation and self.probe_in_flight:
                self.probe_in_flight = False
                self.generation += 1

    def acquire(self) -> int:
        with self.lock:
            if self.opened_at:
                if self.probe_in_flight or time.monotonic() - self.opened_at < self.cool_down:
                    raise CircuitOpenError("provider circuit is cooling down or probing")
                self.probe_in_flight = True
                self.generation += 1
            return self.generation


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
        @resilient(breaker="muse_spark", max_attempts=3)
        def call_model(...): ...

    Failures bump the named breaker; after `failure_threshold` failures the
    breaker opens for `cool_down` seconds and subsequent calls raise
    `CircuitOpenError` immediately. Retries never bump the breaker more than
    once per invocation (the tenacity retry loop catches and re-raises).
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            state = _get_breaker(breaker, failure_threshold, cool_down)
            generation = state.acquire()
            try:
                for attempt in Retrying(
                    stop=stop_after_attempt(max_attempts),
                    wait=(
                        wait_exponential(multiplier=initial_delay, max=max_delay)
                        + wait_random(0, 0.5)
                    ),
                    retry=retry_if_exception_type(retry_on) & retry_if_not_exception_type(BudgetExceededError),
                    reraise=True,
                    sleep=bounded_sleep,
                ):
                    with attempt:
                        result = fn(*args, **kwargs)
                state.record_success(generation)
                return result
            except BudgetExceededError:
                state.release(generation)
                raise
            except RetryError as err:
                state.record_failure(generation)
                log.warning(
                    "resilient call %s exhausted retries (breaker=%s): %s",
                    log_context or fn.__name__,
                    breaker,
                    err,
                )
                raise
            except BaseException:
                # A cancelled half-open probe must never strand the breaker.
                state.record_failure(generation)
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
                except BaseException as e:
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
            with state.lock:
                out[name] = {
                    "failures": state.failures,
                    "opened_at": state.opened_at,
                    "is_open": float(bool(state.opened_at)),
                    "probe_in_flight": float(state.probe_in_flight),
                    "retry_after_seconds": max(0.0, state.cool_down - (time.monotonic() - state.opened_at)) if state.opened_at else 0.0,
                }
    return out
