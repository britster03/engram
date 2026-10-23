"""Request-wide provider deadlines and admission; usage is observed before parsing."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


class BudgetExceededError(TimeoutError):
    pass


@dataclass
class RequestBudget:
    deadline: float
    max_attempts: int = 32
    attempts: list[dict[str, Any]] = field(default_factory=list)

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise BudgetExceededError("absolute query deadline exceeded")
        return remaining


_budget: ContextVar[RequestBudget | None] = ContextVar("engram_query_budget", default=None)
_phase: ContextVar[str | None] = ContextVar("engram_provider_phase", default=None)
_slots: dict[tuple[str, int], threading.BoundedSemaphore] = {}
_lock = threading.Lock()
_health: dict[tuple[str, str], tuple[int, float, float, bool]] = {}


def provider_recovered_at(provider, *, max_age_seconds: float = 300) -> datetime | None:
    """Fresh observed SDK success, never an assumption based on breaker cooldown.

    Health is specific to this process, endpoint and model. A newer attempt
    invalidates an older success until that attempt finishes successfully.
    """
    client, cfg = getattr(provider, "_client", None), getattr(provider, "cfg", None)
    if client is None or cfg is None:
        return None
    key = (str(client.base_url), str(cfg.model_path))
    with _lock:
        observation = _health.get(key)
    if not observation or not observation[3] or time.monotonic() - observation[2] > max_age_seconds:
        return None
    return datetime.fromtimestamp(observation[1], tz=timezone.utc)


def current_model_phase():
    return _phase.get()


@contextmanager
def model_phase(name):
    token = _phase.set(name)
    try:
        yield
    finally:
        _phase.reset(token)


@contextmanager
def query_budget(seconds: float, max_attempts: int = 32):
    parent = _budget.get()
    budget = parent or RequestBudget(time.monotonic() + seconds, max_attempts)
    token = _budget.set(budget)
    try:
        yield budget
    finally:
        _budget.reset(token)


def check_deadline() -> None:
    if budget := _budget.get():
        budget.remaining()


def bounded_sleep(seconds: float) -> None:
    if (budget := _budget.get()) and seconds >= budget.remaining():
        raise BudgetExceededError("retry wait exceeds remaining query deadline")
    time.sleep(seconds)


def call_provider(client, method, kwargs, *, timeout: float = 60, concurrency: int = 2):
    """A permit covers the actual SDK operation, including failed attempts.

    Core/frontier clients sharing an endpoint share admission. Request records
    contain usage and exception types, never prompts or credentials.
    """
    budget = _budget.get()
    key = (str(getattr(client, "base_url", "default")), concurrency)
    with _lock:
        semaphore = _slots.setdefault(key, threading.BoundedSemaphore(concurrency))
    wait = min(timeout, budget.remaining()) if budget else timeout
    if not semaphore.acquire(timeout=wait):
        raise BudgetExceededError("provider capacity unavailable before deadline")
    attempt = None
    try:
        wait = min(timeout, budget.remaining()) if budget else timeout
        if budget:
            if len(budget.attempts) >= budget.max_attempts:
                raise BudgetExceededError("provider attempt budget exhausted")
            attempt = {
                "attempt": len(budget.attempts) + 1,
                "model": kwargs.get("model"),
                "phase": _phase.get(),
                "status": "running",
                "usage": None,
            }
            budget.attempts.append(attempt)
        started = time.monotonic()
        health_key = (str(getattr(client, "base_url", "default")), str(kwargs.get("model")))
        with _lock:
            sequence = _health.get(health_key, (0, 0, 0, False))[0] + 1
            _health[health_key] = (sequence, 0, started, False)
        try:
            response = method(**kwargs, timeout=wait)
            with _lock:
                if _health[health_key][0] == sequence:
                    _health[health_key] = (sequence, time.time(), time.monotonic(), True)
            if attempt is not None:
                usage = getattr(response, "usage", None)
                attempt["usage"] = (
                    usage.model_dump()
                    if callable(getattr(usage, "model_dump", None))
                    else (usage if isinstance(usage, dict) else None)
                )
                attempt["status"] = "returned"
            check_deadline()
            return response
        except BaseException as error:
            if attempt is not None:
                attempt.update(status="error", error=type(error).__name__)
            raise
        finally:
            if attempt is not None:
                attempt["latency_ms"] = (time.monotonic() - started) * 1000
    finally:
        semaphore.release()
