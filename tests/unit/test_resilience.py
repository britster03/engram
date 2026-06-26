"""Retry + circuit breaker unit tests."""

from __future__ import annotations

import time

import pytest

from engram.resilience import (
    CircuitOpenError,
    breaker_snapshot,
    reset_breakers,
    resilient,
    with_timeout,
)


class BoomError(RuntimeError):
    pass


def setup_function(_):
    reset_breakers()


def test_successful_call_resets_failures():
    calls = {"n": 0}

    @resilient(breaker="ok", failure_threshold=3, max_attempts=1)
    def fn():
        calls["n"] += 1
        return "ok"

    assert fn() == "ok"
    assert calls["n"] == 1
    snap = breaker_snapshot()
    assert snap["ok"]["failures"] == 0


def test_retries_then_succeeds():
    calls = {"n": 0}

    @resilient(breaker="flaky", failure_threshold=5, max_attempts=3,
               initial_delay=0.01, max_delay=0.02, retry_on=BoomError)
    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise BoomError("nope")
        return "done"

    assert fn() == "done"
    assert calls["n"] == 3


def test_circuit_opens_after_threshold():
    @resilient(breaker="fail", failure_threshold=2, max_attempts=1,
               cool_down=0.2, retry_on=BoomError)
    def fn():
        raise BoomError("always")

    with pytest.raises(BoomError):
        fn()
    with pytest.raises(BoomError):
        fn()
    # Breaker is open now
    with pytest.raises(CircuitOpenError):
        fn()
    snap = breaker_snapshot()
    assert snap["fail"]["is_open"] == 1.0


def test_circuit_closes_after_cooldown():
    calls = {"n": 0}

    @resilient(breaker="recover", failure_threshold=1, max_attempts=1,
               cool_down=0.05, retry_on=BoomError)
    def fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise BoomError("one-off")
        return "alive"

    with pytest.raises(BoomError):
        fn()
    # Circuit is open
    with pytest.raises(CircuitOpenError):
        fn()
    time.sleep(0.1)
    # After cooldown, one trial call is allowed through (half-open).
    assert fn() == "alive"


def test_with_timeout_raises_on_slow_call():
    @with_timeout(0.05)
    def slow():
        time.sleep(0.5)
        return "late"

    with pytest.raises(TimeoutError):
        slow()


def test_with_timeout_passes_through_normal_exception():
    @with_timeout(1.0)
    def bad():
        raise BoomError("kaboom")

    with pytest.raises(BoomError):
        bad()
