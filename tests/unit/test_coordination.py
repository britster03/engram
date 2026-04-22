"""Lease backends: NoOpLease + leader-election loop."""

from __future__ import annotations

import threading
import time

from engram.coordination import NoOpLease, run_as_leader


def test_noop_lease_is_always_leader():
    lease = NoOpLease()
    assert lease.acquire() is True
    assert lease.renew() is True
    assert lease.is_held() is True
    lease.release()


def test_run_as_leader_invokes_fn_and_exits_on_stop():
    calls = {"n": 0, "inner_stops": 0}

    def work(inner_stop: threading.Event) -> None:
        # Run until we're told to stop
        while not inner_stop.is_set():
            calls["n"] += 1
            inner_stop.wait(0.05)
        calls["inner_stops"] += 1

    outer_stop = threading.Event()
    t = threading.Thread(
        target=run_as_leader,
        args=(NoOpLease(), outer_stop, work),
        kwargs={"renew_interval_s": 0.1, "poll_interval_s": 0.05},
        daemon=True,
    )
    t.start()
    time.sleep(0.3)
    outer_stop.set()
    t.join(timeout=3.0)
    assert calls["n"] > 0
    assert calls["inner_stops"] >= 1
