"""Dedicated reconciliation process for production Compose deployments."""

from __future__ import annotations

import signal
import threading

from engram.consolidation.reconciliation import ReconciliationContext, run_forever
from engram.deps import get_state


def main() -> None:
    state = get_state()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    run_forever(
        ReconciliationContext(cfg=state.cfg, control_plane=state.control_plane, neo4j=state.neo4j),
        stop,
    )


if __name__ == "__main__":
    main()
