"""Small dedicated scheduler for daily decay in single-host deployments."""

from __future__ import annotations

import logging
import signal
import threading
from datetime import datetime

from engram.decay import run_daily
from engram.deps import get_state

log = logging.getLogger(__name__)


def _due(schedule: str, now: datetime) -> bool:
    """Support the configured daily ``minute hour * * *`` schedule only."""
    fields = schedule.split()
    if (
        len(fields) != 5
        or fields[2:] != ["*", "*", "*"]
        or not fields[0].isdigit()
        or not fields[1].isdigit()
        or not 0 <= int(fields[0]) <= 59
        or not 0 <= int(fields[1]) <= 23
    ):
        raise ValueError("single-host decay supports only a daily 'minute hour * * *' schedule")
    return fields[0] == str(now.minute) and fields[1] == str(now.hour)


def main() -> None:
    state = get_state()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    last_run = ""
    while not stop.wait(1.0):
        now = datetime.now()
        day = now.date().isoformat()
        if day == last_run or not _due(state.cfg.decay.schedule, now):
            continue
        try:
            updated = run_daily(state.neo4j, state.cfg.decay)
            last_run = day
            log.info("decay complete", extra={"updated_nodes": updated})
        except Exception:
            log.exception("decay failed")


if __name__ == "__main__":
    main()
