from __future__ import annotations

from datetime import datetime

import pytest

from engram.decay_worker import _due


def test_daily_schedule_matches_only_the_configured_minute_and_hour():
    assert _due("0 3 * * *", datetime(2026, 1, 1, 3, 0))
    assert not _due("0 3 * * *", datetime(2026, 1, 1, 3, 1))


def test_non_daily_schedule_is_rejected():
    with pytest.raises(ValueError):
        _due("*/5 * * * *", datetime.now())
