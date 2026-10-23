"""Bounded parallel discovery and deterministic fusion of eligible identities.

No discovery text is evidence. The caller hydrates and verifies PostgreSQL
rows before selecting eligible IDs for reciprocal-rank fusion.
"""

from __future__ import annotations

import contextvars
import math
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Any

_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="memory-discovery")
_SLOTS = threading.BoundedSemaphore(8)


@dataclass
class Discovery:
    channels: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    status: dict[str, str] = field(default_factory=dict)
    latency_ms: dict[str, float] = field(default_factory=dict)

    def candidates(self) -> list[dict[str, Any]]:
        by_id: dict[str, dict[str, Any]] = {}
        for channel in sorted(self.channels):
            for row in self.channels[channel]:
                identity = str(row["memory_id"])
                previous = by_id.get(identity)
                if previous is None or int(row.get("projected_revision") or 0) > int(
                    previous.get("projected_revision") or 0
                ):
                    # Raw channel scores must not leak into canonical ranking.
                    by_id[identity] = {k: v for k, v in row.items() if k != "score"}
        return list(by_id.values())


def discover(
    channels: Mapping[str, Callable[[], Sequence[Mapping[str, Any]]]], *, timeout: float, limit: int
) -> Discovery:
    result = Discovery()
    futures = {}
    started = time.monotonic()

    def execute(call: Callable) -> tuple[Any, float]:
        start = time.monotonic()
        rows = call()
        return rows, (time.monotonic() - start) * 1000

    for name, call in channels.items():
        if not _SLOTS.acquire(blocking=False):
            result.status[name] = "saturated"
            continue
        try:
            future = _EXECUTOR.submit(contextvars.copy_context().run, execute, call)
        except Exception:
            _SLOTS.release()
            raise
        future.add_done_callback(lambda _: _SLOTS.release())
        futures[future] = name
    done, pending = wait(futures, timeout=max(0.0, timeout - (time.monotonic() - started)))
    for future in pending:
        future.cancel()
        result.status[futures[future]] = "timeout"
    for future in done:
        name = futures[future]
        try:
            rows, elapsed = future.result()
            clean: list[dict[str, Any]] = []
            seen: set[str] = set()
            for row in rows:
                identity = str(row.get("memory_id") or "")
                score = float(row.get("score") or 0.0)
                if not identity or identity in seen or not math.isfinite(score):
                    continue
                seen.add(identity)
                clean.append(dict(row))
                if len(clean) >= limit:
                    break
            result.channels[name] = clean
            result.status[name] = "ok" if clean else "empty"
            result.latency_ms[name] = elapsed
        except Exception:
            # Diagnostics intentionally omit provider/query/error content.
            result.status[name] = "unavailable"
    return result


def reciprocal_rank_fusion(
    channels: Mapping[str, Sequence[Mapping[str, Any]]], eligible: set[str], *, k: int = 60
) -> dict[str, float]:
    if k < 1:
        raise ValueError("rrf k must be positive")
    scores: dict[str, float] = {}
    for name in sorted(channels):
        seen: set[str] = set()
        rank = 0
        for row in channels[name]:
            identity = str(row.get("memory_id") or "")
            if identity not in eligible or identity in seen:
                continue
            seen.add(identity)
            rank += 1
            scores[identity] = scores.get(identity, 0.0) + 1.0 / (k + rank)
    return dict(sorted(scores.items(), key=lambda item: (-item[1], item[0])))
