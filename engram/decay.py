"""Soft memory decay (§10).

retrieval_weight = alpha*recency + beta*frequency + gamma*centrality, with percentile
normalisation for frequency and centrality per §10.1. Runs as a daily cron
over all ACTIVE nodes; HISTORICAL nodes are exempt.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any

from engram.config import DecayConfig

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DecayPreset:
    alpha: float
    beta: float
    gamma: float
    half_life_lambda: float  # used in exp(-λ · days_since_last_access)


PRESETS: dict[str, DecayPreset] = {
    "personal_conversation": DecayPreset(0.40, 0.30, 0.30, 0.01),   # ~69 days
    "coding_agent":          DecayPreset(0.40, 0.50, 0.10, 0.05),   # ~14 days
    "knowledge_base":        DecayPreset(0.20, 0.30, 0.50, 0.005),  # ~138 days
}


def compute_weight(
    *,
    preset: DecayPreset,
    days_since_last_access: float,
    access_count: int,
    p95_access_count: float,
    relates_to_degree: int,
    p95_relates_to_degree: float,
) -> float:
    recency = math.exp(-preset.half_life_lambda * max(0.0, days_since_last_access))
    freq_num = math.log1p(access_count)
    freq_den = math.log1p(p95_access_count) or 1.0
    frequency = min(1.0, freq_num / freq_den)
    cent_den = p95_relates_to_degree or 1.0
    centrality = min(1.0, relates_to_degree / cent_den)
    return (
        preset.alpha * recency
        + preset.beta * frequency
        + preset.gamma * centrality
    )


# ----------------------------------------------------------------------
# Daily run
# ----------------------------------------------------------------------

def run_daily(
    neo4j: Any,
    cfg: DecayConfig,
    *,
    batch_size: int = 10_000,
) -> int:
    preset = PRESETS[cfg.preset]
    p95_access, p95_deg = _compute_percentiles(neo4j)
    updated = 0
    offset = 0
    while True:
        rows = _fetch_batch(neo4j, offset, batch_size)
        if not rows:
            break
        for r in rows:
            weight = compute_weight(
                preset=preset,
                days_since_last_access=_days_since(r.get("last_accessed_at")),
                access_count=int(r.get("access_count") or 0),
                p95_access_count=p95_access,
                relates_to_degree=int(r.get("degree") or 0),
                p95_relates_to_degree=p95_deg,
            )
            _write_weight(neo4j, r["source_uri"], weight)
            updated += 1
        offset += len(rows)
    # Clamp HISTORICAL nodes to 0
    _clamp_historical(neo4j)
    return updated


def _days_since(iso_ts: str | None) -> float:
    if not iso_ts:
        return 0.0
    from datetime import datetime, timezone
    try:
        then = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    now = datetime.now(timezone.utc)
    return max(0.0, (now - then).total_seconds() / 86400.0)


def _compute_percentiles(neo4j: Any) -> tuple[float, float]:
    try:
        access_rows = neo4j.run_template(
            "MATCH (n:Node) WHERE n.status = 'ACTIVE' "
            "RETURN coalesce(n.access_count, 0) AS v",
            {},
        )
        degree_rows = neo4j.run_template(
            "MATCH (n:Node) WHERE n.status = 'ACTIVE' "
            "OPTIONAL MATCH (n)-[r:RELATES_TO]-() WHERE r.status = 'ACTIVE' "
            "RETURN count(r) AS v",
            {},
        )
    except Exception:
        return 1.0, 1.0
    access = sorted(int(r["v"]) for r in access_rows) or [0]
    degree = sorted(int(r["v"]) for r in degree_rows) or [0]
    return float(_percentile(access, 95)), float(_percentile(degree, 95))


def _percentile(values: list[int], pct: float) -> int:
    if not values:
        return 1
    k = round((pct / 100.0) * (len(values) - 1))
    return max(1, values[k])


def _fetch_batch(neo4j: Any, offset: int, batch_size: int) -> list[dict]:
    try:
        return neo4j.run_template(
            "MATCH (n:Node) WHERE n.status = 'ACTIVE' "
            "OPTIONAL MATCH (n)-[r:RELATES_TO]-() WHERE r.status = 'ACTIVE' "
            "WITH n, count(r) AS degree "
            "RETURN n.source_uri AS source_uri, n.last_accessed_at AS last_accessed_at, "
            "n.access_count AS access_count, degree "
            "ORDER BY n.source_uri SKIP $offset LIMIT $limit",
            {"offset": offset, "limit": batch_size},
        )
    except Exception:
        return []


def _write_weight(neo4j: Any, source_uri: str, weight: float) -> None:
    try:
        neo4j.run_template(
            "MATCH (n:Node {source_uri: $uri}) SET n.retrieval_weight = $w",
            {"uri": source_uri, "w": float(weight)},
        )
    except Exception:
        log.debug("write weight failed for %s", source_uri, exc_info=True)


def _clamp_historical(neo4j: Any) -> None:
    try:
        neo4j.run_template(
            "MATCH (n:Node) WHERE n.status = 'HISTORICAL' SET n.retrieval_weight = 0.0",
            {},
        )
    except Exception:
        log.debug("historical clamp failed", exc_info=True)
