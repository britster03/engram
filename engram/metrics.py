"""Prometheus metrics (§13.2).

Exposed via `/metrics` in plain text format. A subset of the SDD's required
metrics is emitted; the rest become meaningful only once we have real
traffic and will be added as their values become observable.
"""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

REGISTRY = CollectorRegistry()

# Query / ingest pipeline latencies
query_latency = Histogram(
    "engram_query_latency_seconds",
    "Per-phase latency for query requests.",
    ["phase"],
    registry=REGISTRY,
)
ingest_stage = Histogram(
    "engram_ingest_pipeline_stage_seconds",
    "Per-step duration in the ingest pipeline.",
    ["stage"],
    registry=REGISTRY,
)
query_depth_predicted_vs_reached = Counter(
    "engram_query_depth_predicted_vs_reached",
    "Counter of predicted_depth vs actual terminal depth.",
    ["predicted", "reached"],
    registry=REGISTRY,
)
reentries_per_query = Histogram(
    "engram_reentries_per_query",
    "Number of frontier re-entries per query.",
    buckets=(0, 1, 2, 3),
    registry=REGISTRY,
)
l0_gate_decisions = Counter(
    "engram_l0_gate_decisions",
    "Outcome of the L0 gate.",
    ["decision", "reason"],
    registry=REGISTRY,
)
ingest_events_total = Counter(
    "engram_ingest_events_total",
    "Counter of ingest events by final status.",
    ["final_status"],
    registry=REGISTRY,
)
core_model_calls = Counter(
    "engram_core_model_calls_total",
    "Calls per Core Model task.",
    ["task", "provider"],
    registry=REGISTRY,
)
frontier_tokens = Counter(
    "engram_frontier_tokens_total",
    "Frontier LLM tokens in/out.",
    ["direction"],
    registry=REGISTRY,
)

# Gauges
consolidation_queue_depth = Gauge(
    "engram_consolidation_queue_depth",
    "Current consolidation queue depth.",
    registry=REGISTRY,
)
kg_node_count = Gauge("engram_kg_node_count", "KG node count.", registry=REGISTRY)
kg_edge_count = Gauge("engram_kg_edge_count", "KG edge count.", registry=REGISTRY)


def render_latest() -> tuple[bytes, str]:
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
