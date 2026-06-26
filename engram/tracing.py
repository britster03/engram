"""OpenTelemetry tracing setup.

One call to `configure_tracing()` at process boot. OTEL SDK + auto-
instrumentation for FastAPI, HTTPX, Redis, and the neo4j Python driver.
Custom spans for pipeline stages via `span()` / `@traced`.

Export is controlled by environment variables (standard OTEL conventions):

  OTEL_EXPORTER_OTLP_ENDPOINT     e.g. http://otel-collector:4317
  OTEL_EXPORTER_OTLP_HEADERS      e.g. "authorization=Bearer ..."
  OTEL_SERVICE_NAME               default: "engram"
  OTEL_RESOURCE_ATTRIBUTES        default: deployment.environment=production
  ENGRAM_TRACING_ENABLED          set to "0" to force-disable

Tenant propagation: every outgoing span carries `tenant_id` as an
attribute so per-tenant slicing is possible in backends like Tempo / Honeycomb.

Heavy dependencies are imported lazily; the module is cheap to import when
tracing is disabled.
"""

from __future__ import annotations

import contextlib
import functools
import logging
import os
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

_ENABLED = False
_tracer = None


def _tracing_enabled() -> bool:
    if os.environ.get("ENGRAM_TRACING_ENABLED", "").lower() in {"0", "false", "no"}:
        return False
    # Enabled if any OTEL env var is configured
    return any(
        os.environ.get(k)
        for k in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_HEADERS",
        )
    )


def configure_tracing(app=None) -> None:
    """Install OTEL SDK + instrumentations if OTEL env vars are set.

    Idempotent. Safe to call multiple times; only the first call does work.
    `app` is an optional FastAPI instance for HTTP auto-instrumentation.
    """
    global _ENABLED, _tracer
    if _ENABLED:
        return
    if not _tracing_enabled():
        log.info("OTEL tracing disabled (no OTEL_EXPORTER_OTLP_* env vars)")
        return
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        log.warning(
            "OTEL env vars are set but opentelemetry-sdk is not installed; "
            "install `opentelemetry-sdk opentelemetry-exporter-otlp` to enable tracing"
        )
        return

    resource = Resource.create(
        {
            "service.name": os.environ.get("OTEL_SERVICE_NAME", "engram"),
            "service.version": _version(),
        }
    )
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter()
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("engram")
    _ENABLED = True
    log.info("OTEL tracing configured")

    # Auto-instrument HTTP + Redis + Neo4j where the libraries are present.
    _try_instrument_http(app)
    _try_instrument_redis()
    _try_instrument_neo4j()


def _try_instrument_http(app) -> None:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        if app is not None:
            FastAPIInstrumentor.instrument_app(app)
        HTTPXClientInstrumentor().instrument()
    except Exception as err:
        log.debug("HTTP instrumentation skipped: %s", err)


def _try_instrument_redis() -> None:
    try:
        from opentelemetry.instrumentation.redis import RedisInstrumentor
        RedisInstrumentor().instrument()
    except Exception as err:
        log.debug("Redis instrumentation skipped: %s", err)


def _try_instrument_neo4j() -> None:
    # Neo4j has no official OTEL instrumentation yet; spans for Cypher calls
    # are emitted from our own run_template wrapper.
    pass


def _version() -> str:
    try:
        from engram import __version__
        return __version__
    except Exception:
        return "0.0.0"


# ----------------------------------------------------------------------
# Public tracing helpers
# ----------------------------------------------------------------------

@contextlib.contextmanager
def span(name: str, **attributes: Any) -> Iterator[Any]:
    """Context manager that opens an OTEL span when tracing is enabled.

    Usage:
        with span("ingest.gate", event_id=eid, tenant_id=tid):
            ...
    """
    if not _ENABLED or _tracer is None:
        yield None
        return
    from engram.tenancy import current_tenant_id
    attrs = {"tenant_id": current_tenant_id(), **attributes}
    with _tracer.start_as_current_span(name, attributes=attrs) as s:
        yield s


def traced(name: str | None = None, **default_attrs: Any) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that wraps a function call in a span."""

    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        span_name = name or f"{fn.__module__}.{fn.__qualname__}"

        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            with span(span_name, **default_attrs):
                return fn(*args, **kwargs)

        return wrapper

    return deco


def is_enabled() -> bool:
    return _ENABLED
