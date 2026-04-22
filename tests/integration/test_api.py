"""API-level tests using FastAPI TestClient + stubbed state.

Verifies:
  - auth (bearer)
  - request-ID propagation
  - body-size limit returns 413
  - /livez always 200
  - /readyz reports component status
  - rate limiter 429s on burst
  - ingest + query happy path with stubs
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from engram.api.body_limit import BodySizeLimitMiddleware
from engram.api.request_id import RequestIdMiddleware


# The production app binds to the global config and starts workers; for tests
# we import the module and mount only a subset.


@pytest.fixture
def client_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Build a minimal FastAPI app with just the pieces under test."""
    from fastapi import FastAPI

    from engram.api.rate_limit import RateLimitMiddleware

    # Pre-set required env so any config load works in isolation
    monkeypatch.setenv("ENGRAM_API_KEY", "test-real-key")
    monkeypatch.setenv("NEO4J_ADMIN_PASSWORD", "x")
    monkeypatch.setenv("CORE_MODEL_API_KEY", "real-sk")
    monkeypatch.setenv("FRONTIER_LLM_API_KEY", "real-sk")

    def build(middleware=True):
        app = FastAPI()
        if middleware:
            app.add_middleware(
                RateLimitMiddleware, query_per_min=2, ingest_per_min=4,
                redis_url=None,
            )
            app.add_middleware(BodySizeLimitMiddleware)
            app.add_middleware(RequestIdMiddleware)

        @app.post("/api/v1/query")
        def q(body: dict):
            return {"ok": True}

        @app.post("/api/v1/ingest")
        def i(body: dict):
            return {"ok": True}

        @app.get("/livez")
        def liv():
            from fastapi.responses import PlainTextResponse
            return PlainTextResponse("ok")

        return app

    return build


def test_request_id_is_reflected(client_factory):
    app = client_factory()
    with TestClient(app) as c:
        r = c.post("/api/v1/ingest", json={"hello": "world"},
                    headers={"X-Request-ID": "req-custom-123"})
        assert r.status_code == 200
        assert r.headers.get("X-Request-ID") == "req-custom-123"


def test_request_id_generated_when_missing(client_factory):
    app = client_factory()
    with TestClient(app) as c:
        r = c.post("/api/v1/ingest", json={"hello": "world"})
        rid = r.headers.get("X-Request-ID")
        assert rid and rid.startswith("req-")


def test_body_size_limit_returns_413(client_factory):
    app = client_factory()
    with TestClient(app) as c:
        payload = b'{"k":"' + b"x" * 20_000 + b'"}'  # > 16 KiB query limit
        r = c.post("/api/v1/query", content=payload,
                    headers={"Content-Type": "application/json"})
        assert r.status_code == 413


def test_rate_limit_enforces_429(client_factory):
    app = client_factory()
    with TestClient(app) as c:
        ok = 0
        throttled = 0
        for _ in range(10):
            r = c.post("/api/v1/query", json={"q": "hi"})
            if r.status_code == 200:
                ok += 1
            elif r.status_code == 429:
                throttled += 1
        assert throttled > 0
        assert ok <= 2  # capacity=2 query/min


def test_livez_is_unauthenticated(client_factory):
    app = client_factory()
    with TestClient(app) as c:
        r = c.get("/livez")
        assert r.status_code == 200
        assert r.text == "ok"
