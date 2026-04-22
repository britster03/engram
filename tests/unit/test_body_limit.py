"""Body-size middleware unit tests."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from engram.api.body_limit import BodySizeLimitMiddleware, DEFAULT_LIMIT


def _make_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(BodySizeLimitMiddleware)

    @app.post("/api/v1/query")
    def q(body: dict):
        return {"ok": True}

    @app.post("/api/v1/ingest")
    def i(body: dict):
        return {"ok": True}

    @app.post("/other")
    def o(body: dict):
        return {"ok": True}

    return app


def test_query_rejects_oversized_body():
    app = _make_app()
    with TestClient(app) as client:
        # Query limit is 16 KiB
        body = "x" * 30_000
        r = client.post(
            "/api/v1/query",
            headers={"Content-Length": str(len(body))},
            content=body,
        )
        assert r.status_code == 413


def test_ingest_accepts_medium_body():
    app = _make_app()
    with TestClient(app) as client:
        body = b'{"k":"' + b"x" * 100_000 + b'"}'
        r = client.post("/api/v1/ingest", content=body,
                         headers={"Content-Type": "application/json"})
        assert r.status_code == 200


def test_default_limit_for_unknown_path():
    app = _make_app()
    with TestClient(app) as client:
        body = b"x" * (DEFAULT_LIMIT + 100)
        r = client.post("/other", content=body,
                         headers={"Content-Type": "application/octet-stream"})
        assert r.status_code == 413


def test_get_is_unaffected():
    app = _make_app()
    with TestClient(app) as client:
        r = client.get("/api/v1/query")
        # No handler for GET, but middleware should not intercept it
        assert r.status_code in {404, 405}
