"""Client retries must carry the same explicit ingest request identity."""

import json
from pathlib import Path

import httpx


def test_python_ingest_retains_key_across_automatic_retries(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "clients/python"))
    from engram_client import EngramClient
    from engram_client import client as client_module

    monkeypatch.setattr(client_module.time, "sleep", lambda _: None)
    bodies = []

    def respond(request):
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            return httpx.Response(503, json={"detail": "retry"})
        return httpx.Response(202, json={"event_id": "evt-1", "pair_id": "pair-1", "status": "RECEIVED"})

    with httpx.Client(base_url="https://example.test", transport=httpx.MockTransport(respond)) as http:
        client = EngramClient(base_url="https://example.test", api_key="test", http=http)
        client.ingest(user="Alice", assistant="Recorded.")
        assert bodies[0]["idempotency_key"] == bodies[1]["idempotency_key"]
        assert bodies[0]["idempotency_key"]
        client.ingest(user="Alice", assistant="Recorded.")
        assert bodies[2]["idempotency_key"] != bodies[0]["idempotency_key"]
        bindings = [{"name": "Alice", "identity_key": "employee:42"}]
        client.ingest(user="Alice", assistant="Recorded.", idempotency_key="caller-key", entity_bindings=bindings)
        assert bodies[3]["idempotency_key"] == "caller-key"
        assert bodies[3]["entity_bindings"] == bindings
