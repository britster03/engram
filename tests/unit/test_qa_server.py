from fastapi.testclient import TestClient

from scripts.qa_server import app


def test_qa_server_serves_current_admin_screens():
    with TestClient(app) as client:
        for path in ("/admin/login", "/admin/dashboard", "/admin/chat", "/admin/kg"):
            response = client.get(path)

            assert response.status_code == 200
            assert "Engram" in response.text


def test_qa_server_admin_templates_include_responsive_and_null_safe_controls():
    with TestClient(app) as client:
        dashboard = client.get("/admin/dashboard")
        assert dashboard.status_code == 200
        assert "bulkJob?.rejected_rows || []" in dashboard.text
        assert "lg:hidden" in dashboard.text
        assert 'href="/admin/chat"' in dashboard.text
        assert 'href="/admin/kg"' in dashboard.text

        kg = client.get("/admin/kg")
        assert kg.status_code == 200
        assert 'x-model="nodeType" @change="load()"' in kg.text


def test_qa_server_stubs_current_admin_api_calls():
    with TestClient(app) as client:
        graph = client.get("/api/v1/kg/graph")
        assert graph.status_code == 200
        assert graph.json()["nodes"]

        chat = client.post(
            "/api/v1/chat/completions",
            json={"stream": True, "messages": [{"role": "user", "content": "Hello"}]},
        )
        assert chat.status_code == 200
        assert chat.headers["content-type"].startswith("text/event-stream")
        assert "simulated Engram chat response" in chat.text

        bulk = client.post(
            "/api/v1/ingest/bulk",
            data={"dry_run": "true"},
            files={
                "file": (
                    "turns.jsonl",
                    b'{"user":"hi","assistant":"stored"}\n',
                    "application/x-ndjson",
                )
            },
        )
        assert bulk.status_code == 202
        assert bulk.json()["status"] == "DRY_RUN"
