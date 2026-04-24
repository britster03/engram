"""E2E tests for Engram API endpoints.

Since admin UI pages (/admin/*) are NOT mounted in the app,
these tests verify the REST API endpoints directly.
"""
import pytest
import requests

pytestmark = pytest.mark.e2e

BASE_URL = "http://localhost:8000"


def _api_key():
    key = None
    try:
        with open(".env") as f:
            for line in f:
                if line.startswith("ENGRAM_API_KEY="):
                    key = line.strip().split("=", 1)[1]
                    break
    except FileNotFoundError:
        pass
    return key or ""


def _admin_key():
    key = None
    try:
        with open(".env") as f:
            for line in f:
                if line.startswith("ENGRAM_ADMIN_KEY="):
                    key = line.strip().split("=", 1)[1]
                    break
    except FileNotFoundError:
        pass
    return key or ""


class TestAPIHealth:
    def test_health(self, base_url):
        resp = requests.get(f"{base_url}/api/v1/health", timeout=30)
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "components" in data


class TestAPIAuth:
    def test_config_requires_auth(self, base_url):
        resp = requests.get(f"{base_url}/api/v1/config", timeout=10)
        assert resp.status_code in (401, 403)

    def test_config_with_auth(self, base_url):
        key = _api_key()
        if not key:
            pytest.skip("No API key available")
        resp = requests.get(
            f"{base_url}/api/v1/config",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10
        )
        assert resp.status_code == 200


class TestIngest:
    def test_ingest(self, base_url):
        key = _api_key()
        if not key:
            pytest.skip("No API key available")
        turn_pair = {
            "user": {"content": "Hello", "timestamp": "2026-04-25T00:00:00Z"},
            "assistant": {"content": "Hi", "timestamp": "2026-04-25T00:00:01Z"}
        }
        resp = requests.post(
            f"{base_url}/api/v1/ingest",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"session_id": "test-001", "turn_pair": turn_pair},
            timeout=30
        )
        assert resp.status_code in (200, 202)
        data = resp.json()
        assert "pair_id" in data or "event_id" in data


class TestQuery:
    def test_query(self, base_url):
        key = _api_key()
        if not key:
            pytest.skip("No API key available")
        resp = requests.post(
            f"{base_url}/api/v1/query",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"query": "test query"},
            timeout=60
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "answer" in data or "retrieval_metadata" in data


class TestSessions:
    def test_create_session(self, base_url):
        key = _api_key()
        if not key:
            pytest.skip("No API key available")
        resp = requests.post(
            f"{base_url}/api/v1/sessions",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10
        )
        assert resp.status_code in (200, 201)
        data = resp.json()
        assert "session_id" in data


class TestAdminAPI:
    def test_admin_no_auth(self, base_url):
        resp = requests.get(f"{base_url}/api/v1/admin/tenants", timeout=10)
        assert resp.status_code in (401, 403, 404)

    def test_admin_with_auth(self, base_url):
        key = _admin_key()
        if not key:
            pytest.skip("No admin key available")
        resp = requests.get(
            f"{base_url}/api/v1/admin/tenants",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10
        )
        assert resp.status_code in (200, 403)
