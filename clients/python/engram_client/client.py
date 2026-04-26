"""Blocking (and streaming) Python client for Engram."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from typing import Any

import httpx

from engram_client.models import (
    ConsolidationStatus,
    HealthPayload,
    IngestResponse,
    QueryResponse,
    SessionState,
    TenantPayload,
)

log = logging.getLogger(__name__)


class EngramError(RuntimeError):
    def __init__(self, status_code: int, detail: Any, request_id: str | None = None):
        super().__init__(f"{status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail
        self.request_id = request_id


class EngramClient:
    """Synchronous Engram client.

    For async, use `AsyncEngramClient` (mirror implementation, httpx.AsyncClient).

    Retries 429 and 5xx responses with exponential backoff up to `retries`
    attempts. Honours `Retry-After`.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout: float = 60.0,
        retries: int = 3,
        user_agent: str = "engram-python/0.1.0",
        http: httpx.Client | None = None,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("base_url must include scheme (http:// or https://)")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.retries = retries
        self._http = http or httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            headers={"User-Agent": user_agent},
            http2=True,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> EngramClient:
        return self

    def __exit__(self, *_a) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Core calls
    # ------------------------------------------------------------------

    def ingest(
        self,
        *,
        user: str,
        assistant: str,
        session_id: str | None = None,
        user_turn_idx: int = 0,
        assistant_turn_idx: int | None = None,
        source: str = "client",
    ) -> IngestResponse:
        body = {
            "session_id": session_id,
            "source": source,
            "turn_pair": {
                "user": {"content": user, "turn_idx": user_turn_idx},
                "assistant": {
                    "content": assistant,
                    "turn_idx": assistant_turn_idx or user_turn_idx + 1,
                },
            },
        }
        raw = self._post("/api/v1/ingest", body)
        return IngestResponse.model_validate(raw)

    def query(
        self,
        query: str,
        *,
        session_id: str | None = None,
        session_context: str | None = None,
        max_depth: str | None = None,
        max_reentries: int | None = None,
    ) -> QueryResponse:
        body = {
            "session_id": session_id,
            "query": query,
            "session_context": session_context,
            "max_depth": max_depth,
            "max_reentries": max_reentries,
        }
        raw = self._post("/api/v1/query", body)
        return QueryResponse.model_validate(raw)

    def query_stream(
        self,
        query: str,
        *,
        session_id: str | None = None,
        session_context: str | None = None,
    ) -> Iterator[str]:
        """Stream a query response. Yields the answer text chunks in order."""
        body = {
            "session_id": session_id,
            "query": query,
            "session_context": session_context,
            "stream": True,
        }
        with self._http.stream(
            "POST", "/api/v1/query", json=body,
            headers=self._auth_headers(),
        ) as r:
            r.raise_for_status()
            # Very small SSE parser — handles the events we emit: metadata, delta, done, error
            event = None
            for line in r.iter_lines():
                if line.startswith("event:"):
                    event = line[len("event:"):].strip()
                elif line.startswith("data:"):
                    data = line[len("data:"):].strip()
                    if event == "delta":
                        try:
                            payload = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                        yield payload.get("text", "")
                    elif event == "error":
                        raise EngramError(500, data)
                    elif event == "done":
                        return

    def chat_completions(
        self,
        messages: list[dict[str, str]],
        *,
        session_id: str | None = None,
        stream: bool = False,
        session_context: str | None = None,
        max_depth: str | None = None,
        max_reentries: int | None = None,
    ) -> dict[str, Any]:
        body = {
            "messages": messages,
            "session_id": session_id,
            "stream": stream,
            "session_context": session_context,
            "max_depth": max_depth,
            "max_reentries": max_reentries,
        }
        return self._post("/api/v1/chat/completions", body)

    def chat_completions_stream(
        self,
        messages: list[dict[str, str]],
        *,
        session_id: str | None = None,
        session_context: str | None = None,
        max_depth: str | None = None,
        max_reentries: int | None = None,
    ) -> Iterator[str]:
        body = {
            "messages": messages,
            "session_id": session_id,
            "session_context": session_context,
            "max_depth": max_depth,
            "max_reentries": max_reentries,
            "stream": True,
        }
        with self._http.stream(
            "POST", "/api/v1/chat/completions", json=body,
            headers=self._auth_headers(),
        ) as r:
            r.raise_for_status()
            event = None
            for line in r.iter_lines():
                if line.startswith("event:"):
                    event = line[len("event:"):].strip()
                elif line.startswith("data:"):
                    data = line[len("data:"):].strip()
                    if event == "delta":
                        try:
                            payload = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                        yield payload.get("text", "")
                    elif event == "error":
                        raise EngramError(500, data)
                    elif event == "done":
                        return

    # ------------------------------------------------------------------
    # Session helpers
    # ------------------------------------------------------------------

    def create_session(self) -> str:
        raw = self._post("/api/v1/sessions", {})
        return raw["session_id"]

    def get_session(self, session_id: str) -> SessionState:
        raw = self._get(f"/api/v1/sessions/{session_id}")
        return SessionState.model_validate(raw)

    def end_session(self, session_id: str) -> None:
        self._delete(f"/api/v1/sessions/{session_id}")

    def send_message(
        self, session_id: str, *, user: str, assistant: str,
    ) -> IngestResponse:
        body = {"user": user, "assistant": assistant}
        raw = self._post(f"/api/v1/sessions/{session_id}/message", body)
        return IngestResponse.model_validate({
            "event_id": raw["event_id"],
            "pair_id": raw["pair_id"],
            "status": "RECEIVED",
        })

    # ------------------------------------------------------------------
    # Memory helpers
    # ------------------------------------------------------------------

    def get_memory(self, source_uri: str) -> dict[str, Any]:
        return self._get(f"/api/v1/memories/{_strip_scheme(source_uri)}")

    def list_memories(
        self, *, prefix: str = "mem://", limit: int = 50, cursor: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"prefix": prefix, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._get("/api/v1/memories", params=params)

    def retire(self, source_uri: str) -> dict[str, Any]:
        return self._post(f"/api/v1/memories/{_strip_scheme(source_uri)}/retire", {})

    def unmerge(self, source_uri: str) -> dict[str, Any]:
        return self._post(f"/api/v1/memories/{_strip_scheme(source_uri)}/unmerge", {})

    # ------------------------------------------------------------------
    # Admin (uses the same client; supply the admin key as api_key)
    # ------------------------------------------------------------------

    def create_tenant(
        self, tenant_id: str, *, display_name: str = "", quotas: dict[str, int] | None = None,
    ) -> tuple[TenantPayload, str]:
        """Create a tenant. Returns (payload, api_key). The api_key is
        shown exactly once — persist it immediately.
        """
        body = {"tenant_id": tenant_id, "display_name": display_name}
        if quotas:
            body["quotas"] = quotas
        raw = self._post("/api/v1/admin/tenants", body)
        api_key = raw.pop("api_key")
        return TenantPayload.model_validate(raw), api_key

    def list_tenants(self) -> list[TenantPayload]:
        raw = self._get("/api/v1/admin/tenants")
        return [TenantPayload.model_validate(t) for t in raw]

    def mint_tenant_key(self, tenant_id: str) -> str:
        raw = self._post(f"/api/v1/admin/tenants/{tenant_id}/keys", {})
        return raw["api_key"]

    def revoke_tenant_key(self, tenant_id: str, key_hash: str) -> None:
        self._delete(f"/api/v1/admin/tenants/{tenant_id}/keys/{key_hash}")

    def suspend_tenant(self, tenant_id: str) -> TenantPayload:
        raw = self._post(f"/api/v1/admin/tenants/{tenant_id}/suspend", {})
        return TenantPayload.model_validate(raw)

    def resume_tenant(self, tenant_id: str) -> TenantPayload:
        raw = self._post(f"/api/v1/admin/tenants/{tenant_id}/resume", {})
        return TenantPayload.model_validate(raw)

    # ------------------------------------------------------------------
    # Ops
    # ------------------------------------------------------------------

    def health(self) -> HealthPayload:
        raw = self._get("/api/v1/health", auth=False)
        return HealthPayload.model_validate(raw)

    def consolidation_status(self) -> ConsolidationStatus:
        raw = self._get("/api/v1/consolidation/status")
        return ConsolidationStatus.model_validate(raw)

    # ------------------------------------------------------------------
    # HTTP plumbing
    # ------------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _with_retry(self, method: str, path: str, **kwargs) -> httpx.Response:
        last_err: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                r = self._http.request(method, path, **kwargs)
            except httpx.HTTPError as err:
                last_err = err
                if attempt >= self.retries:
                    raise
                time.sleep(min(8.0, 0.5 * (2 ** attempt)))
                continue
            if r.status_code < 500 and r.status_code != 429:
                return r
            if attempt >= self.retries:
                return r
            retry_after = float(r.headers.get("Retry-After", "0")) or min(8.0, 0.5 * (2 ** attempt))
            time.sleep(retry_after)
        raise last_err or RuntimeError("unreachable")

    def _raise_for_status(self, r: httpx.Response) -> None:
        if r.status_code < 400:
            return
        detail: Any
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        raise EngramError(r.status_code, detail, r.headers.get("X-Request-ID"))

    def _get(
        self, path: str, *, params: dict[str, Any] | None = None, auth: bool = True,
    ) -> Any:
        headers = self._auth_headers() if auth else {"User-Agent": self._http.headers.get("User-Agent", "")}
        r = self._with_retry("GET", path, params=params, headers=headers)
        self._raise_for_status(r)
        return r.json()

    def _post(self, path: str, body: dict[str, Any]) -> Any:
        r = self._with_retry("POST", path, json=body, headers=self._auth_headers())
        self._raise_for_status(r)
        return r.json() if r.content else {}

    def _delete(self, path: str) -> None:
        r = self._with_retry("DELETE", path, headers=self._auth_headers())
        self._raise_for_status(r)


def _strip_scheme(uri: str) -> str:
    if uri.startswith("mem://"):
        return uri[len("mem://"):]
    return uri
