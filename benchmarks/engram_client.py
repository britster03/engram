"""Thin, fail-closed Engram API client for the benchmark harness.

Every conversation gets an isolated tenant. Fresh ingestion retains every
submitted event ID and waits on tenant-scoped artifact readiness for those exact
IDs. Corpus reuse enumerates the tenant's source events, verifies the expected
count, then applies the same exact readiness check. Consolidation is observed
separately because overview readiness is not the same contract as memory
readiness.

``wait_for_drain`` remains only for compatibility with older diagnostics. The
LoCoMo runner does not use its queue-level rise/settle heuristic.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import httpx


class EngramError(RuntimeError):
    """Raised when Engram returns an unexpected HTTP status."""


class DrainTimeoutError(RuntimeError):
    """Raised when ingest does not finish within the allotted time."""


class IngestFailedError(RuntimeError):
    """Raised when one or more exact event IDs reach FAILED."""


@dataclass
class DrainConfig:
    """Deadlines for exact readiness and the legacy queue-settle helper."""

    max_wait_s: float = 600.0      # hard ceiling for one conversation's ingest
    poll_interval_s: float = 2.0   # how often to poll the status endpoint
    settle_s: float = 8.0          # queue must stay empty this long after activity
    activity_grace_s: float = 45.0  # if no activity is ever seen, give up waiting
    #                                 for a rise after this long and treat as drained


@dataclass
class EngramClient:
    """One client, bound to a single tenant's API key.

    Create the tenant + key first with `EngramClient.create_tenant(...)`, which
    returns a client already bound to that tenant.
    """

    base_url: str
    api_key: str
    tenant_id: str = "_default"
    timeout_s: float = 60.0          # ingest / status / health (fast)
    query_timeout_s: float = 300.0   # /query fires several LLM calls; needs headroom
    _http: httpx.Client = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._http = httpx.Client(
            base_url=self.base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.timeout_s,
        )

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> EngramClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- tenant setup (Issue C) -------------------------------------------

    @classmethod
    def create_tenant(
        cls,
        *,
        base_url: str,
        admin_key: str,
        tenant_id: str,
        display_name: str = "",
        timeout_s: float = 60.0,
        query_timeout_s: float = 300.0,
        reuse_if_exists: bool = True,
    ) -> EngramClient:
        """Create an isolated tenant and return a client bound to its key.

        POST /api/v1/admin/tenants returns the fresh tenant's api_key directly,
        so no separate mint call is needed. Requires the admin key.

        If the tenant already exists (409) and `reuse_if_exists` is set, mint a
        fresh key for it instead of failing -- this makes re-runs idempotent.
        """
        admin_http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {admin_key}"},
            timeout=timeout_s,
        )
        try:
            resp = admin_http.post(
                "/api/v1/admin/tenants",
                json={"tenant_id": tenant_id, "display_name": display_name},
            )
            if resp.status_code == 409 and reuse_if_exists:
                key_resp = admin_http.post(f"/api/v1/admin/tenants/{tenant_id}/keys")
                if key_resp.status_code != 200:
                    raise EngramError(
                        f"mint-key for existing tenant {tenant_id} failed: "
                        f"{key_resp.status_code} {key_resp.text}"
                    )
                api_key = key_resp.json()["api_key"]
            elif resp.status_code in (200, 201):
                api_key = resp.json()["api_key"]
            else:
                raise EngramError(
                    f"create-tenant {tenant_id} failed: "
                    f"{resp.status_code} {resp.text}"
                )
        finally:
            admin_http.close()

        return cls(
            base_url=base_url,
            api_key=api_key,
            tenant_id=tenant_id,
            timeout_s=timeout_s,
            query_timeout_s=query_timeout_s,
        )

    @classmethod
    def bind_existing_tenant(
        cls,
        *,
        base_url: str,
        admin_key: str,
        tenant_id: str,
        timeout_s: float = 60.0,
        query_timeout_s: float = 300.0,
    ) -> EngramClient:
        """Mint a key only after proving the versioned corpus tenant exists."""
        admin_http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {admin_key}"},
            timeout=timeout_s,
        )
        try:
            tenant_resp = admin_http.get(f"/api/v1/admin/tenants/{tenant_id}")
            if tenant_resp.status_code != 200:
                raise EngramError(
                    f"reused corpus tenant {tenant_id} is unavailable: "
                    f"{tenant_resp.status_code} {tenant_resp.text}"
                )
            key_resp = admin_http.post(f"/api/v1/admin/tenants/{tenant_id}/keys")
            if key_resp.status_code != 200:
                raise EngramError(
                    f"mint-key for corpus tenant {tenant_id} failed: "
                    f"{key_resp.status_code} {key_resp.text}"
                )
            api_key = str(key_resp.json()["api_key"])
        finally:
            admin_http.close()
        return cls(
            base_url=base_url,
            api_key=api_key,
            tenant_id=tenant_id,
            timeout_s=timeout_s,
            query_timeout_s=query_timeout_s,
        )

    # -- ingest ------------------------------------------------------------

    def ingest_pair(
        self,
        *,
        session_id: str,
        user_content: str,
        assistant_content: str,
        user_timestamp: str | None = None,
        assistant_timestamp: str | None = None,
        user_turn_idx: int | None = None,
        assistant_turn_idx: int | None = None,
        user_external_id: str | None = None,
        assistant_external_id: str | None = None,
        user_speaker: str | None = None,
        assistant_speaker: str | None = None,
        source_conversation_id: str | None = None,
        source_session_id: str | None = None,
        user_image_caption: str | None = None,
        assistant_image_caption: str | None = None,
        user_image_urls: list[str] | None = None,
        assistant_image_urls: list[str] | None = None,
        user_image_query: str | None = None,
        assistant_image_query: str | None = None,
        session_context: str | None = None,
        source: str = "locomo",
        force_store: bool = False,
    ) -> dict[str, Any]:
        """POST one user/assistant turn pair. Returns the 202 body (event_id...).

        Content is capped at Engram's per-turn limit; longer turns raise 422 at
        the API, which we surface rather than silently truncate.
        """
        turn_pair: dict[str, Any] = {
            "user": _turn(
                user_content,
                user_timestamp,
                user_turn_idx,
                external_id=user_external_id,
                speaker=user_speaker,
                source_conversation_id=source_conversation_id,
                source_session_id=source_session_id,
                image_caption=user_image_caption,
                image_urls=user_image_urls,
                image_query=user_image_query,
            ),
            "assistant": _turn(
                assistant_content,
                assistant_timestamp,
                assistant_turn_idx,
                external_id=assistant_external_id,
                speaker=assistant_speaker,
                source_conversation_id=source_conversation_id,
                source_session_id=source_session_id,
                image_caption=assistant_image_caption,
                image_urls=assistant_image_urls,
                image_query=assistant_image_query,
            ),
        }
        body = {
            "session_id": session_id,
            "turn_pair": turn_pair,
            "source": source,
            "force_store": force_store,
        }
        if session_context is not None:
            body["session_context"] = session_context
        resp = self._http.post("/api/v1/ingest", json=body)
        if resp.status_code != 202:
            raise EngramError(f"ingest failed: {resp.status_code} {resp.text}")
        return resp.json()

    # -- drain (Issue A) ---------------------------------------------------

    def consolidation_status(self) -> dict[str, Any]:
        resp = self._http.get("/api/v1/consolidation/status")
        if resp.status_code != 200:
            raise EngramError(f"status failed: {resp.status_code} {resp.text}")
        return resp.json()

    def event_status(self, event_ids: list[str]) -> dict[str, Any]:
        """Return exact tenant-scoped readiness for a bounded event-ID set."""
        resp = self._http.post("/api/v1/events/status", json={"event_ids": event_ids})
        if resp.status_code != 200:
            raise EngramError(f"event status failed: {resp.status_code} {resp.text}")
        return resp.json()

    def wait_for_events(
        self,
        event_ids: list[str],
        cfg: DrainConfig | None = None,
    ) -> dict[str, Any]:
        """Wait until the submitted events are memory-ready or fail closed."""
        if not event_ids:
            raise ValueError("event_ids must not be empty")
        cfg = cfg or DrainConfig()
        start = time.monotonic()
        last: dict[str, Any] = {}
        while True:
            elapsed = time.monotonic() - start
            if elapsed > cfg.max_wait_s:
                raise DrainTimeoutError(
                    f"tenant {self.tenant_id}: {len(event_ids)} exact events did not "
                    f"become memory-ready within {cfg.max_wait_s:.0f}s; last={last}"
                )
            last = self.event_status(event_ids)
            if last.get("missing_ids"):
                raise EngramError(
                    f"event status omitted submitted IDs: {last['missing_ids']}"
                )
            if int(last.get("failed_count", 0)):
                failures = [
                    f"{row.get('event_id')}:{row.get('error') or row.get('status')}"
                    for row in last.get("failures", [])
                ]
                raise IngestFailedError("; ".join(failures))
            if bool(last.get("memory_ready")):
                return {**last, "waited_s": elapsed}
            time.sleep(cfg.poll_interval_s)

    def _is_busy(self, status: dict[str, Any]) -> bool:
        """True while the tenant still has consolidation work outstanding."""
        if int(status.get("queue_depth", 0)) > 0:
            return True
        by_status = status.get("by_status") or {}
        return any(
            int(by_status.get(s, 0)) > 0 for s in ("PENDING", "PROCESSING")
        )

    def wait_for_drain(self, cfg: DrainConfig | None = None) -> dict[str, float]:
        """Block until background ingest/consolidation for this tenant is done.

        Rise-then-settle to defeat the "premature zero" (see module docstring):

          1. Poll until we observe the queue go BUSY at least once -- proof the
             worker actually picked up the just-ingested events. If we never see
             activity within `activity_grace_s`, assume the batch drained faster
             than our poll interval (or produced no consolidation work) and
             proceed.
          2. Once activity has been seen, wait for the queue to read empty
             continuously for `settle_s` -- a momentary dip to zero between two
             tasks does not count as drained.

        All bounded by `max_wait_s`. Returns timing telemetry for logging.
        """
        cfg = cfg or DrainConfig()
        start = time.monotonic()
        seen_activity = False
        idle_since: float | None = None

        while True:
            now = time.monotonic()
            elapsed = now - start
            if elapsed > cfg.max_wait_s:
                raise DrainTimeoutError(
                    f"tenant {self.tenant_id}: ingest did not drain within "
                    f"{cfg.max_wait_s:.0f}s (seen_activity={seen_activity})"
                )

            busy = self._is_busy(self.consolidation_status())

            if busy:
                seen_activity = True
                idle_since = None
            else:
                if not seen_activity:
                    # Possibly a premature zero: worker hasn't started. Only give
                    # up waiting for a rise after the grace period.
                    if elapsed >= cfg.activity_grace_s:
                        return {
                            "waited_s": elapsed,
                            "saw_activity": 0.0,
                        }
                else:
                    # Activity happened and the queue is now empty -> settle.
                    if idle_since is None:
                        idle_since = now
                    elif now - idle_since >= cfg.settle_s:
                        return {
                            "waited_s": time.monotonic() - start,
                            "saw_activity": 1.0,
                        }

            time.sleep(cfg.poll_interval_s)

    def wait_for_overview_ready(self, cfg: DrainConfig | None = None) -> dict[str, Any]:
        """Wait for tenant consolidation after exact events are memory-ready.

        Unlike the legacy rise-then-settle drain, this is called only after
        every requested event has committed its consolidation intent. A zero
        queue is therefore authoritative and cannot be the premature-zero
        race that exists immediately after submission.
        """
        cfg = cfg or DrainConfig()
        started = time.monotonic()
        while True:
            elapsed = time.monotonic() - started
            if elapsed > cfg.max_wait_s:
                raise DrainTimeoutError(
                    f"tenant {self.tenant_id}: overviews did not become ready "
                    f"within {cfg.max_wait_s:.0f}s"
                )
            status = self.consolidation_status()
            if not self._is_busy(status):
                return {**status, "overview_ready": True, "waited_s": elapsed}
            time.sleep(cfg.poll_interval_s)

    def list_events(self, *, source: str, limit: int = 500) -> dict[str, Any]:
        """Enumerate this tenant's source events before reusing a corpus."""
        resp = self._http.get(
            "/api/v1/events",
            params={"source": source, "limit": limit},
        )
        if resp.status_code != 200:
            raise EngramError(f"event list failed: {resp.status_code} {resp.text}")
        return resp.json()

    # -- query -------------------------------------------------------------

    def query(
        self,
        question: str,
        *,
        max_depth: str | None = None,
        min_depth: str | None = None,
        max_reentries: int | None = None,
        session_context: str | None = None,
        include_trace: bool = True,
        force_retrieval: bool = True,
        retrieval_mode: str = "adaptive",
    ) -> dict[str, Any]:
        """POST a question. Returns {answer, retrieval_metadata, ...}.

        The full `retrieval_metadata` is preserved by the caller -- it is what
        turns a bare score into failure analysis (l0_decision, cascade depth,
        nodes retrieved, latency).
        """
        body: dict[str, Any] = {"query": question}
        if max_depth is not None:
            body["max_depth"] = max_depth
        if min_depth is not None:
            body["min_depth"] = min_depth
        if max_reentries is not None:
            body["max_reentries"] = max_reentries
        if session_context is not None:
            body["session_context"] = session_context
        body["include_trace"] = include_trace
        body["retrieval_mode"] = retrieval_mode
        # Avoid an ambiguous payload for explicit ablation modes. The legacy
        # flag remains useful for older servers when the adaptive mode is used.
        if retrieval_mode == "adaptive":
            body["force_retrieval"] = force_retrieval
        # Longer per-request timeout than the client default: a query drives the
        # full cascade + several LLM calls. A transport error (incl. timeout) is
        # wrapped as EngramError so callers catch it uniformly and one slow query
        # cannot abort a whole benchmark run.
        try:
            resp = self._http.post(
                "/api/v1/query", json=body, timeout=self.query_timeout_s
            )
        except httpx.HTTPError as err:
            raise EngramError(f"query request failed: {err}") from err
        if resp.status_code != 200:
            raise EngramError(f"query failed: {resp.status_code} {resp.text}")
        return resp.json()

    # -- health ------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        resp = self._http.get("/api/v1/health")
        if resp.status_code != 200:
            raise EngramError(f"health failed: {resp.status_code} {resp.text}")
        return resp.json()

    def configuration(self) -> dict[str, Any]:
        """Return the authenticated, non-secret effective runtime configuration."""
        resp = self._http.get("/api/v1/config")
        if resp.status_code != 200:
            raise EngramError(f"config failed: {resp.status_code} {resp.text}")
        return resp.json()

    def metrics_text(self) -> str:
        """Return the process metrics snapshot used for benchmark deltas."""
        resp = self._http.get("/metrics")
        if resp.status_code != 200:
            raise EngramError(f"metrics failed: {resp.status_code} {resp.text}")
        return resp.text


def _turn(
    content: str,
    timestamp: str | None,
    turn_idx: int | None,
    *,
    external_id: str | None = None,
    speaker: str | None = None,
    source_conversation_id: str | None = None,
    source_session_id: str | None = None,
    image_caption: str | None = None,
    image_urls: list[str] | None = None,
    image_query: str | None = None,
) -> dict[str, Any]:
    turn: dict[str, Any] = {"content": content}
    if timestamp is not None:
        turn["timestamp"] = timestamp
    if turn_idx is not None:
        turn["turn_idx"] = turn_idx
    optional = {
        "external_id": external_id,
        "speaker": speaker,
        "source_conversation_id": source_conversation_id,
        "source_session_id": source_session_id,
        "source_task": "locomo",
        "image_caption": image_caption,
        "image_urls": image_urls,
        "image_query": image_query,
    }
    turn.update({key: value for key, value in optional.items() if value is not None})
    return turn


if __name__ == "__main__":
    # Tiny connectivity smoke test. Needs a running Engram + env vars:
    #   ENGRAM_BASE_URL (default http://127.0.0.1:8000)
    #   ENGRAM_API_KEY  (default local tenant key)
    import os

    base = os.environ.get("ENGRAM_BASE_URL", "http://127.0.0.1:8000")
    key = os.environ.get("ENGRAM_API_KEY")
    if not key:
        raise SystemExit("set ENGRAM_API_KEY to run the smoke test")

    with EngramClient(base_url=base, api_key=key) as client:
        print("health:", client.health())
        print("consolidation status:", client.consolidation_status())
