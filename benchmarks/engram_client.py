"""Thin Engram API client for the benchmark harness.

Wraps the four things the harness needs from a running Engram instance and,
critically, solves the two correctness issues that would otherwise make the
baseline lie:

* Issue C (cross-conversation contamination) -> `create_tenant()` gives each
  conversation its own isolated memory space. Engram's multi-tenancy does the
  actual isolation; we just create one tenant per conversation and send that
  conversation's traffic under its key.

* Issue A (async ingest race) -> `wait_for_drain()`. Engram's /ingest returns
  202 immediately and builds the memory in a background worker. Querying before
  that finishes scores ~0% and looks like a retrieval failure when it is really
  a timing bug. We block until the background work is done.

Drain detection watches BOTH background stages:

  1. Ingest — the event ledger's per-tenant RECEIVED/PROCESSING counts. Event
     rows are INSERTed synchronously by POST /ingest, so this count is accurate
     the instant we finish submitting (no "premature zero" to guard against).
  2. Consolidation — GET /api/v1/consolidation/status.

Watching consolidation ALONE is unsafe, and this is not hypothetical: with
Neo4j down, 175 of 214 events never left RECEIVED, so consolidation (which is
enqueued at the LAST step of ingest) never started, its queue read empty, and
a consolidation-only check reported "drained" after 46s. The benchmark then
queried a nearly-empty memory and produced a real-looking 0%. Stage 1 is the
one that actually decides whether memories exist, so it is now authoritative.

`wait_for_drain()` also surfaces terminal event counts (COMPLETE, FAILED,
GATED_SKIP) so the caller can refuse to score a run whose ingest did not
actually succeed.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

# Event statuses that mean "ingest still owes us work for this tenant".
_PENDING_EVENT_STATUSES = ("RECEIVED", "PROCESSING", "GATED_STORE", "INDEXED")

# Where the server's event ledger usually lives, relative to the harness cwd.
_LEDGER_CANDIDATES = (
    "data/event_ledger.db",
    "engram/data/event_ledger.db",
    "../data/event_ledger.db",
)


def find_ledger(explicit: str | None = None) -> Path | None:
    """Locate the SQLite event ledger the running server writes to."""
    if explicit:
        p = Path(explicit)
        return p if p.exists() else None
    existing = [Path(c) for c in _LEDGER_CANDIDATES if Path(c).exists()]
    if not existing:
        return None
    # More than one copy can exist if the server was ever started from a
    # different directory; the freshest one is the live database.
    return max(existing, key=lambda p: p.stat().st_mtime)


class EngramError(RuntimeError):
    """Raised when Engram returns an unexpected HTTP status."""


class DrainTimeout(RuntimeError):
    """Raised when ingest does not finish within the allotted time."""


@dataclass
class DrainConfig:
    """Tunables for the drain wait."""

    max_wait_s: float = 600.0      # hard ceiling for one conversation's ingest
    poll_interval_s: float = 2.0   # how often to poll
    settle_s: float = 8.0          # everything must stay quiet this long
    activity_grace_s: float = 45.0  # consolidation-only fallback: how long to
    #                                 wait for the queue to rise before assuming
    #                                 it drained faster than we could observe
    stall_timeout_s: float = 180.0  # abort if pending counts stop moving at all


@dataclass
class DrainResult:
    """Outcome of a drain wait — enough for the caller to judge run health."""

    waited_s: float
    by_status: dict[str, int]      # terminal + pending event counts for tenant
    source: str                    # "ledger" (authoritative) or "consolidation"

    @property
    def total(self) -> int:
        return sum(self.by_status.values())

    @property
    def failed(self) -> int:
        return self.by_status.get("FAILED", 0)

    @property
    def pending(self) -> int:
        return sum(self.by_status.get(s, 0) for s in _PENDING_EVENT_STATUSES)

    @property
    def stored(self) -> int:
        """Events that produced memory (COMPLETE); GATED_SKIP stored nothing."""
        return self.by_status.get("COMPLETE", 0)

    def healthy(self, *, min_stored_ratio: float = 0.5) -> bool:
        """True when ingest actually produced memory for most submitted events.

        A run that fails this should not be scored: the questions would be
        answered against a memory that was never built.
        """
        if self.total == 0:
            return False
        if self.pending or self.failed:
            return False
        return (self.stored / self.total) >= min_stored_ratio

    def summary(self) -> str:
        parts = ", ".join(f"{k}={v}" for k, v in sorted(self.by_status.items()))
        return f"{parts or '(no events)'} [source={self.source}]"


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
    ledger_path: str | None = None   # event ledger; auto-located when None
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
        ledger_path: str | None = None,
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
            ledger_path=ledger_path,
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
        source: str = "locomo",
    ) -> dict[str, Any]:
        """POST one user/assistant turn pair. Returns the 202 body (event_id...).

        Content is capped at Engram's per-turn limit; longer turns raise 422 at
        the API, which we surface rather than silently truncate.
        """
        turn_pair: dict[str, Any] = {
            "user": _turn(user_content, user_timestamp, user_turn_idx),
            "assistant": _turn(
                assistant_content, assistant_timestamp, assistant_turn_idx
            ),
        }
        body = {"session_id": session_id, "turn_pair": turn_pair, "source": source}
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

    def ingest_status(self) -> dict[str, int] | None:
        """Per-tenant event counts by status, straight from the event ledger.

        This is the authoritative "did ingest actually happen" signal. Returns
        None when the ledger cannot be located or read, so callers can fall
        back to the (weaker) consolidation-only signal.
        """
        ledger = find_ledger(self.ledger_path)
        if ledger is None:
            return None
        try:
            con = sqlite3.connect(f"file:{ledger}?mode=ro", uri=True, timeout=5.0)
            try:
                rows = con.execute(
                    "SELECT status, COUNT(*) FROM events WHERE tenant_id = ? "
                    "GROUP BY status",
                    (self.tenant_id,),
                ).fetchall()
            finally:
                con.close()
        except sqlite3.Error:
            # Busy/locked while the server writes — treat as "unknown this tick".
            return None
        return {str(status): int(count) for status, count in rows}

    def _is_busy(self, status: dict[str, Any]) -> bool:
        """True while the tenant still has consolidation work outstanding."""
        if int(status.get("queue_depth", 0)) > 0:
            return True
        by_status = status.get("by_status") or {}
        return any(
            int(by_status.get(s, 0)) > 0 for s in ("PENDING", "PROCESSING")
        )

    def wait_for_drain(self, cfg: DrainConfig | None = None) -> DrainResult:
        """Block until this tenant's background ingest + consolidation is done.

        Ingest is authoritative: an event row exists the moment /ingest returns,
        so "any RECEIVED/PROCESSING/GATED_STORE/INDEXED rows" is an exact answer
        to "is there work left?". Only once ingest is fully terminal do we also
        wait for the consolidation queue to settle.

        Raises DrainTimeout on `max_wait_s`, or when pending counts stop moving
        for `stall_timeout_s` (a stalled worker — e.g. Neo4j down — would
        otherwise burn the full timeout and then be scored as if it worked).
        The exception message carries the status breakdown.
        """
        cfg = cfg or DrainConfig()
        start = time.monotonic()
        idle_since: float | None = None
        last_pending: int | None = None
        last_change = start
        ledger_seen = False

        while True:
            now = time.monotonic()
            elapsed = now - start

            counts = self.ingest_status()
            if counts is None:
                # No ledger access — fall back to the weaker consolidation-only
                # wait so the harness still works off-box.
                if not ledger_seen:
                    return self._drain_consolidation_only(cfg, start)
                pending = last_pending or 0
            else:
                ledger_seen = True
                pending = sum(counts.get(s, 0) for s in _PENDING_EVENT_STATUSES)

            if pending != last_pending:
                last_pending = pending
                last_change = now

            if elapsed > cfg.max_wait_s:
                raise DrainTimeout(
                    f"tenant {self.tenant_id}: not drained within "
                    f"{cfg.max_wait_s:.0f}s; pending={pending} "
                    f"counts={counts or '{}'}"
                )
            if pending > 0 and (now - last_change) > cfg.stall_timeout_s:
                raise DrainTimeout(
                    f"tenant {self.tenant_id}: ingest STALLED — pending={pending} "
                    f"unchanged for {cfg.stall_timeout_s:.0f}s. Is Neo4j/Redis up? "
                    f"counts={counts or '{}'}"
                )

            quiet = pending == 0 and not self._is_busy(self.consolidation_status())
            if quiet:
                if idle_since is None:
                    idle_since = now
                elif now - idle_since >= cfg.settle_s:
                    return DrainResult(
                        waited_s=time.monotonic() - start,
                        by_status=counts or {},
                        source="ledger",
                    )
            else:
                idle_since = None

            time.sleep(cfg.poll_interval_s)

    def _drain_consolidation_only(
        self, cfg: DrainConfig, start: float
    ) -> DrainResult:
        """Fallback when the event ledger is unreachable (rise-then-settle).

        Weaker than the ledger check — it cannot distinguish "ingest finished"
        from "ingest never started" — so it is used only when we have no
        ledger access at all.
        """
        seen_activity = False
        idle_since: float | None = None
        while True:
            now = time.monotonic()
            elapsed = now - start
            if elapsed > cfg.max_wait_s:
                raise DrainTimeout(
                    f"tenant {self.tenant_id}: consolidation did not drain "
                    f"within {cfg.max_wait_s:.0f}s"
                )
            if self._is_busy(self.consolidation_status()):
                seen_activity = True
                idle_since = None
            elif not seen_activity:
                if elapsed >= cfg.activity_grace_s:
                    return DrainResult(elapsed, {}, "consolidation")
            else:
                if idle_since is None:
                    idle_since = now
                elif now - idle_since >= cfg.settle_s:
                    return DrainResult(
                        time.monotonic() - start, {}, "consolidation"
                    )
            time.sleep(cfg.poll_interval_s)

    # -- query -------------------------------------------------------------

    def query(
        self,
        question: str,
        *,
        max_depth: str | None = None,
        max_reentries: int | None = None,
        session_context: str | None = None,
    ) -> dict[str, Any]:
        """POST a question. Returns {answer, retrieval_metadata, ...}.

        The full `retrieval_metadata` is preserved by the caller -- it is what
        turns a bare score into failure analysis (l0_decision, cascade depth,
        nodes retrieved, latency).
        """
        body: dict[str, Any] = {"query": question}
        if max_depth is not None:
            body["max_depth"] = max_depth
        if max_reentries is not None:
            body["max_reentries"] = max_reentries
        if session_context is not None:
            body["session_context"] = session_context
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


def _turn(
    content: str, timestamp: str | None, turn_idx: int | None
) -> dict[str, Any]:
    turn: dict[str, Any] = {"content": content}
    if timestamp is not None:
        turn["timestamp"] = timestamp
    if turn_idx is not None:
        turn["turn_idx"] = turn_idx
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
