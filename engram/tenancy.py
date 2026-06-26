"""Multi-tenant model — tenant registry, context propagation, auth binding.

Each tenant is a logical slice of the system:
  - its own sub-directory under the filesystem root
  - `tenant_id` property on every KG node + filter in every Cypher template
  - `tenant_id` column on every SQLite/Postgres table + filter in every query
  - its own rate-limit bucket + token / memory quotas
  - its own API keys (rotatable; stored as SHA-256 hashes)

Single-tenant deployments use the built-in `_default` tenant — all the
tenant machinery stays out of the way in that configuration.

Tenant identity propagates to every handler via a `ContextVar`. Middleware
resolves the tenant from the bearer token, then the entire request is
processed under that tenant's context without threading it through every
function signature.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import re
import secrets
import sqlite3
import threading
import time
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_TENANT_ID = "_default"
_TENANT_ID_RE = re.compile(r"^[a-z0-9_][a-z0-9\-_]{0,62}$")

_current_tenant: contextvars.ContextVar[Tenant | None] = contextvars.ContextVar(
    "engram_current_tenant", default=None
)


@dataclass
class TenantQuotas:
    """Per-tenant resource caps. Use -1 for unlimited."""
    requests_per_minute: int = 120
    ingest_per_minute: int = 600
    max_memories: int = -1                   # hard cap on KG node count
    max_monthly_tokens: int = -1             # frontier token ceiling
    max_consolidation_backlog: int = 10_000


@dataclass
class Tenant:
    tenant_id: str
    display_name: str
    api_key_hashes: list[str] = field(default_factory=list)
    quotas: TenantQuotas = field(default_factory=TenantQuotas)
    created_at: str = ""
    status: str = "ACTIVE"                   # ACTIVE | SUSPENDED | DELETED

    def matches_key(self, api_key: str) -> bool:
        digest = hash_key(api_key)
        return digest in self.api_key_hashes

    def to_payload(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "display_name": self.display_name,
            "api_key_hashes": list(self.api_key_hashes),
            "quotas": asdict(self.quotas),
            "created_at": self.created_at,
            "status": self.status,
        }

    @classmethod
    def from_payload(cls, data: dict[str, Any]) -> Tenant:
        quotas = TenantQuotas(**data.get("quotas", {}))
        return cls(
            tenant_id=data["tenant_id"],
            display_name=data.get("display_name", ""),
            api_key_hashes=list(data.get("api_key_hashes", [])),
            quotas=quotas,
            created_at=data.get("created_at", ""),
            status=data.get("status", "ACTIVE"),
        )


# ----------------------------------------------------------------------
# API key helpers
# ----------------------------------------------------------------------

def hash_key(api_key: str) -> str:
    """Return a SHA-256 hex digest of an API key.

    API keys are never stored in plaintext. A newly-minted key is returned
    once to the admin caller; future lookups match by hash.
    """
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def generate_api_key(*, prefix: str = "engram") -> str:
    """Generate a cryptographically-random API key.

    Shape: `engram_<32-char-urlsafe-random>`. Long enough to resist
    brute force, short enough for humans to paste.
    """
    return f"{prefix}_{secrets.token_urlsafe(24)}"


def validate_tenant_id(tenant_id: str) -> None:
    if not _TENANT_ID_RE.match(tenant_id):
        raise ValueError(
            f"invalid tenant_id {tenant_id!r}: must match {_TENANT_ID_RE.pattern}"
        )


# ----------------------------------------------------------------------
# Registry (persisted in SQLite under the main control-plane DB)
# ----------------------------------------------------------------------

_REGISTRY_SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id      TEXT PRIMARY KEY,
    payload        TEXT NOT NULL,
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS api_keys (
    key_hash       TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL REFERENCES tenants(tenant_id),
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_used_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);
"""


class TenantRegistry:
    """Persistent tenant store keyed by tenant_id.

    Stored in the control-plane SQLite (so tests and single-node deploys
    don't need an extra dependency). The Postgres backend uses identical
    SQL except for `datetime('now')` → `now()`; that swap is handled
    by the PG store's constructor.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._tls = threading.local()
        self._init()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._tls, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                str(self._path), isolation_level=None, check_same_thread=False,
                timeout=30.0,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._tls.conn = conn
        return conn

    def _init(self) -> None:
        self._conn().executescript(_REGISTRY_SCHEMA)

    # ------------------------------------------------------------------
    # Tenant CRUD
    # ------------------------------------------------------------------

    def create(
        self,
        tenant_id: str,
        *,
        display_name: str = "",
        quotas: TenantQuotas | None = None,
    ) -> tuple[Tenant, str]:
        """Create a tenant and return (tenant, freshly-minted API key).

        The API key is shown once and never stored in plaintext.
        """
        validate_tenant_id(tenant_id)
        existing = self.get(tenant_id)
        if existing is not None:
            raise ValueError(f"tenant already exists: {tenant_id}")
        tenant = Tenant(
            tenant_id=tenant_id,
            display_name=display_name or tenant_id,
            api_key_hashes=[],
            quotas=quotas or TenantQuotas(),
            created_at=_now_iso(),
        )
        api_key = generate_api_key()
        digest = hash_key(api_key)
        tenant.api_key_hashes.append(digest)

        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO tenants (tenant_id, payload, updated_at) "
                "VALUES (?, ?, datetime('now'))",
                (tenant.tenant_id, json.dumps(tenant.to_payload())),
            )
            conn.execute(
                "INSERT INTO api_keys (key_hash, tenant_id) VALUES (?, ?)",
                (digest, tenant.tenant_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return tenant, api_key

    def get(self, tenant_id: str) -> Tenant | None:
        row = self._conn().execute(
            "SELECT payload FROM tenants WHERE tenant_id = ?", (tenant_id,),
        ).fetchone()
        if row is None:
            return None
        return Tenant.from_payload(json.loads(row["payload"]))

    def list(self) -> list[Tenant]:
        rows = self._conn().execute(
            "SELECT payload FROM tenants ORDER BY tenant_id"
        ).fetchall()
        return [Tenant.from_payload(json.loads(r["payload"])) for r in rows]

    def update_status(self, tenant_id: str, status: str) -> None:
        t = self.get(tenant_id)
        if t is None:
            raise KeyError(tenant_id)
        t.status = status
        self._save(t)

    def update_quotas(self, tenant_id: str, quotas: TenantQuotas) -> None:
        t = self.get(tenant_id)
        if t is None:
            raise KeyError(tenant_id)
        t.quotas = quotas
        self._save(t)

    def _save(self, t: Tenant) -> None:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE tenants SET payload = ?, updated_at = datetime('now') "
                "WHERE tenant_id = ?",
                (json.dumps(t.to_payload()), t.tenant_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # API key CRUD
    # ------------------------------------------------------------------

    def issue_key(self, tenant_id: str) -> str:
        t = self.get(tenant_id)
        if t is None:
            raise KeyError(tenant_id)
        api_key = generate_api_key()
        digest = hash_key(api_key)
        t.api_key_hashes.append(digest)
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE tenants SET payload = ?, updated_at = datetime('now') "
                "WHERE tenant_id = ?",
                (json.dumps(t.to_payload()), tenant_id),
            )
            conn.execute(
                "INSERT INTO api_keys (key_hash, tenant_id) VALUES (?, ?)",
                (digest, tenant_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return api_key

    def revoke_key(self, tenant_id: str, key_hash: str) -> bool:
        t = self.get(tenant_id)
        if t is None:
            raise KeyError(tenant_id)
        if key_hash not in t.api_key_hashes:
            return False
        t.api_key_hashes.remove(key_hash)
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE tenants SET payload = ?, updated_at = datetime('now') "
                "WHERE tenant_id = ?",
                (json.dumps(t.to_payload()), tenant_id),
            )
            conn.execute(
                "DELETE FROM api_keys WHERE key_hash = ? AND tenant_id = ?",
                (key_hash, tenant_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return True

    def resolve_key(self, api_key: str) -> Tenant | None:
        digest = hash_key(api_key)
        row = self._conn().execute(
            "SELECT tenant_id FROM api_keys WHERE key_hash = ?", (digest,),
        ).fetchone()
        if row is None:
            return None
        t = self.get(row["tenant_id"])
        if t is None or t.status != "ACTIVE":
            return None
        # best-effort: update last_used_at
        with suppress(Exception):
            self._conn().execute(
                "UPDATE api_keys SET last_used_at = datetime('now') WHERE key_hash = ?",
                (digest,),
            )
        return t

    # ------------------------------------------------------------------
    # Bootstrapping
    # ------------------------------------------------------------------

    def ensure_default(self, *, legacy_api_key: str | None) -> Tenant:
        """Create the `_default` tenant on first boot.

        If `legacy_api_key` is provided, it's hashed and stored so existing
        deployments can keep their pre-tenancy API key working.
        """
        existing = self.get(DEFAULT_TENANT_ID)
        if existing is not None:
            if legacy_api_key:
                digest = hash_key(legacy_api_key)
                if digest not in existing.api_key_hashes:
                    existing.api_key_hashes.append(digest)
                    self._save(existing)
                    conn = self._conn()
                    conn.execute(
                        "INSERT OR IGNORE INTO api_keys (key_hash, tenant_id) "
                        "VALUES (?, ?)",
                        (digest, DEFAULT_TENANT_ID),
                    )
            return existing
        tenant = Tenant(
            tenant_id=DEFAULT_TENANT_ID,
            display_name="Default tenant",
            api_key_hashes=[],
            quotas=TenantQuotas(),
            created_at=_now_iso(),
        )
        if legacy_api_key:
            tenant.api_key_hashes.append(hash_key(legacy_api_key))
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO tenants (tenant_id, payload, updated_at) "
                "VALUES (?, ?, datetime('now'))",
                (tenant.tenant_id, json.dumps(tenant.to_payload())),
            )
            if legacy_api_key:
                conn.execute(
                    "INSERT INTO api_keys (key_hash, tenant_id) VALUES (?, ?)",
                    (hash_key(legacy_api_key), DEFAULT_TENANT_ID),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return tenant


# ----------------------------------------------------------------------
# Context propagation
# ----------------------------------------------------------------------

def set_current_tenant(tenant: Tenant | None) -> None:
    _current_tenant.set(tenant)


def get_current_tenant() -> Tenant | None:
    return _current_tenant.get()


def current_tenant_id() -> str:
    """Return the current tenant_id, or `_default` when no context is bound."""
    t = _current_tenant.get()
    return t.tenant_id if t is not None else DEFAULT_TENANT_ID


def require_tenant() -> Tenant:
    t = _current_tenant.get()
    if t is None:
        raise RuntimeError("no tenant context bound")
    return t


# ----------------------------------------------------------------------

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
