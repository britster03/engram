"""Bearer-token authentication with tenant resolution (§11.4, §12.1).

Flow:
  1. Client sends `Authorization: Bearer <key>`.
  2. Middleware hashes the key, looks it up in the TenantRegistry.
  3. The resolved `Tenant` is bound to the request's context var.
  4. Handlers can call `engram.tenancy.require_tenant()` to access it.

For backward compatibility, single-tenant deployments can keep using the
legacy `api.api_key` config value: the TenantRegistry's `ensure_default()`
adds it to the `_default` tenant at boot. Multi-tenant deployments create
additional tenants via `POST /api/v1/admin/tenants` (admin key required).

An **admin key** authenticates the `/admin/*` surface. Configure via
`ENGRAM_ADMIN_KEY` or `api.admin_key` in config.yaml. If unset, the admin
API is disabled — the only way to create tenants is via the CLI.
"""

from __future__ import annotations

import os

from fastapi import Depends, Header, HTTPException, status

from engram.config import get_config
from engram.tenancy import DEFAULT_TENANT_ID, set_current_tenant


def _lookup_tenant(api_key: str):
    """Resolve a bearer token. Tries the TenantRegistry first, then the
    legacy single-key path so existing deployments keep working until they
    cut over to tenants."""
    from engram.deps import get_state
    try:
        state = get_state()
    except Exception:
        state = None

    if state is not None:
        try:
            t = state.tenant_registry.resolve_key(api_key)
            if t is not None:
                return t
        except Exception:
            pass
    # Legacy single-tenant key
    cfg = get_config()
    if cfg.api.api_key and api_key == cfg.api.api_key:
        from engram.tenancy import Tenant, TenantQuotas
        return Tenant(
            tenant_id=DEFAULT_TENANT_ID,
            display_name="Default tenant (legacy key)",
            api_key_hashes=[],
            quotas=TenantQuotas(
                requests_per_minute=cfg.api.rate_limit_query_per_minute,
                ingest_per_minute=cfg.api.rate_limit_ingest_per_minute,
            ),
            status="ACTIVE",
        )
    return None


def _extract_bearer(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return authorization.split(" ", 1)[1].strip()


async def require_tenant_auth(authorization: str | None = Header(default=None)) -> None:
    """Dependency: resolve tenant or 401.

    Async on purpose: a sync dependency runs in a threadpool worker, and the
    tenant ContextVar it sets there does NOT propagate to the (also
    threadpooled) sync endpoint — so every request fell back to `_default`.
    An async dependency runs in the request's main context, which the endpoint
    inherits, so `current_tenant_id()` sees the resolved tenant.
    """
    token = _extract_bearer(authorization)
    tenant = _lookup_tenant(token)
    if tenant is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid api key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    set_current_tenant(tenant)


def require_admin(authorization: str | None = Header(default=None)) -> None:
    """Dependency: admin-only endpoints require the admin key."""
    token = _extract_bearer(authorization)
    cfg = get_config()
    admin_key = getattr(cfg.api, "admin_key", None) or os.environ.get("ENGRAM_ADMIN_KEY")
    if not admin_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="admin api is disabled; set ENGRAM_ADMIN_KEY to enable",
        )
    if token != admin_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid admin key",
            headers={"WWW-Authenticate": "Bearer"},
        )


# Back-compat alias — old code imports `require_bearer` / `AuthDep`.
require_bearer = require_tenant_auth
AuthDep = Depends(require_tenant_auth)
AdminDep = Depends(require_admin)
