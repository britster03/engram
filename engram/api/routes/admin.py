"""Admin API — tenant CRUD and key rotation.

Protected by the admin key (`ENGRAM_ADMIN_KEY` env / `api.admin_key` config).
If no admin key is configured the entire surface returns 404 so the
endpoints don't leak even the fact that they exist.

Endpoints:
  POST   /api/v1/admin/tenants                      — create a tenant
  GET    /api/v1/admin/tenants                      — list tenants
  GET    /api/v1/admin/tenants/{id}                 — get a tenant
  POST   /api/v1/admin/tenants/{id}/keys            — mint a new API key
  DELETE /api/v1/admin/tenants/{id}/keys/{hash}     — revoke a key
  PATCH  /api/v1/admin/tenants/{id}/quotas          — update quotas
  POST   /api/v1/admin/tenants/{id}/suspend         — SUSPENDED
  POST   /api/v1/admin/tenants/{id}/resume          — ACTIVE
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from engram.api.auth import AdminDep
from engram.deps import get_state
from engram.tenancy import TenantQuotas, validate_tenant_id

router = APIRouter(
    prefix="/api/v1/admin",
    tags=["admin"],
    dependencies=[AdminDep],
)


class CreateTenantRequest(BaseModel):
    tenant_id: str = Field(..., max_length=64)
    display_name: str = Field(default="", max_length=128)
    quotas: dict[str, int] | None = None


class TenantPayload(BaseModel):
    tenant_id: str
    display_name: str
    status: str
    created_at: str
    quotas: dict[str, int]
    api_key_count: int


class CreateTenantResponse(TenantPayload):
    api_key: str   # returned exactly once — caller must persist it


class MintKeyResponse(BaseModel):
    tenant_id: str
    api_key: str   # returned exactly once


def _payload(t) -> TenantPayload:
    return TenantPayload(
        tenant_id=t.tenant_id,
        display_name=t.display_name,
        status=t.status,
        created_at=t.created_at,
        quotas={
            "requests_per_minute": t.quotas.requests_per_minute,
            "ingest_per_minute": t.quotas.ingest_per_minute,
            "max_memories": t.quotas.max_memories,
            "max_monthly_tokens": t.quotas.max_monthly_tokens,
            "max_consolidation_backlog": t.quotas.max_consolidation_backlog,
        },
        api_key_count=len(t.api_key_hashes),
    )


@router.post("/tenants", response_model=CreateTenantResponse, status_code=201)
def create_tenant(req: CreateTenantRequest) -> CreateTenantResponse:
    validate_tenant_id(req.tenant_id)
    state = get_state()
    quotas = TenantQuotas(**(req.quotas or {}))
    try:
        tenant, api_key = state.tenant_registry.create(
            req.tenant_id, display_name=req.display_name, quotas=quotas,
        )
    except ValueError as err:
        raise HTTPException(status_code=409, detail=str(err)) from err
    base = _payload(tenant)
    return CreateTenantResponse(**base.model_dump(), api_key=api_key)


@router.get("/tenants", response_model=list[TenantPayload])
def list_tenants() -> list[TenantPayload]:
    state = get_state()
    return [_payload(t) for t in state.tenant_registry.list()]


@router.get("/tenants/{tenant_id}", response_model=TenantPayload)
def get_tenant(tenant_id: str) -> TenantPayload:
    state = get_state()
    t = state.tenant_registry.get(tenant_id)
    if t is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    return _payload(t)


@router.post("/tenants/{tenant_id}/keys", response_model=MintKeyResponse)
def mint_key(tenant_id: str) -> MintKeyResponse:
    state = get_state()
    try:
        api_key = state.tenant_registry.issue_key(tenant_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="tenant not found")
    return MintKeyResponse(tenant_id=tenant_id, api_key=api_key)


@router.delete("/tenants/{tenant_id}/keys/{key_hash}")
def revoke_key(tenant_id: str, key_hash: str) -> dict[str, Any]:
    state = get_state()
    try:
        revoked = state.tenant_registry.revoke_key(tenant_id, key_hash)
    except KeyError:
        raise HTTPException(status_code=404, detail="tenant not found")
    if not revoked:
        raise HTTPException(status_code=404, detail="key_hash not found for tenant")
    return {"tenant_id": tenant_id, "revoked": key_hash}


@router.patch("/tenants/{tenant_id}/quotas", response_model=TenantPayload)
def update_quotas(tenant_id: str, quotas: dict[str, int]) -> TenantPayload:
    state = get_state()
    try:
        current = state.tenant_registry.get(tenant_id)
        if current is None:
            raise KeyError(tenant_id)
        updated = TenantQuotas(**{**current.quotas.__dict__, **quotas})
        state.tenant_registry.update_quotas(tenant_id, updated)
    except KeyError:
        raise HTTPException(status_code=404, detail="tenant not found")
    t = state.tenant_registry.get(tenant_id)
    return _payload(t)


@router.post("/tenants/{tenant_id}/suspend", response_model=TenantPayload)
def suspend(tenant_id: str) -> TenantPayload:
    state = get_state()
    try:
        state.tenant_registry.update_status(tenant_id, "SUSPENDED")
    except KeyError:
        raise HTTPException(status_code=404, detail="tenant not found")
    return _payload(state.tenant_registry.get(tenant_id))


@router.post("/tenants/{tenant_id}/resume", response_model=TenantPayload)
def resume(tenant_id: str) -> TenantPayload:
    state = get_state()
    try:
        state.tenant_registry.update_status(tenant_id, "ACTIVE")
    except KeyError:
        raise HTTPException(status_code=404, detail="tenant not found")
    return _payload(state.tenant_registry.get(tenant_id))
