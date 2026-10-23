"""Tenant registry, hashing, and context."""

from __future__ import annotations

import pytest

from engram.tenancy import (
    DEFAULT_TENANT_ID,
    TenantQuotas,
    TenantRegistry,
    current_tenant_id,
    generate_api_key,
    hash_key,
    set_current_tenant,
    validate_tenant_id,
)
from tests.postgres_support import PostgresTestStore


def test_generate_api_key_shape():
    k = generate_api_key()
    assert k.startswith("engram_")
    assert len(k) > 20


def test_revoked_legacy_key_stays_revoked_across_bootstrap_and_auth(monkeypatch):
    from types import SimpleNamespace

    import engram.deps
    from engram.api.auth import _lookup_tenant

    registry = TenantRegistry(PostgresTestStore())
    registry.ensure_default(legacy_api_key="bootstrap-key")
    assert registry.revoke_key("_default", hash_key("bootstrap-key"))
    registry.ensure_default(legacy_api_key="bootstrap-key")
    monkeypatch.setattr(engram.deps, "get_state", lambda: SimpleNamespace(tenant_registry=registry))
    assert _lookup_tenant("bootstrap-key") is None


def test_registry_outage_fails_closed(monkeypatch):
    from fastapi import HTTPException

    import engram.deps
    from engram.api.auth import _lookup_tenant

    def unavailable():
        raise ConnectionError("private database details")

    monkeypatch.setattr(engram.deps, "get_state", unavailable)
    with pytest.raises(HTTPException) as error:
        _lookup_tenant("configured-key")
    assert error.value.status_code == 503
    assert "private" not in error.value.detail


def test_hash_key_stable():
    assert hash_key("abc") == hash_key("abc")
    assert hash_key("abc") != hash_key("abd")


def test_validate_tenant_id_rules():
    validate_tenant_id("acme-corp")
    validate_tenant_id("_default")
    with pytest.raises(ValueError):
        validate_tenant_id("Acme-Corp")  # uppercase
    with pytest.raises(ValueError):
        validate_tenant_id("has space")
    with pytest.raises(ValueError):
        validate_tenant_id("")


def test_registry_create_and_resolve():
    reg = TenantRegistry(PostgresTestStore())
    t, key = reg.create(
        "acme",
        display_name="Acme",
        quotas=TenantQuotas(
            requests_per_minute=5,
            ingest_per_minute=10,
        ),
    )
    assert t.tenant_id == "acme"
    assert t.quotas.requests_per_minute == 5
    assert reg.resolve_key(key) is not None
    assert reg.resolve_key(key).tenant_id == "acme"
    assert reg.resolve_key("engram_wrong") is None


def test_registry_rejects_duplicate():
    reg = TenantRegistry(PostgresTestStore())
    reg.create("acme")
    with pytest.raises(ValueError):
        reg.create("acme")


def test_registry_issue_and_revoke():
    reg = TenantRegistry(PostgresTestStore())
    reg.create("acme")
    k2 = reg.issue_key("acme")
    assert reg.resolve_key(k2).tenant_id == "acme"
    revoked = reg.revoke_key("acme", hash_key(k2))
    assert revoked is True
    assert reg.resolve_key(k2) is None


def test_registry_ensure_default_idempotent():
    reg = TenantRegistry(PostgresTestStore())
    t1 = reg.ensure_default(legacy_api_key="legacy-key")
    t2 = reg.ensure_default(legacy_api_key="legacy-key")
    assert t1.tenant_id == DEFAULT_TENANT_ID
    assert t2.tenant_id == DEFAULT_TENANT_ID
    assert reg.resolve_key("legacy-key").tenant_id == DEFAULT_TENANT_ID


def test_registry_suspend_gates_resolve():
    reg = TenantRegistry(PostgresTestStore())
    _, key = reg.create("acme")
    reg.update_status("acme", "SUSPENDED")
    assert reg.resolve_key(key) is None
    reg.update_status("acme", "ACTIVE")
    assert reg.resolve_key(key) is not None


def test_context_default_and_override():
    assert current_tenant_id() == DEFAULT_TENANT_ID
    from engram.tenancy import Tenant

    set_current_tenant(
        Tenant(
            tenant_id="acme",
            display_name="",
            api_key_hashes=[],
            quotas=TenantQuotas(),
            status="ACTIVE",
        )
    )
    assert current_tenant_id() == "acme"
    set_current_tenant(None)
    assert current_tenant_id() == DEFAULT_TENANT_ID
