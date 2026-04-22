"""Session cache — tenant-namespaced keys prevent cross-tenant collision."""

from __future__ import annotations

from engram.config import SessionCacheConfig
from engram.storage.redis_cache import SessionCache
from engram.tenancy import Tenant, TenantQuotas, set_current_tenant


def _mem_cache() -> SessionCache:
    return SessionCache(
        SessionCacheConfig(backend="memory"), default_ttl_seconds=60,
    )


def _tenant(tid: str) -> Tenant:
    return Tenant(
        tenant_id=tid, display_name=tid, api_key_hashes=[],
        quotas=TenantQuotas(), status="ACTIVE",
    )


def test_keys_are_tenant_namespaced():
    cache = _mem_cache()
    assert cache._key("sess-1", tenant_id="acme") == "session:acme:sess-1"
    assert cache._key("sess-1", tenant_id="globex") == "session:globex:sess-1"


def test_same_session_id_different_tenants_do_not_collide():
    cache = _mem_cache()
    cache.set("s1", {"tid": "acme", "n": 1}, tenant_id="acme")
    cache.set("s1", {"tid": "globex", "n": 2}, tenant_id="globex")
    a = cache.get("s1", tenant_id="acme")
    b = cache.get("s1", tenant_id="globex")
    assert a is not None and a["tid"] == "acme"
    assert b is not None and b["tid"] == "globex"


def test_context_var_is_used_when_no_tenant_id_passed():
    cache = _mem_cache()
    set_current_tenant(_tenant("foo"))
    cache.set("s1", {"who": "foo"})
    assert cache.get("s1") == {"who": "foo"}
    set_current_tenant(_tenant("bar"))
    assert cache.get("s1") is None
    set_current_tenant(None)


def test_delete_respects_tenant_scope():
    cache = _mem_cache()
    cache.set("s1", {"n": 1}, tenant_id="a")
    cache.set("s1", {"n": 2}, tenant_id="b")
    cache.delete("s1", tenant_id="a")
    assert cache.get("s1", tenant_id="a") is None
    assert cache.get("s1", tenant_id="b") is not None
