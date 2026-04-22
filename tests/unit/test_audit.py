"""Audit log unit tests."""

from __future__ import annotations

from pathlib import Path

from engram.audit import AuditLog


def test_audit_record_and_tail(tmp_path: Path):
    log = AuditLog(tmp_path / "audit.db")
    log.record(
        tenant_id="acme", actor="admin-key", action="tenant.create",
        target="acme", details={"display_name": "Acme"},
        request_id="req-1",
    )
    log.record(
        tenant_id="acme", actor="admin-key", action="tenant.keys.mint",
        target="acme", request_id="req-2",
    )
    log.record(
        tenant_id="other", actor="admin-key", action="tenant.create",
        target="other",
    )
    events = log.tail("acme")
    assert len(events) == 2
    actions = {e["action"] for e in events}
    assert actions == {"tenant.create", "tenant.keys.mint"}
    assert all(e["tenant_id"] == "acme" for e in events)


def test_audit_tenant_isolation(tmp_path: Path):
    log = AuditLog(tmp_path / "audit.db")
    log.record(tenant_id="a", actor="x", action="x.y", target="a")
    log.record(tenant_id="b", actor="x", action="x.y", target="b")
    assert [e["tenant_id"] for e in log.tail("a")] == ["a"]
    assert [e["tenant_id"] for e in log.tail("b")] == ["b"]


def test_audit_prune(tmp_path: Path):
    log = AuditLog(tmp_path / "audit.db")
    log.record(tenant_id="a", actor="x", action="x.y")
    # Force row into the past
    log._conn().execute(
        "UPDATE audit_log SET ts = datetime('now', '-400 days')"
    )
    removed = log.prune(retention_days=30)
    assert removed == 1
    assert log.tail("a") == []
