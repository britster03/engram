"""Pytest configuration — tenant context isolation between tests."""

from __future__ import annotations

import pytest

from engram.tenancy import set_current_tenant


@pytest.fixture(autouse=True)
def _reset_tenant_context():
    """Each test starts with no tenant bound; leaks between tests would be a bug."""
    set_current_tenant(None)
    yield
    set_current_tenant(None)
