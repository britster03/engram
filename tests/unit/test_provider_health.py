from types import SimpleNamespace

import pytest

from engram.models import budget


def test_observed_health_is_endpoint_model_scoped_and_invalidated_by_failure(monkeypatch):
    monkeypatch.setattr(budget, "_health", {})
    provider = SimpleNamespace(
        _client=SimpleNamespace(base_url="https://model.test"),
        cfg=SimpleNamespace(model_path="one"),
    )
    assert budget.provider_recovered_at(provider) is None
    budget.call_provider(
        provider._client, lambda **kwargs: SimpleNamespace(usage=None), {"model": "one"}
    )
    assert budget.provider_recovered_at(provider) is not None
    provider.cfg.model_path = "two"
    assert budget.provider_recovered_at(provider) is None
    provider.cfg.model_path = "one"

    def broken(**kwargs):
        raise TimeoutError("unavailable")

    with pytest.raises(TimeoutError):
        budget.call_provider(provider._client, broken, {"model": "one"})
    assert budget.provider_recovered_at(provider) is None


def test_late_older_success_cannot_hide_a_newer_failure(monkeypatch):
    monkeypatch.setattr(budget, "_health", {})
    provider = SimpleNamespace(
        _client=SimpleNamespace(base_url="https://model.test"),
        cfg=SimpleNamespace(model_path="one"),
    )

    def outer(**kwargs):
        def failed(**kwargs):
            raise TimeoutError("newer call failed")

        with pytest.raises(TimeoutError):
            budget.call_provider(provider._client, failed, {"model": "one"})
        return SimpleNamespace(usage=None)

    budget.call_provider(provider._client, outer, {"model": "one"})
    assert budget.provider_recovered_at(provider) is None
