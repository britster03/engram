"""Provider wrappers must not hide recoverable failures from bookkeeping."""

import httpx
import openai
import pytest
from temporalio.api.failure.v1 import Failure
from temporalio.converter import DefaultFailureConverter, DefaultPayloadConverter
from temporalio.exceptions import ApplicationError

from engram.models.core import CoreModelError
from engram.temporal.workflows import ingestion_failure_summary


def round_trip(error):
    failure = Failure()
    converter = DefaultFailureConverter()
    payload = DefaultPayloadConverter()
    converter.to_failure(error, payload, failure)
    return converter.from_failure(failure, payload)


@pytest.mark.parametrize(
    "kind,status", [(openai.RateLimitError, 429), (openai.InternalServerError, 503)]
)
def test_wrapped_provider_failure_preserves_type_after_temporal_serialization(kind, status):
    response = httpx.Response(
        status, request=httpx.Request("POST", "https://model.invalid/responses")
    )
    wrapped = CoreModelError("openai-responses API error: provider failed")
    wrapped.__cause__ = kind("provider failed", response=response, body=None)
    summary = ingestion_failure_summary(round_trip(wrapped))
    assert summary.startswith(kind.__name__ + ":")
    assert len(summary) <= 500


def test_timeout_wrapper_is_recoverable_but_authentication_and_parse_failures_are_not():
    request = httpx.Request("POST", "https://model.invalid/responses")
    wrapped = CoreModelError("wrapped timeout")
    wrapped.__cause__ = openai.APITimeoutError(request=request)
    assert ingestion_failure_summary(round_trip(wrapped)).startswith("APITimeoutError:")
    wrapped.__cause__ = openai.AuthenticationError(
        "authentication failed", response=httpx.Response(401, request=request), body=None
    )
    assert ingestion_failure_summary(round_trip(wrapped)).startswith("CoreModelError:")
    wrapped.__cause__ = ValueError("invalid JSON")
    assert ingestion_failure_summary(round_trip(wrapped)).startswith("CoreModelError:")


def test_non_retryable_validation_wrapper_does_not_become_provider_recovery():
    permanent = ApplicationError(
        "invalid source quote", type="UngroundedExtraction", non_retryable=True
    )
    permanent.__cause__ = ApplicationError("prior timeout", type="APITimeoutError")
    assert ingestion_failure_summary(permanent).startswith("UngroundedExtraction:")


def test_unknown_or_cyclic_causes_remain_bounded_and_non_recoverable():
    error = RuntimeError("unknown " * 200)
    error.__cause__ = error
    assert len(ingestion_failure_summary(error)) == 500
    assert ingestion_failure_summary(None) == "None"


@pytest.mark.parametrize("patched", [False, True])
@pytest.mark.parametrize("canonical", [False, True])
def test_failed_ingestion_bookkeeping_keeps_replay_compatibility(monkeypatch, patched, canonical):
    import asyncio

    from temporalio.exceptions import ActivityError

    from engram.temporal import workflows

    provider = ApplicationError("rate limit", type="RateLimitError")
    wrapped = ApplicationError("provider failed", type="CoreModelError")
    wrapped.__cause__ = provider
    failure = ActivityError(
        "activity failed",
        scheduled_event_id=1,
        started_event_id=2,
        identity="test",
        activity_type="test",
        activity_id="test",
        retry_state=None,
    )
    failure.__cause__ = wrapped
    calls = []

    async def execute_activity(name, *args, **kwargs):
        if name in {"engram.process_ingest", "engram.canonical_gate"}:
            raise failure
        calls.append((name, kwargs["args"]))

    def patch(identity):
        assert identity in {"provider-ingestion-failure-cause-v1", "durable-terminal-bookkeeping-v1"}
        return patched

    monkeypatch.setattr(workflows.workflow, "execute_activity", execute_activity)
    monkeypatch.setattr(workflows.workflow, "patched", patch)
    run = (
        workflows.CanonicalIngestWorkflow().run("event", "tenant")
        if canonical
        else workflows.IngestWorkflow().run("event")
    )
    with pytest.raises(ApplicationError):
        asyncio.run(run)
    assert len(calls) == 1
    assert calls[0][1][-1].startswith("RateLimitError:" if patched else "CoreModelError:")
    assert calls[0][1][0] == "event"
    if canonical:
        assert calls[0][1][1] == "tenant"
