"""Pass-through instrumentation installed only in benchmark processes."""
from __future__ import annotations

import contextvars
import functools
import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from .ledger import append, known_provider_count, known_provider_tokens, safe

CURRENT = contextvars.ContextVar("lifecycle_trace", default=None)
OPERATION = contextvars.ContextVar("lifecycle_operation", default=None)
_INSTALLED = False


def tokens(text):
    from engram.tokens import count_tokens
    return count_tokens(str(text or ""))


def error_info(error):
    import traceback
    diag=getattr(error,"diag",None)
    return {"type": type(error).__name__, "status_code": getattr(error, "status_code", None),"table":getattr(diag,"table_name",None),"constraint":getattr(diag,"constraint_name",None), "frames": [{"file": Path(f.filename).name, "line": f.lineno, "function": f.name} for f in traceback.extract_tb(error.__traceback__)[-8:]]}


@dataclass
class Trace:
    identity: str
    component: str = "query"
    stage: str | None = None
    evaluation_id: str | None = None
    qa_id: str | None = None
    calls: list = field(default_factory=list)
    events: list = field(default_factory=list)
    executed: list = field(default_factory=list)
    started: float = field(default_factory=time.perf_counter)
    lock: object = field(default_factory=threading.RLock, repr=False)

    def emit(self, kind, **values):
        event = {"kind": kind, "stage": self.stage, "elapsed_ms": (time.perf_counter() - self.started) * 1000, **safe(values)}
        with self.lock:
            self.events.append(event)
        return event

    def step(self, metadata):
        step = metadata.get("step", "")
        if step.endswith("_start"):
            self.stage = step[:-6].upper()
        self.emit("cascade", metadata=metadata)

    def to_dict(self):
        return {"trace_id": self.identity, "evaluation_id": self.evaluation_id,
                "qa_id": self.qa_id, "component": self.component, "events": self.events,
                "calls": self.calls, "executed_levels": list(dict.fromkeys(self.executed)),
                "total_ms": (time.perf_counter() - self.started) * 1000}


@contextmanager
def capture(trace):
    token = CURRENT.set(trace)
    try:
        yield trace
    finally:
        CURRENT.reset(token)


def background():
    try:
        from temporalio import activity
        info = activity.info()
        return {"workflow_id": info.workflow_id, "activity_id": info.activity_id, "activity_type": info.activity_type, "activity_attempt": info.attempt}
    except RuntimeError:
        return {"activity_type": "session_commit"}


def usage(response, request):
    raw = getattr(response, "usage", None)
    raw = safe(raw) if raw is not None else {}
    incoming = raw.get("input_tokens", raw.get("prompt_tokens"))
    outgoing = raw.get("output_tokens", raw.get("completion_tokens"))
    input_details = raw.get("input_tokens_details", raw.get("prompt_tokens_details")) or {}
    output_details = raw.get("output_tokens_details", raw.get("completion_tokens_details")) or {}
    text = getattr(response, "output_text", None)
    if text is None and getattr(response, "choices", None):
        text = response.choices[0].message.content or ""
    if text is None:
        text = "".join(getattr(part, "text", "") for item in (getattr(response, "output", None) or []) for part in (getattr(item, "content", None) or []))
    estimates = incoming is None or outgoing is None
    return {"provider_input_tokens":incoming,"provider_output_tokens":outgoing,"cache_write_input_tokens":input_details.get("cache_write_tokens"),"input_tokens": incoming if incoming is not None else tokens(request.get("instructions", "") + str(request.get("input", request.get("messages", "")))), "output_tokens": outgoing if outgoing is not None else tokens(text), "reasoning_tokens": output_details.get("reasoning_tokens"), "cached_input_tokens": input_details.get("cached_tokens"), "token_count_source": "tokenizer_estimate" if estimates else "provider", "raw_usage": raw, "response_id": getattr(response, "id", None), "response_text": text}


def _journal(value):
    directory = os.environ.get("LOCOMO_TRACE_DIR")
    if directory:
        append(Path(directory) / f"provider_attempts.{os.getpid()}.jsonl", value)


def _sdk(original, provider, sdk_api):
    @functools.wraps(original)
    def wrapped(*args, **kwargs):
        trace = CURRENT.get()
        if trace is None:
            return original(*args, **kwargs)
        # The SDK request body is evidence. Preserve all body parameters, while
        # excluding transport options and credentials (including nested values).
        transport = {"extra_headers", "headers", "timeout", "api_key", "base_url",
                     "http_client", "extra_query"}
        request = safe({k: v for k, v in kwargs.items() if k not in transport})
        attempt = {"attempt_id": uuid.uuid4().hex, "trace_id": trace.identity,
                   "evaluation_id": trace.evaluation_id, "qa_id": trace.qa_id,
                   "component": trace.component, "level": trace.stage,
                   "provider": provider.cfg.provider,
                   "provider_role": provider._lifecycle_role, "sdk_api": sdk_api,
                   "model": request.get("model"), "request": request,
                   **(OPERATION.get() or {})}
        if "instructions" in request:
            attempt["prompt_parts"] = {**attempt.get("prompt_parts", {}), "system_prompt":tokens(request["instructions"])}
        _journal({**attempt, "state": "STARTED", "time_ns": time.time_ns()})
        started = time.perf_counter()
        try:
            response = original(*args, **kwargs)
            attempt.update(usage(response, request), status="RETURNED")
            return response
        except BaseException as error:
            attempt.update(status="FAILED", error=error_info(error), input_tokens=None, output_tokens=None, reasoning_tokens=None, cached_input_tokens=None, token_count_source="unavailable")
            raise
        finally:
            attempt["latency_ms"] = (time.perf_counter() - started) * 1000
            with trace.lock:
                trace.calls.append(attempt)
            _journal({**attempt, "state": "FINISHED", "time_ns": time.time_ns()})
    return wrapped


def instrument_provider(provider, role=None):
    if getattr(provider, "_lifecycle_observed", False):
        return provider
    provider._lifecycle_role = role or ("frontier" if hasattr(provider, "answer") else "core")
    client = getattr(provider, "_client", None)
    if client is None:
        raise RuntimeError("Provider attempt observation is unsupported for this provider")
    for api, resource in [("responses", getattr(client, "responses", None)),
                          ("chat.completions", getattr(getattr(client, "chat", None), "completions", None))]:
        if resource is not None:
            resource.create = _sdk(resource.create, provider, api)
    for name in ("complete", "answer", "plan_retrieval"):
        original = getattr(provider, name, None)
        if original is None:
            continue
        def make(method, method_name):
            @functools.wraps(method)
            def wrapped(*args, **kwargs):
                existing = CURRENT.get()
                info = background() if existing is None else {}
                trace = existing or Trace(f"background:{uuid.uuid4().hex}", component="ingestion")
                phase = "tool_planning" if method_name == "plan_retrieval" else "answerability" if method_name == "answer" and trace.stage in {"L0", "L1", "L2"} else "generation" if method_name == "answer" else info.get("activity_type", trace.stage or "core")
                from engram.models.budget import current_model_phase
                phase = current_model_phase() or phase
                operation = {"operation_id": uuid.uuid4().hex, "phase": phase,
                             "method": method_name, "provider_role": provider._lifecycle_role,
                             **info, "prompt_parts": {k: tokens(v) for k, v in kwargs.items() if k in {"system_prompt", "msc", "user_query", "user_prompt"}}}
                with capture(trace):
                    token = OPERATION.set(operation)
                    started = time.perf_counter()
                    if method_name == "answer":
                        trace.emit("model_context", text=kwargs.get("msc", ""), phase=phase, operation_id=operation["operation_id"])
                        if trace.stage == "L0":
                            trace.executed.append("L0")
                    try:
                        result = method(*args, **kwargs)
                        trace.emit("provider_result", method=method_name, result=result, phase=phase, operation_id=operation["operation_id"], latency_ms=(time.perf_counter() - started) * 1000)
                        return result
                    except BaseException as error:
                        trace.emit("provider_error", method=method_name, phase=phase, error=error_info(error), operation_id=operation["operation_id"], latency_ms=(time.perf_counter() - started) * 1000)
                        raise
                    finally:
                        OPERATION.reset(token)
                        if existing is None and os.environ.get("LOCOMO_TRACE_DIR"):
                            append(Path(os.environ["LOCOMO_TRACE_DIR"]) / f"ingestion_trace.{os.getpid()}.jsonl", trace.to_dict())
            return wrapped
        setattr(provider, name, make(original, name))
    provider._lifecycle_observed = True
    return provider


def summarize_usage(calls):
    unknown = sum(c.get("input_tokens") is None or c.get("output_tokens") is None for c in calls)
    incoming = sum(c.get("input_tokens") or 0 for c in calls)
    outgoing = sum(c.get("output_tokens") or 0 for c in calls)
    def details(key):
        return sum(c.get(key) or 0 for c in calls) if not calls or all(c.get(key) is not None for c in calls) else None
    result = {"total_input_tokens": None if unknown else incoming, "total_output_tokens": None if unknown else outgoing, "total_tokens": None if unknown else incoming + outgoing, "known_input_tokens":sum(known_provider_count(c,"input") for c in calls), "known_output_tokens":sum(known_provider_count(c,"output") for c in calls), "known_total_tokens":sum(known_provider_tokens(c) for c in calls),"estimated_total_tokens":sum((c.get("input_tokens") or 0)+(c.get("output_tokens") or 0)-known_provider_tokens(c) for c in calls),"estimated_usage_calls":sum(c.get("token_count_source")=="tokenizer_estimate" for c in calls),"cache_write_input_tokens":details("cache_write_input_tokens"), "unknown_usage_calls": unknown, "total_reasoning_tokens": details("reasoning_tokens"), "cached_input_tokens": details("cached_input_tokens"), "provider_attempts": len(calls), "retry_count": len(calls) - len({c["operation_id"] for c in calls}), "token_count_source": "unavailable_partial" if unknown else "not_used" if not calls else "provider" if all(c["token_count_source"] == "provider" for c in calls) else "provider_and_estimate", "system_prompt_tokens": sum(c.get("prompt_parts", {}).get("system_prompt", 0) for c in calls), "query_rewrite_input_tokens": 0, "query_rewrite_output_tokens": 0}
    for prefix, phase in [("router", "tool_planning"), ("generation", "generation"), ("answerability", "answerability"), ("sufficiency", "sufficiency"), ("output_support", "output_support"), ("decomposition", "decomposition")]:
        subset = [c for c in calls if c.get("phase") == phase]
        for direction in ("input", "output"):
            result[f"{prefix}_{direction}_tokens"] = None if any(c.get(f"{direction}_tokens") is None for c in subset) else sum(c.get(f"{direction}_tokens") or 0 for c in subset)
    return result


def observe_function(owner, name, kind, *, instance=False):
    original = getattr(owner, name)
    @functools.wraps(original)
    def wrapped(*args, **kwargs):
        trace = CURRENT.get()
        if trace is None:
            return original(*args, **kwargs)
        started = time.perf_counter()
        inputs = list(args[1:] if instance else args)
        try:
            result = original(*args, **kwargs)
            trace.emit(kind, function=name, inputs=inputs, arguments=kwargs, result=result, latency_ms=(time.perf_counter() - started) * 1000)
            return result
        except BaseException as error:
            trace.emit(kind, function=name, error=error_info(error), latency_ms=(time.perf_counter() - started) * 1000)
            raise
    setattr(owner, name, wrapped)


def install():
    global _INSTALLED
    if _INSTALLED:
        return
    from engram import deps
    original = deps.build_providers
    def build(*args, **kwargs):
        return tuple(instrument_provider(p, role) for p, role in
                     zip(original(*args, **kwargs), ("core", "frontier"), strict=True))
    deps.build_providers = build
    from engram.retrieval.tools import MemoryTools
    stages = {"current_session_search": "L1", "current_session_originals": "L1", "past_session_search": "L2", "facts_search": "L3_FACTS", "semantic_memory_search": "L3_VECTOR", "graph_search": "L3_GRAPH"}
    for name, stage in stages.items():
        original_tool = getattr(MemoryTools, name)
        def make(method, method_name, level):
            @functools.wraps(method)
            def wrapped(self, *args, **kwargs):
                trace = CURRENT.get()
                started = time.perf_counter()
                if trace is not None:
                    trace.executed.append(level)
                try:
                    result = method(self, *args, **kwargs)
                    if trace is not None:
                        trace.emit("tool", function=method_name, level=level, inputs=args, arguments=kwargs, result=result, diagnostics=self.diagnostics, latency_ms=(time.perf_counter() - started) * 1000)
                    return result
                except BaseException as error:
                    if trace is not None:
                        trace.emit("tool", function=method_name, level=level, error=error_info(error), latency_ms=(time.perf_counter() - started) * 1000)
                    raise
            return wrapped
        setattr(MemoryTools, name, make(original_tool, name, stage))
    from engram.retrieval import hybrid, orchestrator, sufficiency
    from engram.retrieval.context import ContextBuilder
    from engram.retrieval.evidence import CanonicalVerifier
    from engram.retrieval.planner import AdaptivePlanner
    observe_function(sufficiency, "pack_items", "quality_packing")
    # Rejected text is intentionally not logged through the generic wrapper.
    original_rejection = CanonicalVerifier.rejection
    def rejection(self, candidate, reason):
        result = original_rejection(self, candidate, reason)
        trace = CURRENT.get()
        if trace is not None:
            trace.emit("verification_rejection", result=result)
        return result
    CanonicalVerifier.rejection = rejection
    for owner, name, kind, instance in [(AdaptivePlanner, "plan", "planning", True), (CanonicalVerifier, "verify", "verification", True), (ContextBuilder, "pack_evidence", "packing", True), (ContextBuilder, "build", "context", True), (hybrid, "reciprocal_rank_fusion", "fusion", False), (orchestrator, "_rank_profile_evidence", "ranking", False), (orchestrator, "_rank_and_limit_semantic_evidence", "ranking", False)]:
        observe_function(owner, name, kind, instance=instance)
    from engram.cache import EmbeddingCache
    from engram.models.embeddings import EmbeddingService
    for name in ("get", "set"):
        observe_function(EmbeddingCache, name, "embedding_cache", instance=True)
    for name in ("embed", "embed_batch"):
        method = getattr(EmbeddingService, name)
        def embedding_wrapper(original, method_name):
            @functools.wraps(original)
            def wrapped(self, *args, **kwargs):
                existing = CURRENT.get()
                trace = existing or Trace(f"embedding:{uuid.uuid4().hex}", component="ingestion")
                with capture(trace):
                    start = time.perf_counter()
                    try:
                        result = original(self, *args, **kwargs)
                        trace.emit("embedding", function=method_name, texts=args[0], count=1 if method_name == "embed" else len(args[0]), dimensions=self.dim, model=self.cfg.embedding_model_path, latency_ms=(time.perf_counter()-start)*1000, background=background() if existing is None else None)
                        return result
                    finally:
                        if existing is None and os.environ.get("LOCOMO_TRACE_DIR"):
                            append(Path(os.environ["LOCOMO_TRACE_DIR"])/f"embedding_trace.{os.getpid()}.jsonl", trace.to_dict())
            return wrapped
        setattr(EmbeddingService, name, embedding_wrapper(method, name))
    _INSTALLED = True
