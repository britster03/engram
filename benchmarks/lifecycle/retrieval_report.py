"""Readable store reads and exact frontier inputs for one benchmark question."""
from __future__ import annotations

from .ledger import digest, safe


_PREVIEW_ITEMS = 10


_FIELDS = {
    "abstract", "alias", "answer", "asserted_at", "body", "blip_caption",
    "canonical_name", "canonical_uri", "caption", "compacted", "confidence",
    "content", "created_at", "date_time", "description", "event_time",
    "key_entities", "key_facts", "l0_abstract", "l1_summary", "label",
    "memory_type", "name", "node_type", "object_entity_name", "object_type",
    "object_value", "overview", "predicate", "projected_revision", "relation",
    "revision", "role", "score", "source_span", "speaker", "status",
    "summary", "text", "timestamp", "turn_idx", "type", "updated_at",
    "valid_from", "valid_until", "value", "version_number",
}


def _readable(value):
    """Keep source text, identity, and ranking in a bounded CSV preview."""
    if isinstance(value, dict):
        result = {}
        for key, part in value.items():
            if key == "tenant_id":
                continue
            if key in _FIELDS or key == "id" or key.endswith(("_id", "_uri")):
                result[key] = _readable(part) if isinstance(part, (dict, list)) else safe(part)
            elif isinstance(part, (dict, list)):
                nested = _readable(part)
                if nested:
                    result[key] = nested
        return result
    if isinstance(value, list):
        preview = [_readable(part) for part in value[:_PREVIEW_ITEMS]]
        return {"total_count": len(value), "preview": preview} if len(value) > _PREVIEW_ITEMS else preview
    return safe(value)


def question_retrieval(trace, *, answer_mode=None):
    """Distinguish database returns from the context actually sent to frontier."""
    sources = {"postgresql": [], "neo4j": [], "redis": []}
    labels = {
        "postgres_conversations": "postgresql",
        "postgres_canonical": "postgresql",
        "neo4j": "neo4j",
        "redis_sessions": "redis",
    }
    groups = {}
    result_hashes = {}
    preview_hashes = {}
    for event in trace.events:
        if event.get("kind") != "store" or event.get("store") not in labels:
            continue
        result = event.get("result")
        items = result if isinstance(result, list) else [result] if result is not None else []
        result_hash = digest(result) if "result" in event else None
        key = (event.get("stage"), event["store"], event.get("function"))
        if key not in groups:
            record = {
                "stage": event.get("stage"),
                "store": event["store"],
                "operation": event.get("function"),
                "read_count": 0,
                "returned_count": None,
                "distinct_result_count": 0,
                "items": [],
                "omitted_count": 0,
                "sample_result_sha256": None,
                "error_count": 0,
                "error": None,
            }
            sources[labels[event["store"]]].append(record)
            groups[key] = record
            result_hashes[key] = set()
            preview_hashes[key] = set()
        record = groups[key]
        record["read_count"] += 1
        if "result" in event:
            record["returned_count"] = (record["returned_count"] or 0) + len(items)
            result_hashes[key].add(result_hash)
            record["distinct_result_count"] = len(result_hashes[key])
            if record["sample_result_sha256"] is None:
                record["sample_result_sha256"] = result_hash
            for item in items:
                if len(record["items"]) >= _PREVIEW_ITEMS:
                    break
                readable = _readable(item)
                preview_hash = digest(readable)
                if preview_hash not in preview_hashes[key]:
                    record["items"].append(readable)
                    preview_hashes[key].add(preview_hash)
            record["omitted_count"] = max(0, record["returned_count"] - len(record["items"]))
        else:
            record["error_count"] += 1
            if record["error"] is None:
                record["error"] = event.get("error")
    calls = {}
    for call in trace.calls:
        calls.setdefault(call.get("operation_id"), []).append(call)
    frontier_calls = []
    for event in trace.events:
        if event.get("kind") != "model_context":
            continue
        attempts = calls.get(event.get("operation_id"), [])
        frontier_calls.append({
            "phase": event.get("phase"),
            "operation_id": event.get("operation_id"),
            "model": attempts[-1].get("model") if attempts else None,
            "attempt_statuses": [call.get("status") for call in attempts],
            "submitted": bool(attempts),
            "context": event.get("text", ""),
        })
    return {
        **sources,
        "frontier_calls": frontier_calls,
        "answer_mode": answer_mode,
        "trace_id": trace.identity,
    }
