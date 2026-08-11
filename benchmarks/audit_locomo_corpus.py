"""Read-only integrity audit for a versioned LoCoMo corpus tenant.

The benchmark runner proves API-level readiness.  This tool independently
checks the authoritative SQLite ledger and filesystem before the same corpus
is reused for retrieval ablations.  It deliberately performs no repair.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from benchmarks.loader import load_locomo
from benchmarks.run_locomo import _session_pairs
from engram import frontmatter
from engram.storage.filesystem import is_generated_memory_path
from engram.uri import path_to_uri

_CAPTION = re.compile(r"\[Image caption:\s*.*?\]", re.IGNORECASE | re.DOTALL)
_CREATION_CUE = re.compile(
    r"\b(?:authored|built|crafted|created|designed|made|make|makes|making|painted|wrote)\b",
    re.IGNORECASE,
)
_GIFT_CUE = re.compile(r"\b(?:gave|gift|gifted|given|present|received)\b", re.IGNORECASE)
_OWNERSHIP = {"drives", "has", "maintains", "owns"}
_LOCATION_RELATIONS = {
    "born_in",
    "lives_in",
    "located_in",
    "moved_from",
    "moved_to",
    "resides_in",
    "visited",
}
_LOCATION_PLACEHOLDERS = {
    "current location",
    "current place",
    "here",
    "new location",
    "new place",
    "somewhere",
    "there",
    "unknown location",
    "unspecified location",
}


def _counts(rows: list[sqlite3.Row], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row[key]) for row in rows).items()))


def _pair(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("turn_pair") or payload.get("turn_group") or payload
    return value if isinstance(value, dict) else {}


def _turn_ids(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for role in ("user", "assistant"):
        turn = _pair(payload).get(role)
        if isinstance(turn, dict) and turn.get("external_id"):
            values.append(str(turn["external_id"]))
    return values


def _source_text(payload: dict[str, Any]) -> tuple[str, str, set[str]]:
    spoken: list[str] = []
    captions: list[str] = []
    speakers: set[str] = set()
    for role in ("user", "assistant"):
        turn = _pair(payload).get(role)
        if not isinstance(turn, dict):
            continue
        content = str(turn.get("content") or "")
        captions.extend(_CAPTION.findall(content))
        spoken.append(_CAPTION.sub("", content))
        if turn.get("image_caption"):
            captions.append(str(turn["image_caption"]))
        if turn.get("speaker"):
            speakers.add(" ".join(str(turn["speaker"]).casefold().split()))
    return (
        " ".join(" ".join(spoken).casefold().split()),
        " ".join(" ".join(captions).casefold().split()),
        speakers,
    )


def audit(
    *,
    data_path: Path,
    conversation_index: int,
    tenant_id: str,
    ledger_path: Path,
    data_dir: Path,
    limit_pairs: int = 0,
    require_complete: bool = True,
) -> dict[str, Any]:
    """Return a JSON-safe corpus report whose ``passed`` flag is fail-closed."""
    conversation = load_locomo(data_path)[conversation_index]
    source_pairs = list(_session_pairs(conversation))
    if limit_pairs:
        source_pairs = source_pairs[:limit_pairs]
    expected_turn_ids = [
        turn.dia_id
        for _session, _source_session, user, assistant in source_pairs
        for turn in ([user, assistant] if assistant is not None else [user])
    ]

    connection = sqlite3.connect(ledger_path)
    connection.row_factory = sqlite3.Row
    events = list(
        connection.execute(
            "SELECT e.*, s.completed_stage FROM events e "
            "LEFT JOIN event_stage_state s USING(event_id) "
            "WHERE e.tenant_id = ? ORDER BY e.created_at, e.event_id",
            (tenant_id,),
        )
    )
    event_ids = {str(row["event_id"]) for row in events}
    payloads = {str(row["event_id"]): json.loads(str(row["payload"])) for row in events}
    artifacts = list(
        connection.execute(
            "SELECT * FROM ingest_artifacts WHERE tenant_id = ? ORDER BY event_id, source_uri",
            (tenant_id,),
        )
    )
    extractions = list(
        connection.execute(
            "SELECT * FROM extractions WHERE tenant_id = ? ORDER BY event_id",
            (tenant_id,),
        )
    )
    outbox = list(
        connection.execute(
            "SELECT * FROM fs_outbox WHERE tenant_id = ? ORDER BY event_id",
            (tenant_id,),
        )
    )
    tasks = list(
        connection.execute(
            "SELECT * FROM consolidation_tasks WHERE tenant_id = ? ORDER BY task_id",
            (tenant_id,),
        )
    )

    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    def fail(check: str, detail: Any) -> None:
        failures.append({"check": check, "detail": detail})

    def warn(check: str, detail: Any) -> None:
        warnings.append({"check": check, "detail": detail})

    if len(events) != len(source_pairs):
        fail("event_count", {"expected": len(source_pairs), "actual": len(events)})
    actual_turn_ids = [
        turn_id for row in events for turn_id in _turn_ids(payloads[str(row["event_id"])])
    ]
    if Counter(actual_turn_ids) != Counter(expected_turn_ids):
        fail(
            "source_turn_ids",
            {
                "missing": sorted(
                    (Counter(expected_turn_ids) - Counter(actual_turn_ids)).elements()
                ),
                "unexpected": sorted(
                    (Counter(actual_turn_ids) - Counter(expected_turn_ids)).elements()
                ),
            },
        )
    if len(actual_turn_ids) != len(set(actual_turn_ids)):
        fail("duplicate_source_turn_ids", len(actual_turn_ids) - len(set(actual_turn_ids)))
    source_conversations = {
        str(turn.get("source_conversation_id") or "")
        for payload in payloads.values()
        for turn in _pair(payload).values()
        if isinstance(turn, dict)
    }
    if source_conversations != {conversation.sample_id}:
        fail("source_conversation_id", conversation.sample_id)
    if require_complete:
        non_complete = [
            str(row["event_id"])
            for row in events
            if row["status"] != "COMPLETE" or row["completed_stage"] != "COMPLETE"
        ]
        if non_complete:
            fail(
                "memory_readiness", {"non_complete": non_complete[:25], "count": len(non_complete)}
            )
    retried = {
        str(row["event_id"]): int(row["retry_count"]) for row in events if row["retry_count"]
    }
    if retried:
        warn("event_retries", retried)
    event_errors = {
        str(row["event_id"]): str(row["error_message"]) for row in events if row["error_message"]
    }
    if event_errors:
        fail("event_errors", event_errors)
    if require_complete and len(extractions) != len(events):
        fail("extraction_count", {"events": len(events), "extractions": len(extractions)})

    root = data_dir / tenant_id
    parsed: dict[str, frontmatter.MemoryFile] = {}
    generated = 0
    parse_errors: dict[str, str] = {}
    if root.exists():
        for path in sorted(root.rglob("*.md")):
            if is_generated_memory_path(path):
                generated += 1
                continue
            uri = path_to_uri(path, root)
            try:
                memory = frontmatter.parse(path.read_text(encoding="utf-8"))
                frontmatter.validate_required_keys(memory.frontmatter)
                frontmatter.validate_metadata(memory.frontmatter)
                parsed[uri] = memory
            except Exception as error:  # an audit must report every corrupt file
                parse_errors[uri] = str(error)
    elif events:
        fail("tenant_root", f"missing {root}")
    if parse_errors:
        fail("frontmatter_parse", parse_errors)

    ids: dict[str, list[str]] = defaultdict(list)
    for uri, memory in parsed.items():
        fm = memory.frontmatter
        ids[str(fm.get("id") or "")].append(uri)
        if fm.get("tenant_id") != tenant_id:
            fail("filesystem_tenant", {"uri": uri, "actual": fm.get("tenant_id")})
        actual_hash = frontmatter.content_hash(memory.body)
        if fm.get("content_hash") != actual_hash:
            fail(
                "content_hash",
                {"uri": uri, "expected": fm.get("content_hash"), "actual": actual_hash},
            )
    duplicate_ids = {key: value for key, value in ids.items() if not key or len(value) > 1}
    if duplicate_ids:
        fail("artifact_identity", duplicate_ids)

    artifact_uris: set[str] = set()
    for row in artifacts:
        uri = str(row["source_uri"])
        artifact_uris.add(uri)
        artifact_memory = parsed.get(uri)
        if artifact_memory is None:
            fail("artifact_file_missing", {"event_id": row["event_id"], "uri": uri})
            continue
        fm = artifact_memory.frontmatter
        if str(fm.get("id") or "") != str(row["artifact_id"]):
            fail(
                "artifact_id_mismatch",
                {"uri": uri, "ledger": row["artifact_id"], "file": fm.get("id")},
            )
        if str(fm.get("content_hash") or "") != str(row["content_hash"]):
            fail("artifact_hash_mismatch", uri)
        if require_complete and (
            row["filesystem_state"] != "COMMITTED" or row["kg_state"] != "COMMITTED"
        ):
            fail(
                "artifact_commit_state",
                {"uri": uri, "filesystem": row["filesystem_state"], "kg": row["kg_state"]},
            )
    unledgered = sorted(set(parsed) - artifact_uris)
    if unledgered:
        fail("unledgered_memory_files", unledgered[:25])

    episode_by_event: dict[str, list[tuple[str, frontmatter.MemoryFile]]] = defaultdict(list)
    semantic_findings: list[dict[str, Any]] = []
    atomized = 0
    relation_review = 0
    for uri, memory in parsed.items():
        fm = memory.frontmatter
        node_type = fm.get("node_type")
        source_event = str(
            fm.get("source_event_id") or (fm.get("provenance") or {}).get("ingest_event_id") or ""
        )
        if source_event and source_event not in event_ids:
            fail("unknown_source_event", {"uri": uri, "event_id": source_event})
        if node_type == "DOCUMENT":
            episode_by_event[source_event].append((uri, memory))
            payload = payloads.get(source_event, {})
            source_ids = _turn_ids(payload)
            if list(fm.get("source_turn_ids") or []) != source_ids:
                fail(
                    "episode_source_turn_ids",
                    {"uri": uri, "expected": source_ids, "actual": fm.get("source_turn_ids")},
                )
            for role in ("user", "assistant"):
                turn = _pair(payload).get(role)
                if not isinstance(turn, dict):
                    continue
                content = str(turn.get("content") or "")
                if content and content not in memory.body:
                    fail("source_content_preservation", {"uri": uri, "role": role})
                caption = str(turn.get("image_caption") or "")
                if caption and caption not in memory.body:
                    fail("caption_preservation", {"uri": uri, "role": role})
        if node_type != "FACT":
            continue
        fact = fm.get("fact")
        if not isinstance(fact, dict):
            fail("fact_payload", uri)
            continue
        if fact.get("atomized_from"):
            atomized += 1
        if fact.get("relation_normalized") is not True:
            relation_review += 1
        # Conflict classification applies only to graph assertions between two
        # entity nodes. Literal-object FACTs remain first-class files/nodes but
        # intentionally have no RELATES_TO edge to classify.
        if fact.get("object_uri") and not isinstance(fm.get("conflict"), dict):
            fail("conflict_decision", uri)
        expected_episode = f"mem://user/episodes/{source_event}.md"
        if fm.get("source_episode_uri") != expected_episode:
            fail("fact_episode_reference", {"uri": uri, "expected": expected_episode})
        payload = payloads.get(source_event, {})
        spoken, captions, speakers = _source_text(payload)
        relation = str(fact.get("relation") or "")
        subject = " ".join(str(fact.get("subject") or "").casefold().split())
        obj = " ".join(str(fact.get("object") or "").casefold().split())
        if relation in _OWNERSHIP and obj and obj in captions and obj not in spoken:
            fail("caption_only_ownership", uri)
        if relation in _LOCATION_RELATIONS and obj in _LOCATION_PLACEHOLDERS:
            fail("placeholder_location_fact", uri)
        if relation == "created_by":
            if subject in speakers:
                fail("created_by_direction", uri)
            if not _CREATION_CUE.search(spoken):
                fail("created_by_source_support", uri)
        if relation == "gifted_by" and not _GIFT_CUE.search(spoken):
            fail("gifted_by_source_support", uri)
        semantic_findings.append({"uri": uri, "relation": relation, "status": fm.get("status")})

    episode_multiplicity = {
        event_id: len(values) for event_id, values in episode_by_event.items() if len(values) != 1
    }
    episode_expected_events = (
        event_ids
        if require_complete
        else {
            str(row["event_id"])
            for row in events
            if row["completed_stage"]
            in {
                "FILESYSTEM_COMMITTED",
                "KG_COMMITTED",
                "CONSOLIDATION_COMMITTED",
                "COMPLETE",
            }
        }
    )
    missing_episodes = sorted(episode_expected_events - set(episode_by_event))
    if episode_multiplicity or missing_episodes:
        fail(
            "episode_per_event",
            {"multiplicity": episode_multiplicity, "missing": missing_episodes[:25]},
        )

    if require_complete:
        bad_outbox = [
            {"event_id": row["event_id"], "state": row["state"], "error": row["error_message"]}
            for row in outbox
            if row["state"] != "INDEXED" or row["error_message"]
        ]
        if bad_outbox:
            fail("filesystem_outbox", bad_outbox[:25])

    report = {
        "schema_version": 1,
        "passed": not failures,
        "tenant_id": tenant_id,
        "conversation": conversation.sample_id,
        "expected": {"pairs": len(source_pairs), "turns": len(expected_turn_ids)},
        "sqlite": {
            "event_count": len(events),
            "status_counts": _counts(events, "status"),
            "stage_counts": _counts(events, "completed_stage"),
            "retry_sum": sum(int(row["retry_count"]) for row in events),
            "extraction_count": len(extractions),
            "artifact_count": len(artifacts),
            "artifact_type_counts": _counts(artifacts, "artifact_type"),
            "outbox_state_counts": _counts(outbox, "state"),
            "consolidation_status_counts": _counts(tasks, "status"),
        },
        "filesystem": {
            "memory_count": len(parsed),
            "node_type_counts": dict(
                sorted(
                    Counter(str(m.frontmatter.get("node_type")) for m in parsed.values()).items()
                )
            ),
            "generated_file_count": generated,
            "atomized_fact_count": atomized,
            "relation_review_count": relation_review,
        },
        "semantic_sample": semantic_findings[:25],
        "failures": failures,
        "warnings": warnings,
    }
    connection.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="benchmarks/data/locomo10.json")
    parser.add_argument("--conversation-index", type=int, default=0)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--limit-pairs", type=int, default=0)
    parser.add_argument("--allow-incomplete", action="store_true")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    report = audit(
        data_path=Path(args.data),
        conversation_index=args.conversation_index,
        tenant_id=args.tenant_id,
        ledger_path=Path(args.ledger),
        data_dir=Path(args.data_dir),
        limit_pairs=args.limit_pairs,
        require_complete=not args.allow_incomplete,
    )
    rendered = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        destination = Path(args.output)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
