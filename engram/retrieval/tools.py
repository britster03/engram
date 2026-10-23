"""Semantic retrieval tools; authorization and query text are server-owned.

The model planner supplies entities, predicates and temporal requirements in a
validated RetrievalPlan. The cascade, not the model, chooses the next tool.
"""

from __future__ import annotations

import json
import re
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import pairwise
from typing import Any

from engram import tokens
from engram.retrieval.domain import RetrievalIntent, RetrievalPlan, RetrievalRoute, TemporalKind
from engram.retrieval.templates import run_template

TOOL_DESCRIPTIONS = {
    "current_session_search": "Read the active conversation from cache, then PostgreSQL.",
    "past_session_search": "Find original records in other sessions, using summaries as hints.",
    "facts_search": "Read canonical facts or their history for resolved entities and time bounds.",
    "semantic_memory_search": "Discover semantically related memories and verify them in PostgreSQL.",
    "graph_search": "Read bounded relationship paths and verify every claim in PostgreSQL.",
}

_STOP_WORDS = frozenset(
    [
        "a",
        "an",
        "the",
        "i",
        "me",
        "my",
        "we",
        "our",
        "you",
        "your",
        "he",
        "she",
        "they",
        "it",
        "its",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "do",
        "does",
        "did",
        "have",
        "has",
        "had",
        "what",
        "who",
        "when",
        "where",
        "why",
        "how",
        "which",
        "can",
        "could",
        "would",
        "should",
        "please",
        "tell",
        "give",
        "show",
        "find",
        "recall",
        "remember",
        "say",
        "said",
        "mentioned",
        "discuss",
        "discussed",
        "about",
        "of",
        "to",
        "in",
        "on",
        "for",
        "from",
        "with",
        "and",
        "or",
        "this",
        "that",
        "those",
        "these",
        "last",
        "previous",
        "session",
        "sessions",
        "conversation",
        "earlier",
        "current",
        "now",
        "recently",
    ]
)


def lexical_query(query: str) -> str:
    """Make a bounded OR tsquery from words, with no tsquery operators from input."""
    words = re.findall(r"[^\W_]+", query.casefold(), flags=re.UNICODE)
    words = list(dict.fromkeys(word for word in words if word not in _STOP_WORDS))[:24]
    return " | ".join(words)


_PREDICATE_SEARCH_TERMS = {
    "HAS_ROLE": "role job title position promoted promotion",
    "WORKS_AT": "work works employer company employed",
    "HAS_MANAGER": "manager supervisor reports",
    "LOCATED_IN": "location live lives moved city",
    "ATTENDS_SCHOOL": "school college university studies studying education",
    "HAS_BIRTH_DATE": "born birth birthday",
    "PREFERS": "prefer prefers favorite favourite",
}


def _record_text(row):
    pair = (row.get("payload") or {}).get("turn_pair") or {}
    return " ".join(str((pair.get(role) or {}).get("content") or "") for role in ("user", "assistant"))


@dataclass
class ConversationContext:
    text: str = ""
    sources: list[dict[str, Any]] = field(default_factory=list)
    degraded: list[str] = field(default_factory=list)
    items: list[Any] = field(default_factory=list)


def _pack_records(records: list[dict[str, Any]], budget: int, *, tenant_id: str = "_default", defer_packing: bool = False) -> ConversationContext:
    """Keep source metadata and role boundaries together; never slice JSON."""
    blocks: list[str] = []
    sources: list[dict[str, Any]] = []
    used = 0
    items = []
    for event in records:
        pair = (event.get("payload") or {}).get("turn_pair") or {}
        source = {
            "event_id": event.get("event_id"),
            "session_id": event.get("session_id"),
            "recorded_at": str(event.get("created_at") or ""),
            "source_type": "conversation_record",
        }
        record = {
            **source,
            "messages": [
                {"role": role, "content": str((pair.get(role) or {}).get("content") or ""),
                 "turn_idx": (pair.get(role) or {}).get("turn_idx"),
                 "timestamp": (pair.get(role) or {}).get("timestamp")}
                for role in ("user", "assistant")
                if (pair.get(role) or {}).get("content")
            ],
        }
        if not record["messages"]:
            continue
        block = json.dumps(record, ensure_ascii=False)
        cost = tokens.count_tokens(block)
        if not defer_packing and used + cost > budget:
            continue
        blocks.append(block)
        sources.append(source)
        from engram.retrieval.sufficiency import EvidenceItem
        for message in record["messages"]:
            items.append(EvidenceItem(id=f"event:{source['event_id']}:{message['role']}:{message['turn_idx']}",
                text=message["content"], source_kind="conversation_record", tenant_id=tenant_id,
                role=message["role"], timestamp=str(message["timestamp"] or source["recorded_at"]),
                source={**source, "turn_idx": message["turn_idx"]},
                bundles=event.get("reference_bundles", [])))
        used += cost
    return ConversationContext(text="\n".join(blocks), sources=sources, items=items)


class MemoryTools:
    def __init__(self, ctx: Any, *, tenant_id: str, session_id: str | None) -> None:
        self.ctx = ctx
        self.tenant_id = tenant_id
        self.session_id = session_id
        self.diagnostics: dict[str, Any] = {}

    def current_session_search(self, supplied_context: str | None) -> ConversationContext:
        cfg = self.ctx.cfg.retrieval
        if cfg.quality_mode == "enforce":
            return self._trusted_current_context(supplied_context)
        if supplied_context:
            return ConversationContext(
                text=tokens.truncate_to_tokens(
                    supplied_context, cfg.session_budget_tokens, from_end=True
                ),
                sources=[{"session_id": self.session_id, "source_type": "session_context"}],
            )
        if not self.session_id:
            return ConversationContext()
        cache = self.ctx.session_cache
        if cache is not None:
            try:
                payload = cache.get(self.session_id, tenant_id=self.tenant_id)
                if payload:
                    from engram.session.manager import SessionState

                    session = SessionState.from_payload(payload)
                    text = tokens.truncate_to_tokens(
                        session.render(max_turns=50), cfg.session_budget_tokens, from_end=True
                    )
                    if text:
                        return ConversationContext(
                            text=text,
                            sources=[
                                {
                                    "session_id": self.session_id,
                                    "source_type": "session_cache",
                                }
                            ],
                        )
            except Exception:
                pass  # PostgreSQL is the fallback for a failed/missing cache.
        if self.ctx.control_plane is None:
            return ConversationContext()
        snapshot_getter = getattr(self.ctx.control_plane, "get_session_state", None)
        if callable(snapshot_getter):
            payload = snapshot_getter(self.session_id, tenant_id=self.tenant_id)
            if payload:
                from engram.session.manager import SessionState

                text = tokens.truncate_to_tokens(
                    SessionState.from_payload(payload).render(max_turns=50),
                    cfg.session_budget_tokens,
                    from_end=True,
                )
                if text:
                    return ConversationContext(
                        text=text,
                        sources=[
                            {
                                "session_id": self.session_id,
                                "source_type": "postgresql_session",
                            }
                        ],
                    )
        records = self.ctx.control_plane.list_session_events(
            self.session_id, tenant_id=self.tenant_id, limit=100
        )
        return _pack_records(list(reversed(records)), cfg.session_budget_tokens)

    def _trusted_current_context(self, supplied_context):
        """Only server snapshots or ledger records can assert historical roles."""
        from engram.retrieval.sufficiency import EvidenceItem, render_items
        if self.session_id:
            payload = None
            cache = self.ctx.session_cache
            if cache is not None:
                with suppress(Exception):
                    payload = cache.get(self.session_id, tenant_id=self.tenant_id)
            if not payload and self.ctx.control_plane is not None:
                getter = getattr(self.ctx.control_plane, "get_session_state", None)
                if callable(getter):
                    payload = getter(self.session_id, tenant_id=self.tenant_id)
            if payload and payload.get("session_id") == self.session_id:
                # Native snapshots are tenant scoped and written by the server.
                # Compacted prose is not original evidence. Its older originals
                # remain searchable at the next L1 checkpoint.
                items = []
                for turn in reversed(payload.get("turns", [])[-50:]):
                    if turn.get("role") not in {"user", "assistant"} or not turn.get("content") or not turn.get("ingestible", True):
                        continue
                    idx = turn.get("turn_idx")
                    if not isinstance(idx, int):
                        continue
                    item = EvidenceItem(id=f"session:{self.session_id}:{turn['role']}:{idx}",
                        text=turn["content"], source_kind="conversation_record", tenant_id=self.tenant_id,
                        role=turn["role"], timestamp=turn.get("timestamp"),
                        source={"session_id": self.session_id, "turn_idx": idx,
                                "source_type": "session_turn", "role": turn["role"]})
                    if tokens.count_tokens(render_items([item, *items])) <= self.ctx.cfg.retrieval.session_budget_tokens:
                        items.insert(0, item)
                if items:
                    return ConversationContext(text=render_items(items), items=items,
                        sources=[item.source for item in items])
        if self.session_id and self.ctx.control_plane is not None:
            records = self.ctx.control_plane.list_session_events(self.session_id,
                tenant_id=self.tenant_id, limit=100)
            return _pack_records(list(reversed(records)), self.ctx.cfg.retrieval.session_budget_tokens,
                                 tenant_id=self.tenant_id)
        if supplied_context:
            return ConversationContext(items=[EvidenceItem(id="caller-context", text=supplied_context,
                source_kind="caller_context", tenant_id=self.tenant_id)])
        return ConversationContext()

    def _search_query(self, plan):
        full = lexical_query(plan.query)
        if self.ctx.cfg.retrieval.quality_mode != "enforce":
            return lexical_query(" ".join([plan.query, *plan.entity_hints]))
        names = set(lexical_query(" ".join(plan.entity_hints)).split(" | "))
        topical = [word for word in full.split(" | ") if word not in names]
        from engram.predicate_registry import normalize_predicate
        for predicate in [plan.predicate_hint, *plan.required_predicates]:
            if predicate:
                topical.extend(_PREDICATE_SEARCH_TERMS.get(
                    normalize_predicate(predicate).canonical_predicate, "").split())
        topical = list(dict.fromkeys(topical))[:32]
        # Requiring both an entity and topic prevents popular names alone
        # dominating top-K. Fallback remains bounded and server-authored.
        if topical and names - {""}:
            return "(" + " | ".join(sorted(names - {""})) + ") & (" + " | ".join(topical) + ")"
        return " | ".join(list(dict.fromkeys([*full.split(" | "), *topical]))[:40]).strip(" |")

    @staticmethod
    def _time_relevance(row, plan):
        # A ranking hint only: a later utterance can describe an older event.
        raw = ((row.get("payload") or {}).get("turn_pair") or {}).get("user", {}).get("timestamp")
        try:
            observed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return 0
        def utc(value):
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        observed, scope = utc(observed), plan.temporal_scope
        if scope.since is not None and observed < utc(scope.since):
            return -1
        until = scope.until or scope.as_of
        if until is not None and observed > utc(until):
            return -1
        return 1 if scope.since is not None or until is not None else 0

    def _reference_neighbors(self, records):
        getter = getattr(self.ctx.control_plane, "adjacent_session_events", None)
        if not callable(getter):
            return records
        by_id = {r["event_id"]: dict(r) for r in records}
        anchors = [r for r in records if re.search(
            r"\b(he|she|they|it|his|her|their|this|that|those|these)\b", _record_text(r), re.I
        )][:2]
        for anchor in anchors:
            bundle = "reference:" + anchor["event_id"]
            # Neighbors are discovery context, not asserted co-reference.
            # The sufficiency checker must still establish the binding.
            neighbors = getter(anchor["event_id"], tenant_id=self.tenant_id)
            for row in [anchor, *neighbors]:
                item = by_id.setdefault(row["event_id"], dict(row))
                item["reference_bundles"] = list(dict.fromkeys([*item.get("reference_bundles", []), bundle]))
        return list(by_id.values())

    def _search_records(self, plan, **kwargs):
        cfg = self.ctx.cfg.retrieval
        limit = cfg.conversation_candidates if cfg.quality_mode == "enforce" else cfg.past_session_limit
        records = self.ctx.control_plane.search_session_events(self._search_query(plan),
            tenant_id=self.tenant_id, limit=limit, **kwargs)
        if not records and cfg.quality_mode == "enforce":
            records = self.ctx.control_plane.search_session_events(lexical_query(plan.query),
                tenant_id=self.tenant_id, limit=limit, **kwargs)
        if cfg.quality_mode == "enforce":
            terms = set(lexical_query(plan.query).split(" | "))
            remaining, selected, covered = list(records), [], set()
            names = {name.casefold() for name in plan.entity_hints}
            def features(row):
                # session_context can mention many unrelated entities. Rank
                # only the actual source utterances that will reach packing.
                text = _record_text(row).casefold()
                bindings = {name for name in names if re.search(r"\b" + re.escape(name) + r"\b", text)}
                words = set(re.findall(r"[^\W_]+", text))
                return bindings, len(terms.intersection(words))
            while remaining and len(selected) < cfg.past_session_limit:
                remaining.sort(key=lambda row: (-len(features(row)[0] - covered), -features(row)[1],
                                                -self._time_relevance(row, plan)))
                best = remaining.pop(0)
                selected.append(best)
                covered.update(features(best)[0])
            records = self._reference_neighbors(selected)
        return _pack_records(records, cfg.session_budget_tokens, tenant_id=self.tenant_id,
                             defer_packing=cfg.quality_mode == "enforce")

    def current_session_originals(self, plan: RetrievalPlan) -> ConversationContext:
        """Search older current-session turns after the cached tail was insufficient."""
        if self.ctx.control_plane is None or not self.session_id:
            return ConversationContext()
        return self._search_records(plan, only_session_id=self.session_id)

    def past_session_search(self, plan: RetrievalPlan) -> ConversationContext:
        store = self.ctx.control_plane
        if store is None:
            return ConversationContext()
        search = lexical_query(" ".join([plan.query, *plan.entity_hints]))
        if not search:
            return ConversationContext()
        session_ids: list[str] = []
        degraded: list[str] = []
        summaries = getattr(self.ctx.repository(), "search_session_summaries", None)
        if callable(summaries):
            try:
                session_ids = [
                    row["session_id"]
                    for row in summaries(
                        search,
                        tenant_id=self.tenant_id,
                        exclude_session_id=self.session_id,
                        limit=self.ctx.cfg.retrieval.past_session_limit,
                    )
                ]
            except Exception:
                degraded.append("session_summary_search")
        result = self._search_records(plan, exclude_session_id=self.session_id, session_ids=session_ids)
        result.degraded = degraded
        return result

    def facts_search(self, plan: RetrievalPlan) -> tuple[list[Any], int]:
        from engram.retrieval.orchestrator import _retrieve_route

        # Repository capabilities retain their historical names internally.
        route = (
            RetrievalRoute.L4
            if plan.intent in {RetrievalIntent.HISTORY, RetrievalIntent.EVIDENCE}
            else RetrievalRoute.L1
        )
        rows, discovered = _retrieve_route(
            self.ctx, self.ctx.repository(), plan, route, tenant_id=self.tenant_id
        )
        hints_loader = getattr(self.ctx.repository(), "get_overview_claim_hints", None)
        if plan.intent is not RetrievalIntent.OVERVIEW or not callable(hints_loader):
            return rows, discovered
        from engram.consolidation.canonical import overview_generation_policy
        from engram.retrieval.domain import DiscoveryCandidate
        from engram.retrieval.evidence import CanonicalVerifier
        from engram.retrieval.orchestrator import (
            _dedupe_verified,
            _repository_hydrate,
            _resolve_subject_ids,
        )

        hints = hints_loader(
            _resolve_subject_ids(self.ctx.repository(), plan, tenant_id=self.tenant_id),
            scope_names=plan.entity_hints,
            generation_policy=overview_generation_policy(self.ctx.cfg.consolidation, self.ctx.cfg.core_model),
            tenant_id=self.tenant_id, limit=plan.max_candidates,
        )
        candidates = [DiscoveryCandidate.from_row({"claim_id": identity,
            "tenant_id": self.tenant_id}, retrieval_level="overview_hint") for identity in hints]
        hydrated = _repository_hydrate(self.ctx.repository(), candidates, plan, tenant_id=self.tenant_id)
        # A missing selected claim must never fall back to arbitrary body text.
        hydrated = [row for row in hydrated if row.claim_id in hints]
        verified = (self.ctx.verifier or CanonicalVerifier()).verify(hydrated,
            plan=plan, tenant_id=self.tenant_id, require_stable_id=True, discovery=True)
        self.diagnostics.update(overview_claim_hints=len(hints), overview_claims_verified=len(verified))
        return _dedupe_verified([*verified, *rows])[:plan.max_candidates], discovered + len(hints)

    def semantic_memory_search(self, plan: RetrievalPlan) -> tuple[list[Any], int]:
        from engram.retrieval.orchestrator import _retrieve_route

        return _retrieve_route(
            self.ctx,
            self.ctx.repository(),
            plan,
            RetrievalRoute.L2,
            tenant_id=self.tenant_id,
            diagnostics=self.diagnostics,
        )

    def graph_search(self, plan: RetrievalPlan) -> tuple[list[Any], int]:
        from engram.retrieval.domain import DiscoveryCandidate
        from engram.retrieval.evidence import CanonicalVerifier
        from engram.retrieval.orchestrator import _repository_hydrate, _resolve_subject_ids

        if self.ctx.neo4j is None or not callable(getattr(self.ctx.neo4j, "run_template", None)):
            return [], 0
        if not callable(getattr(self.ctx.repository(), "resolve_aliases", None)):
            return [], 0
        ids = _resolve_subject_ids(self.ctx.repository(), plan, tenant_id=self.tenant_id)
        if not ids:
            return [], 0
        history = plan.temporal_scope.kind not in {TemporalKind.CURRENT, TemporalKind.ANY}
        rows = run_template(
            self.ctx.neo4j,
            "t_canonical_paths",
            {
                "tenant_id": self.tenant_id,
                "memory_ids": [str(value) for value in ids],
                "hops": self.ctx.cfg.retrieval.graph_max_hops,
                "limit": plan.max_candidates,
                "statuses": ["ACTIVE", "CURRENT", "HISTORICAL", "SUPERSEDED"]
                if history
                else ["ACTIVE", "CURRENT"],
            },
            timeout_s=self.ctx.cfg.retrieval.graph_timeout_seconds,
        )
        # Each result is a complete path. Hydrate all its edges, then keep the
        # path only if EVERY edge survives canonical verification.
        verified: dict[str, Any] = {}
        count = 0
        for row in rows:
            edges = row.get("claims") or []
            if not edges or len(edges) > self.ctx.cfg.retrieval.graph_max_hops:
                continue
            candidates = [DiscoveryCandidate.from_row(edge) for edge in edges]
            if any(not edge.claim_id for edge in candidates):
                continue
            count += len(candidates)
            hydrated = _repository_hydrate(
                self.ctx.repository(), candidates, plan, tenant_id=self.tenant_id
            )
            evidence = (self.ctx.verifier or CanonicalVerifier()).verify(
                hydrated,
                plan=plan,
                tenant_id=self.tenant_id,
                require_stable_id=True,
                discovery=True,
            )
            expected = {edge.claim_id for edge in candidates}
            path_nodes = {
                str(value)
                for edge in evidence
                for value in (edge.subject_id, edge.memory_id, edge.object_entity_id)
                if value
            }
            if len(ids) > 1 and not {str(value) for value in ids} <= path_nodes:
                continue
            surviving = {edge.claim_id for edge in evidence}
            if expected <= surviving:
                by_claim = {edge.claim_id: edge for edge in evidence}
                ordered_edges = [by_claim[edge.claim_id] for edge in candidates]
                endpoints = [{str(edge.subject_id), str(edge.object_entity_id)}
                             for edge in ordered_edges if edge.subject_id and edge.object_entity_id]
                if len(endpoints) != len(ordered_edges) or any(
                    not left.intersection(right) for left, right in pairwise(endpoints)
                ):
                    continue
                bundle = "path:" + ":".join(sorted(expected))
                for edge in evidence:
                    if edge.claim_id in expected:
                        previous = verified.get(edge.claim_id)
                        verified[edge.claim_id] = edge.model_copy(
                            update={
                                "bundle_ids": list(
                                    dict.fromkeys(
                                        [
                                            *(previous.bundle_ids if previous else []),
                                            bundle,
                                        ]
                                    )
                                )
                            }
                        )
        return list(verified.values()), count
