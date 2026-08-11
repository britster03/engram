"""Retrieval Orchestrator — full L0→L4 cascade with fused plan-judge (§4).

Pipeline:
  L0 gate (optional)  →  L1 plan + vector search  →  Ln plan-with-judge
    → Ln execute  →  (continue or terminate)  →  MSC assembly  →  Frontier
    → Frontier re-entry on NEED_MORE up to `max_reentries`.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from engram import frontmatter, prompts, tracing
from engram import metrics as metrics_mod
from engram import tokens as tok_mod
from engram.config import EngramConfig
from engram.frontmatter import FrontmatterError
from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.models.frontier import FrontierLLMProvider, FrontierVerdict
from engram.models.semantic import L1PlanOutput, LnPlanOutput, complete_validated
from engram.resilience import CircuitOpenError
from engram.retrieval.l0_classifier import ClassifierStatus
from engram.retrieval.l0_gate import AlwaysClass0Classifier, L0Classifier, run_l0_gate
from engram.retrieval.templates import TemplateError, run_template
from engram.retrieval.tree_render import render_tree
from engram.storage.filesystem import FilesystemStore

log = logging.getLogger(__name__)


_DEPTH_ORDER = ["SESSION", "L0", "L1", "L2", "L3", "L4"]
RetrievalMode = Literal["adaptive", "forced", "no_memory", "vector_only"]


def _depth_rank(depth: str) -> int:
    try:
        return _DEPTH_ORDER.index(depth)
    except ValueError:
        return len(_DEPTH_ORDER) - 1


def _notify_step(
    on_step: Callable[[dict[str, Any]], None] | None,
    md: RetrievalMetadata,
    step: str,
) -> None:
    """Emit a snapshot of retrieval metadata after a cascade step."""
    if on_step is None:
        return
    on_step({"step": step, **md.to_dict()})


@dataclass
class RetrievalMetadata:
    retrieval_mode: str = "adaptive"
    min_depth: str | None = None
    max_depth: str | None = None
    cascade_depth_reached: str = "L0"
    levels_visited: list[str] = field(default_factory=list)
    predicted_depth: str | None = None
    nodes_retrieved: int = 0
    total_context_tokens: int = 0
    reentries: int = 0
    latency_ms: dict[str, float] = field(default_factory=dict)
    l0_decision: str | None = None
    l0_reason: str | None = None
    trace_id: str | None = None
    trace: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "retrieval_mode": self.retrieval_mode,
            "min_depth": self.min_depth,
            "max_depth": self.max_depth,
            "cascade_depth_reached": self.cascade_depth_reached,
            "levels_visited": self.levels_visited,
            "predicted_depth": self.predicted_depth,
            "nodes_retrieved": self.nodes_retrieved,
            "total_context_tokens": self.total_context_tokens,
            "reentries": self.reentries,
            "latency_ms": self.latency_ms,
            "l0_decision": self.l0_decision,
            "l0_reason": self.l0_reason,
        }


@dataclass
class QueryResult:
    answer: str
    retrieval_metadata: RetrievalMetadata


@dataclass
class OrchestratorContext:
    cfg: EngramConfig
    fs: FilesystemStore
    neo4j: Any
    core: CoreModelProvider
    frontier: FrontierLLMProvider
    embed: EmbeddingService
    l0_classifier: L0Classifier = field(default_factory=AlwaysClass0Classifier)
    l0_classifier_status: ClassifierStatus | None = None


# ----------------------------------------------------------------------
# Top-level entry
# ----------------------------------------------------------------------

def run_query(
    ctx: OrchestratorContext,
    *,
    session_id: str | None,
    query: str,
    session_context: str | None = None,
    max_depth: str | None = None,
    min_depth: str | None = None,
    max_reentries: int | None = None,
    include_trace: bool = False,
    force_retrieval: bool = False,
    retrieval_mode: RetrievalMode = "adaptive",
    on_step: Callable[[dict[str, Any]], None] | None = None,
) -> QueryResult:
    # Keep the legacy boolean compatible while giving benchmark runs one
    # explicit, manifestable strategy name.
    if force_retrieval and retrieval_mode == "adaptive":
        retrieval_mode = "forced"
    max_depth = max_depth or ctx.cfg.retrieval.max_depth
    if max_depth not in _DEPTH_ORDER:
        raise ValueError(f"invalid max_depth: {max_depth}")
    if min_depth is not None:
        if min_depth not in {"L1", "L2", "L3", "L4"}:
            raise ValueError(f"invalid min_depth: {min_depth}")
        if retrieval_mode != "forced":
            raise ValueError("min_depth requires retrieval_mode='forced'")
        if _depth_rank(min_depth) > _depth_rank(max_depth):
            raise ValueError("min_depth cannot exceed max_depth")

    md = RetrievalMetadata(
        retrieval_mode=retrieval_mode,
        min_depth=min_depth,
        max_depth=max_depth,
    )
    if include_trace:
        md.trace_id = f"trace-{uuid.uuid4().hex}"
        md.trace = {
            "retrieval_mode": retrieval_mode,
            "request": {"min_depth": min_depth, "max_depth": max_depth},
            "vector_queries": [],
            "commands": [],
            "sufficiency_decisions": [],
            "hits": [],
            "selected_sources": [],
            "token_allocation": {},
            "reentry_requests": [],
            "l0_gate": {},
            "model_calls": [],
        }
    max_reentries = max_reentries if max_reentries is not None else ctx.cfg.retrieval.max_reentries

    if retrieval_mode == "no_memory":
        return _run_no_memory_ablation(
            ctx,
            md,
            session_context=session_context,
            query=query,
            on_step=on_step,
        )
    if retrieval_mode == "vector_only":
        return _run_vector_only_ablation(
            ctx,
            md,
            session_context=session_context,
            query=query,
            on_step=on_step,
        )

    # --- L0 ------------------------------------------------------------------
    md.levels_visited.append("L0")
    md.cascade_depth_reached = "L0"
    t = time.perf_counter()
    with tracing.span("query.l0_gate"):
        try:
            gate = run_l0_gate(
                query,
                classifier=ctx.l0_classifier,
                embed=ctx.embed,
                neo4j=ctx.neo4j,
                threshold=ctx.cfg.gating.classification_threshold,
                memory_hit_threshold=ctx.cfg.gating.memory_hit_threshold,
                skip=ctx.cfg.retrieval.l0_skip or retrieval_mode == "forced",
                skip_reason=(
                    "retrieval.l0_skip=true"
                    if ctx.cfg.retrieval.l0_skip
                    else "request.force_retrieval=true"
                ),
                classifier_mode=ctx.cfg.gating.classifier_mode,
                classifier_available=(
                    ctx.l0_classifier_status.loaded
                    if ctx.l0_classifier_status is not None
                    else ctx.cfg.gating.classifier_mode == "off"
                ),
            )
        except Exception as err:
            # L0 is best-effort — on failure, default to CONTINUE so queries
            # still get the retrieval cascade.
            log.warning("L0 gate error; defaulting to CONTINUE: %s", err)
            from engram.retrieval.l0_gate import GateDecision
            gate = GateDecision(
                decision="CONTINUE", reason=f"l0-error:{type(err).__name__}",
            )
    md.latency_ms["l0_gate"] = (time.perf_counter() - t) * 1000
    md.l0_decision = gate.decision
    md.l0_reason = gate.reason
    if md.trace is not None:
        md.trace["l0_gate"] = {
            "decision": gate.decision,
            "reason": gate.reason,
            "classifier_mode": gate.classifier_mode,
            "classifier_probability": gate.classifier_probability,
            "classifier_status": (
                ctx.l0_classifier_status.to_dict()
                if ctx.l0_classifier_status is not None
                else None
            ),
        }
    _notify_step(on_step, md, "l0_gate")
    if gate.decision == "BYPASS":
        # Go straight to the frontier with only session context + query.
        msc = _assemble_msc(session_context=session_context, ltm_blocks=[], user_query=query)
        return _answer_loop(ctx, md, session_context, query, msc, max_reentries,
                            max_depth=max_depth, accumulated_hits=[], on_step=on_step)

    # --- L1 ------------------------------------------------------------------
    t = time.perf_counter()
    with tracing.span("query.l1_plan"):
        plan = _l1_plan(
            ctx, query=query, session_context=session_context,
            memory_hit=gate.memory_hit,
            metadata=md,
        )
    md.latency_ms["l1_plan"] = (time.perf_counter() - t) * 1000
    md.levels_visited.append("L1")
    md.cascade_depth_reached = "L1"
    md.predicted_depth = plan.get("predicted_depth", "L4")
    _trace_plan(md, "L1", plan)
    _notify_step(on_step, md, "l1_plan")

    minimum_rank = _depth_rank(min_depth or "L1")
    if (
        plan.get("session_sufficient")
        and plan.get("session_answer_context")
        and minimum_rank <= _depth_rank("L1")
    ):
        msc = _assemble_msc(
            session_context=session_context,
            ltm_blocks=[plan["session_answer_context"]],
            user_query=query,
        )
        return _answer_loop(ctx, md, session_context, query, msc, max_reentries,
                            max_depth=max_depth, accumulated_hits=[], on_step=on_step)

    t = time.perf_counter()
    with tracing.span("query.l1_execute"):
        accumulated = _execute_l1(ctx, plan, query)
    md.latency_ms["l1_execute"] = (time.perf_counter() - t) * 1000
    md.nodes_retrieved = len(accumulated)
    current_results = accumulated
    _trace_hits(ctx, md, accumulated)
    _notify_step(on_step, md, "l1_execute")

    predicted_depth = plan.get("predicted_depth", "L4")
    target_depth = max(_depth_rank(predicted_depth), minimum_rank)
    cap_depth = _depth_rank(max_depth)
    target_depth = min(target_depth, cap_depth)

    # --- L2 / L3 / L4 --------------------------------------------------------
    prev_level = "L1"
    for level_name in ("L2", "L3", "L4"):
        if _depth_rank(level_name) > target_depth:
            break
        t = time.perf_counter()
        with tracing.span(f"query.{level_name.lower()}_plan"):
            ln_plan = _ln_plan(
                ctx,
                level=level_name,
                query=query,
                session_context=session_context,
                previous_level=prev_level,
                previous_results=current_results,
                metadata=md,
            )
        md.latency_ms[f"{level_name.lower()}_plan"] = (time.perf_counter() - t) * 1000
        md.levels_visited.append(level_name)
        md.cascade_depth_reached = level_name
        _trace_plan(md, level_name, ln_plan)
        _notify_step(on_step, md, f"{level_name.lower()}_plan")
        # A forced benchmark depth is a lower bound on planner visitation.
        # Respect sufficiency at that layer, but never let an earlier layer
        # silently turn an advertised L4 run into an L2 run.
        if ln_plan.get("terminate_cascade") and _depth_rank(level_name) >= minimum_rank:
            break
        t = time.perf_counter()
        with tracing.span(f"query.{level_name.lower()}_execute"):
            current_results = _execute_commands(
                ctx, ln_plan.get("commands", []),
                level=level_name, existing=current_results,
            )
        md.latency_ms[f"{level_name.lower()}_execute"] = (time.perf_counter() - t) * 1000
        md.nodes_retrieved = len(current_results)
        _trace_hits(ctx, md, current_results)
        _notify_step(on_step, md, f"{level_name.lower()}_execute")
        prev_level = level_name

    # --- MSC assembly --------------------------------------------------------
    t = time.perf_counter()
    ltm_blocks = _format_ltm_blocks(
        ctx, current_results, md.cascade_depth_reached, trace=md.trace
    )
    md.latency_ms["msc_assembly"] = (time.perf_counter() - t) * 1000
    msc = _assemble_msc(session_context=session_context, ltm_blocks=ltm_blocks, user_query=query)
    md.total_context_tokens = _est_tokens(msc)
    _trace_token_allocation(md, session_context, ltm_blocks, query)
    _notify_step(on_step, md, "msc_assembly")

    return _answer_loop(
        ctx,
        md,
        session_context,
        query,
        msc,
        max_reentries,
        max_depth=max_depth,
        accumulated_hits=current_results,
        on_step=on_step,
    )


def _mark_l0_not_run(md: RetrievalMetadata, reason: str) -> None:
    md.l0_decision = "NOT_RUN"
    md.l0_reason = reason
    if md.trace is not None:
        md.trace["l0_gate"] = {
            "decision": "NOT_RUN",
            "reason": reason,
            "classifier_mode": None,
            "classifier_probability": None,
            "classifier_status": None,
        }


def _run_no_memory_ablation(
    ctx: OrchestratorContext,
    md: RetrievalMetadata,
    *,
    session_context: str | None,
    query: str,
    on_step: Callable[[dict[str, Any]], None] | None,
) -> QueryResult:
    """Answer with the frontier only, without gate, planner, graph, or vectors."""
    md.levels_visited.append("FRONTIER")
    md.cascade_depth_reached = "NO_MEMORY"
    md.predicted_depth = "NO_MEMORY"
    _mark_l0_not_run(md, "retrieval_mode=no_memory")
    msc = _assemble_msc(
        session_context=session_context,
        ltm_blocks=[],
        user_query=query,
    )
    md.total_context_tokens = _est_tokens(msc)
    _trace_token_allocation(md, session_context, [], query)
    _notify_step(on_step, md, "no_memory")
    return _answer_loop(
        ctx,
        md,
        session_context,
        query,
        msc,
        0,
        max_depth="SESSION",
        accumulated_hits=[],
        on_step=on_step,
    )


def _run_vector_only_ablation(
    ctx: OrchestratorContext,
    md: RetrievalMetadata,
    *,
    session_context: str | None,
    query: str,
    on_step: Callable[[dict[str, Any]], None] | None,
) -> QueryResult:
    """Run one raw-query vector search with no semantic planner or cascade."""
    md.levels_visited.append("L1")
    md.cascade_depth_reached = "L1"
    md.predicted_depth = "L1"
    _mark_l0_not_run(md, "retrieval_mode=vector_only")
    plan = {
        "predicted_depth": "L1",
        "mode": "VECTOR_ONLY",
        "vector_queries": [query],
        "entry_points": [],
        "commands": [],
    }
    _trace_plan(md, "L1", plan)
    started = time.perf_counter()
    hits = _execute_l1(ctx, plan, query)
    md.latency_ms["l1_execute"] = (time.perf_counter() - started) * 1000
    md.nodes_retrieved = len(hits)
    _trace_hits(ctx, md, hits)
    blocks = _format_ltm_blocks(ctx, hits, "L1", trace=md.trace)
    msc = _assemble_msc(
        session_context=session_context,
        ltm_blocks=blocks,
        user_query=query,
    )
    md.total_context_tokens = _est_tokens(msc)
    _trace_token_allocation(md, session_context, blocks, query)
    _notify_step(on_step, md, "vector_only")
    return _answer_loop(
        ctx,
        md,
        session_context,
        query,
        msc,
        0,
        max_depth="L1",
        accumulated_hits=hits,
        on_step=on_step,
    )


# ----------------------------------------------------------------------
# Planning
# ----------------------------------------------------------------------

def _l1_plan(
    ctx: OrchestratorContext,
    *,
    query: str,
    session_context: str | None,
    memory_hit: dict | None,
    metadata: RetrievalMetadata | None = None,
) -> dict[str, Any]:
    try:
        tree = render_tree(
            ctx.fs, max_tokens=ctx.cfg.filesystem.tree_display_max_tokens
        )
    except Exception as err:
        log.warning("tree rendering failed; continuing with empty tree: %s", err)
        tree = "(tree render unavailable)"
    prompt = prompts.render(
        "l1_plan",
        query=query,
        session_context=session_context,
        tree_render=tree,
        memory_hit=(memory_hit and memory_hit.get("l0_abstract")),
    )
    try:
        validated, result = complete_validated(
            ctx.core,
            task="l1_plan",
            schema=L1PlanOutput,
            system_prompt=prompt,
            user_prompt="Return the plan JSON.",
        )
        _trace_core_call(metadata, ctx, "l1_plan", result)
        out = validated.model_dump(mode="json")
    except (CoreModelError, CircuitOpenError, Exception) as err:
        # §Graceful degradation: when the Core Model is unavailable we fall
        # back to a minimal "just vector-search the raw query" plan. The
        # frontier still sees whatever vector hits Neo4j returns; it can
        # answer from session + raw hits.
        log.warning("L1 plan failed; using fallback plan: %s", err)
        out = {}
    out.setdefault("predicted_depth", "L4")
    out.setdefault("vector_queries", [query])
    out.setdefault("mode", "HYBRID")
    out.setdefault("commands", [])
    return out


def _ln_plan(
    ctx: OrchestratorContext,
    *,
    level: str,
    query: str,
    session_context: str | None,
    previous_level: str,
    previous_results: list[dict[str, Any]],
    metadata: RetrievalMetadata | None = None,
) -> dict[str, Any]:
    summary = _summarise_results(previous_results, limit=20)
    prompt = prompts.render(
        "ln_plan",
        level=level,
        query=query,
        session_context=session_context,
        previous_level=previous_level,
        previous_results=summary,
    )
    try:
        validated, result = complete_validated(
            ctx.core,
            task="ln_plan",
            schema=LnPlanOutput,
            system_prompt=prompt,
            user_prompt="Return the fused plan-judge JSON.",
        )
        _trace_core_call(metadata, ctx, f"{level.lower()}_plan", result)
        out = validated.model_dump(mode="json")
    except (CoreModelError, CircuitOpenError, Exception) as err:
        # §Graceful degradation: when Ln planning fails we terminate the
        # cascade and let the frontier answer from what we already have.
        log.warning("Ln plan (%s) failed; terminating cascade: %s", level, err)
        return {
            "previous_level_sufficient": False,
            "terminate_cascade": True,
            "commands": [],
            "coverage": {
                "aspects_covered": [],
                "aspects_missing": [f"core-model-unavailable-at-{level}"],
            },
        }
    out.setdefault("terminate_cascade", False)
    out.setdefault("commands", [])
    return out


# ----------------------------------------------------------------------
# Execution
# ----------------------------------------------------------------------

def _execute_l1(
    ctx: OrchestratorContext, plan: dict[str, Any], original_query: str
) -> list[dict[str, Any]]:
    queries = plan.get("vector_queries") or [original_query]
    k = min(ctx.cfg.retrieval.max_l1_vector_results, 10)
    seen: set[str] = set()
    hits: list[dict[str, Any]] = []
    for q in queries[:3]:
        try:
            emb = ctx.embed.embed(q)
            rows = ctx.neo4j.vector_search(
                emb, k=k, dormant_floor=ctx.cfg.decay.dormant_floor
            )
        except Exception as err:
            # §Graceful degradation: if embeddings or Neo4j are unavailable we
            # continue with zero vector hits rather than failing the query.
            # The frontier will still get session + explicit entry_points.
            log.warning("L1 vector search failed for %r: %s", q, err)
            rows = []
        for r in rows:
            if r["source_uri"] in seen:
                continue
            seen.add(r["source_uri"])
            r["retrieval_level"] = "L1"
            hits.append(r)
    # Include explicit entry_points from the plan
    for uri in plan.get("entry_points") or []:
        if not isinstance(uri, str) or uri in seen:
            continue
        seen.add(uri)
        hits.append({"source_uri": uri, "score": None, "retrieval_level": "L1_entry"})
    hits.sort(key=lambda r: r.get("score") or 0.0, reverse=True)
    hits = hits[: ctx.cfg.retrieval.max_l1_vector_results]
    # L1 prompts advertise typed AGFS/KG/HYBRID commands. Execute them through
    # the same whitelist used by deeper levels instead of silently ignoring
    # the model's plan.
    if plan.get("commands"):
        hits = _execute_commands(
            ctx,
            plan["commands"][:20],
            level="L1",
            existing=hits,
        )
    return hits[: ctx.cfg.retrieval.max_l1_vector_results]


def _execute_commands(
    ctx: OrchestratorContext,
    commands: list[dict[str, Any]],
    *,
    level: str,
    existing: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Dispatch the CLI-style commands in §8.4 against storage / KG.

    Recognised commands (either via `template` or a top-level `command` key):
      - find (vector search) / t_top_k_vector
      - ls / t_children_of
      - cat (read full .md body)
      - overview / t_overview_for (read overview.md)
      - rel / t_neighbours_by_relation (follow RELATES_TO hops)
      - history / t_history_chain (SUPERSEDES chain)
      - template <name> (arbitrary whitelisted Cypher template)
    """
    seen: set[str] = {e["source_uri"] for e in existing if e.get("source_uri")}
    # Commands may enrich rows returned by an earlier retrieval level. Copy
    # those mappings so an explicit filesystem read does not mutate the
    # caller's input or create a second hit for the same memory.
    out = [dict(item) for item in existing]
    timeout = (
        ctx.cfg.knowledge_graph.l4_query_timeout_seconds
        if level in ("L3", "L4")
        else ctx.cfg.knowledge_graph.l2_query_timeout_seconds
    )
    for cmd in commands:
        template = cmd.get("template") or cmd.get("command")
        params = cmd.get("params") or {k: v for k, v in cmd.items()
                                       if k not in {"template", "command"}}
        if not template:
            continue

        # --- Filesystem-backed commands ----------------------------------
        if template in ("overview", "t_overview_for", "02_overview_for"):
            uri = params.get("uri") or params.get("path")
            if uri and ctx.fs.exists(uri):
                overview = ctx.fs.read_overview(uri) or ""
                out.append({
                    "source_uri": uri,
                    "l0_abstract": overview[:500],
                    "overview": overview,
                    "retrieval_level": "L3",
                })
            continue
        if template == "cat":
            uri = params.get("uri") or params.get("path")
            if uri and ctx.fs.exists(uri):
                body = _read_full_body(ctx, uri) or ""
                existing_row = next(
                    (row for row in out if row.get("source_uri") == uri),
                    None,
                )
                if existing_row is not None:
                    existing_row["full_body"] = body
                    existing_row["retrieval_level"] = f"{level}_cat"
                else:
                    seen.add(uri)
                    out.append({
                        "source_uri": uri,
                        "l0_abstract": body.splitlines()[0][:500] if body else "",
                        "full_body": body,
                        "retrieval_level": f"{level}_cat",
                    })
            continue

        # --- Vector search ----------------------------------------------
        if template in ("find", "t_top_k_vector"):
            query_text = params.get("query")
            k = max(1, min(int(params.get("k", 10)), ctx.cfg.retrieval.max_l1_vector_results))
            scope = params.get("scope") or params.get("prefix")
            if query_text:
                try:
                    emb = ctx.embed.embed(query_text)
                    rows = ctx.neo4j.vector_search(
                        emb, k=k, uri_prefix=scope,
                        dormant_floor=ctx.cfg.decay.dormant_floor,
                    )
                except Exception as err:
                    log.warning("vector search failed in %s: %s", level, err)
                    rows = []
                for r in rows:
                    if r["source_uri"] in seen:
                        continue
                    seen.add(r["source_uri"])
                    r["retrieval_level"] = level
                    out.append(r)
            continue

        # --- Ergonomic aliases that map to templates ---------------------
        aliased = _alias_to_template(template, params)
        if aliased is not None:
            template, params = aliased
        params = _normalize_template_params(template, params)

        try:
            rows = run_template(ctx.neo4j, template, params, timeout_s=timeout)
        except TemplateError as err:
            log.warning("orchestrator rejected template %s: %s", template, err)
            continue
        except Exception:
            log.exception("template %s failed", template)
            continue
        for r in rows:
            uri = r.get("source_uri")
            if not uri or uri in seen:
                continue
            seen.add(uri)
            r["retrieval_level"] = level
            out.append(r)
    return out


def _alias_to_template(
    command: str, params: dict[str, Any]
) -> tuple[str, dict[str, Any]] | None:
    """Translate short-form CLI commands (§8.4) into Cypher template calls."""
    if command == "ls":
        uri = params.get("uri") or params.get("node_uri")
        if uri:
            return ("t_children_of", {"uri": uri,
                                      "limit": int(params.get("limit", 50))})
        return None
    if command == "rel":
        node_uri = params.get("node_id") or params.get("node_uri") or params.get("uri")
        rel = params.get("type") or params.get("relation")
        hops = int(params.get("hops", 1))
        if node_uri:
            return ("t_neighbours_by_relation",
                    {"node_uri": node_uri, "relation": rel, "hops": hops,
                     "limit": int(params.get("limit", 25))})
        return None
    if command == "history":
        node_uri = params.get("node_id") or params.get("node_uri") or params.get("uri")
        if node_uri:
            return ("t_history_chain",
                    {"node_uri": node_uri, "limit": int(params.get("limit", 20))})
        return None
    return None


def _normalize_template_params(
    template: str, params: dict[str, Any]
) -> dict[str, Any]:
    """Normalize bounded planner aliases to each template's typed contract."""
    normalized = dict(params)
    if template in {
        "t_neighbours_by_relation",
        "t_temporal_filter",
        "t_history_chain",
        "t_cross_references",
    }:
        normalized.setdefault(
            "node_uri",
            normalized.get("node_id") or normalized.get("uri"),
        )
    elif template == "t_path_between":
        normalized.setdefault(
            "src_uri",
            normalized.get("start_node")
            or normalized.get("start_uri")
            or normalized.get("source_uri")
            or normalized.get("from_uri"),
        )
        normalized.setdefault(
            "dst_uri",
            normalized.get("end_node")
            or normalized.get("end_uri")
            or normalized.get("destination_uri")
            or normalized.get("to_uri")
            or normalized.get("target_uri"),
        )
    elif template == "t_children_of":
        normalized.setdefault("uri", normalized.get("node_uri"))
    elif template == "t_find_by_uri_prefix":
        normalized.setdefault(
            "prefix", normalized.get("uri") or normalized.get("scope")
        )
    return {key: value for key, value in normalized.items() if value is not None}


# ----------------------------------------------------------------------
# MSC assembly
# ----------------------------------------------------------------------

def _summarise_results(results: list[dict[str, Any]], *, limit: int) -> str:
    lines = []
    for r in results[:limit]:
        uri = r.get("source_uri", "?")
        abs_ = (r.get("l0_abstract") or "")[:160]
        score = r.get("score")
        score_tag = f" score={score:.3f}" if isinstance(score, float) else ""
        lines.append(f"- {uri}{score_tag} — {abs_}")
    return "\n".join(lines) or "(no results)"


def _format_ltm_blocks(
    ctx: OrchestratorContext,
    results: list[dict[str, Any]],
    cascade_depth: str,
    *,
    trace: dict[str, Any] | None = None,
) -> list[str]:
    """Build bounded LTM context blocks from the selected retrieval hits.

    A vector hit's abstract is sufficient for ranking but often omits the
    exact reason, date, or list needed for answer generation. Load the source
    body at L1/L2 as well as L4, while retaining the smaller 6k-token early-
    cascade budget. This fixes context selection without increasing candidate
    count or leaking memory bodies into traces.
    """
    budget = (
        ctx.cfg.retrieval.full_doc_budget_tokens
        if cascade_depth in ("L4",)
        else ctx.cfg.retrieval.overview_budget_tokens
    )
    spent = 0
    blocks: list[str] = []
    for r in results:
        source_uri = r.get("source_uri")
        if not source_uri:
            continue
        if "full_body" in r:
            body = r["full_body"]
            level = r.get("retrieval_level", "L4")
        elif r.get("overview"):
            body = r["overview"]
            level = "L3"
        elif cascade_depth in ("L1", "L2", "L4"):
            body = _read_full_body(ctx, source_uri)
            if body is None:
                body = r.get("l0_abstract") or ""
            level = r.get("retrieval_level", cascade_depth)
        else:
            body = r.get("l0_abstract") or ""
            level = r.get("retrieval_level", cascade_depth)
        status, confidence, temporal = _status_for(ctx, source_uri)
        if status == "HISTORICAL":
            continue
        tokens = _est_tokens(body)
        if spent + tokens > budget:
            continue
        annotation = _annotate(
            status,
            confidence,
            source_uri,
            level=level,
            temporal=temporal,
        )
        blocks.append(f"{annotation}\n{body.strip()}")
        if trace is not None and len(trace["selected_sources"]) < 100:
            trace["selected_sources"].append(
                {
                    "source_uri": source_uri,
                    "retrieval_level": level,
                    "source_turn_ids": _source_turn_ids_for(ctx, r),
                }
            )
        spent += tokens
    return blocks


def _read_full_body(ctx: OrchestratorContext, source_uri: str) -> str | None:
    try:
        raw = ctx.fs.read(source_uri)
        mf = frontmatter.parse(raw)
        return mf.body
    except (OSError, FrontmatterError):
        return None


def _status_for(
    ctx: OrchestratorContext, source_uri: str
) -> tuple[str, Any, dict[str, Any]]:
    try:
        raw = ctx.fs.read(source_uri)
        mf = frontmatter.parse(raw)
        temporal = mf.frontmatter.get("temporal")
        return (
            str(mf.frontmatter.get("status", "ACTIVE")),
            mf.frontmatter.get("provenance", {}).get("confidence"),
            temporal if isinstance(temporal, dict) else {},
        )
    except (OSError, FrontmatterError):
        return ("ACTIVE", None, {})


def _annotate(
    status: str,
    confidence: Any,
    source_uri: str,
    *,
    level: str,
    temporal: dict[str, Any] | None = None,
) -> str:
    status_tag = f"[{status}]"
    if status == "LOW_CONFIDENCE" and confidence is not None:
        status_tag = f"[LOW_CONFIDENCE: {float(confidence):.2f}]"
    time_tags = []
    for key in ("asserted_at", "valid_from", "valid_until"):
        value = (temporal or {}).get(key)
        if value:
            time_tags.append(f"{key}: {str(value)[:64]}")
    time_suffix = f" ({'; '.join(time_tags)})" if time_tags else ""
    return f"{status_tag} (source: {source_uri}) (level: {level}){time_suffix}"


def _assemble_msc(
    *,
    session_context: str | None,
    ltm_blocks: list[str],
    user_query: str,
    frontier_context_window: int = 200_000,
) -> str:
    """Assemble MSC with the §4.4.2 token budget split.

    Regions (default shares):
      - system prompt + user query     up to 10%
      - session context                up to 30%
      - retrieved LTM                  up to 50%
      - slack (reserved for answer)    10%

    The caller supplies the frontier's context window; LTM and session context
    are truncated to fit. The order matters for attention allocation: session
    context → retrieved LTM → user query (repeated at the end for recency).
    """
    system_budget = int(frontier_context_window * 0.10)
    session_budget = int(frontier_context_window * 0.30)
    ltm_budget = int(frontier_context_window * 0.50)

    session_text = _fit_to_tokens(session_context or "", session_budget)
    ltm_text = _fit_blocks_to_tokens(ltm_blocks, ltm_budget)
    query_text = _fit_to_tokens(user_query, system_budget)

    parts = []
    if session_text:
        parts.append(f"## Session context\n{session_text}")
    parts.append(
        "## Retrieved memory\n" + (ltm_text or "(no relevant long-term memory found)")
    )
    parts.append(f"## User query\n{query_text}")
    return "\n\n".join(parts)


def _fit_to_tokens(text: str, token_budget: int) -> str:
    """Truncate `text` to `token_budget` tokens, preserving the tail.

    Uses the real tokenizer (tiktoken) when available so the MSC budget
    split (§4.4.2) matches what the frontier actually sees.
    """
    if not text:
        return text
    if tok_mod.count_tokens(text) <= token_budget:
        return text
    truncated = tok_mod.truncate_to_tokens(text, token_budget, from_end=True)
    if not truncated:
        return ""
    return f"[… truncated to {token_budget} tokens …]\n{truncated}"


def _fit_blocks_to_tokens(blocks: list[str], token_budget: int) -> str:
    if not blocks:
        return ""
    spent = 0
    out: list[str] = []
    for block in blocks:
        tokens = tok_mod.count_tokens(block)
        if spent + tokens > token_budget:
            break
        out.append(block)
        spent += tokens
    return "\n\n---\n\n".join(out)


def _est_tokens(text: str) -> int:
    """Real token count via tiktoken (fallback: char ratio with safety factor)."""
    return tok_mod.count_tokens(text)


# ----------------------------------------------------------------------
# Frontier re-entry loop
# ----------------------------------------------------------------------

def _answer_loop(
    ctx: OrchestratorContext,
    md: RetrievalMetadata,
    session_context: str | None,
    query: str,
    msc: str,
    max_reentries: int,
    *,
    max_depth: str,
    accumulated_hits: list[dict[str, Any]],
    on_step: Callable[[dict[str, Any]], None] | None = None,
) -> QueryResult:
    reentries = 0
    current_msc = msc
    hits = list(accumulated_hits)
    while True:
        t = time.perf_counter()
        allow_more = reentries < max_reentries
        try:
            verdict: FrontierVerdict = ctx.frontier.answer(
                system_prompt="You are the final-answer generator in Engram.",
                msc=current_msc,
                user_query=query,
                allow_need_more=allow_more,
            )
        except (CoreModelError, CircuitOpenError) as err:
            # Frontier unavailable — return a clear error response rather
            # than hanging or 500-ing. Still populate metadata so the caller
            # can distinguish "no answer" from "crashed".
            log.error("frontier call failed at reentry=%d: %s", reentries, err)
            md.reentries = reentries
            md.latency_ms[f"frontier_answer_{reentries}"] = (
                time.perf_counter() - t
            ) * 1000
            _record_frontier_metrics(ctx, None, outcome="ERROR")
            _trace_frontier_call(md, ctx, None, outcome="ERROR")
            _notify_step(on_step, md, "frontier_error")
            return QueryResult(
                answer=(
                    "The answer generator is temporarily unavailable. "
                    "Retrieval completed but no answer could be produced."
                ),
                retrieval_metadata=md,
            )
        md.latency_ms[f"frontier_answer_{reentries}"] = (time.perf_counter() - t) * 1000
        _record_frontier_metrics(ctx, verdict, outcome=verdict.verdict)
        _trace_frontier_call(md, ctx, verdict, outcome=verdict.verdict)
        if verdict.verdict == "ANSWER" or not allow_more:
            md.reentries = reentries
            _notify_step(on_step, md, "frontier_answer")
            return QueryResult(answer=verdict.answer or "(no answer produced)",
                                retrieval_metadata=md)
        reentries += 1
        md.reentries = reentries
        followups = verdict.suggested_queries or [query]
        if md.trace is not None:
            md.trace["reentry_requests"].append(
                {
                    "index": reentries,
                    "suggested_depth": verdict.suggested_depth,
                    "queries": [str(value)[:500] for value in followups[:3]],
                }
            )
        for q in followups[:3]:
            try:
                emb = ctx.embed.embed(q)
                rows = ctx.neo4j.vector_search(
                    emb,
                    k=min(ctx.cfg.retrieval.max_l1_vector_results, 10),
                    dormant_floor=ctx.cfg.decay.dormant_floor,
                )
            except Exception as err:
                log.warning("re-entry vector search failed for %r: %s", q, err)
                rows = []
            for r in rows:
                if any(h.get("source_uri") == r["source_uri"] for h in hits):
                    continue
                r["retrieval_level"] = "L1_reentry"
                hits.append(r)
        _trace_hits(ctx, md, hits)
        suggested_depth = verdict.suggested_depth or md.predicted_depth or max_depth
        hits = _run_reentry_cascade(
            ctx,
            md,
            session_context,
            query,
            hits,
            suggested_depth=suggested_depth,
            max_depth=max_depth,
            reentry_idx=reentries,
            on_step=on_step,
        )
        ltm_blocks = _format_ltm_blocks(
            ctx, hits, md.cascade_depth_reached, trace=md.trace
        )
        current_msc = _assemble_msc(
            session_context=session_context,
            ltm_blocks=ltm_blocks,
            user_query=query,
        )
        md.nodes_retrieved = len(hits)
        md.total_context_tokens = _est_tokens(current_msc)
        _trace_token_allocation(md, session_context, ltm_blocks, query)
        _notify_step(on_step, md, f"reentry_{reentries}")


def _run_reentry_cascade(
    ctx: OrchestratorContext,
    md: RetrievalMetadata,
    session_context: str | None,
    query: str,
    hits: list[dict[str, Any]],
    *,
    suggested_depth: str,
    max_depth: str,
    reentry_idx: int,
    on_step: Callable[[dict[str, Any]], None] | None,
) -> list[dict[str, Any]]:
    """Resume the Core planner/executor loop when Frontier says NEED_MORE."""
    target_depth = min(_depth_rank(suggested_depth), _depth_rank(max_depth))
    if target_depth <= _depth_rank(md.cascade_depth_reached):
        return hits

    current_results = hits
    previous_level = md.cascade_depth_reached
    if _depth_rank(previous_level) < _depth_rank("L1"):
        previous_level = "L1"
        if "L1" not in md.levels_visited:
            md.levels_visited.append("L1")
        md.cascade_depth_reached = "L1"

    for level_name in ("L2", "L3", "L4"):
        if _depth_rank(level_name) <= _depth_rank(previous_level):
            continue
        if _depth_rank(level_name) > target_depth:
            break
        t = time.perf_counter()
        with tracing.span(f"query.{level_name.lower()}_reentry_plan"):
            ln_plan = _ln_plan(
                ctx,
                level=level_name,
                query=query,
                session_context=session_context,
                previous_level=previous_level,
                previous_results=current_results,
                metadata=md,
            )
        md.latency_ms[f"{level_name.lower()}_reentry_{reentry_idx}_plan"] = (
            time.perf_counter() - t
        ) * 1000
        md.levels_visited.append(level_name)
        md.cascade_depth_reached = level_name
        _trace_plan(md, f"{level_name}_reentry_{reentry_idx}", ln_plan)
        _notify_step(on_step, md, f"{level_name.lower()}_reentry_plan")
        if ln_plan.get("terminate_cascade"):
            break
        t = time.perf_counter()
        with tracing.span(f"query.{level_name.lower()}_reentry_execute"):
            current_results = _execute_commands(
                ctx,
                ln_plan.get("commands", []),
                level=level_name,
                existing=current_results,
            )
        md.latency_ms[f"{level_name.lower()}_reentry_{reentry_idx}_execute"] = (
            time.perf_counter() - t
        ) * 1000
        md.nodes_retrieved = len(current_results)
        _trace_hits(ctx, md, current_results)
        _notify_step(on_step, md, f"{level_name.lower()}_reentry_execute")
        previous_level = level_name
    return current_results


def _trace_plan(md: RetrievalMetadata, level: str, plan: dict[str, Any]) -> None:
    if md.trace is None:
        return
    if level == "L1":
        md.trace["vector_queries"] = [
            str(value)[:500] for value in (plan.get("vector_queries") or [])[:3]
        ]
    md.trace["sufficiency_decisions"].append(
        {
            "level": level,
            "previous_level_sufficient": plan.get("previous_level_sufficient"),
            "session_sufficient": plan.get("session_sufficient"),
            "terminate_cascade": plan.get("terminate_cascade"),
            "predicted_depth": plan.get("predicted_depth"),
        }
    )
    for command in (plan.get("commands") or [])[:20]:
        if not isinstance(command, dict):
            continue
        name = command.get("template") or command.get("command")
        params = command.get("params") or {
            key: value
            for key, value in command.items()
            if key not in {"template", "command"}
        }
        md.trace["commands"].append(
            {
                "level": level,
                "name": str(name)[:100],
                "params": _bounded_trace_value(params),
            }
        )


def _trace_core_call(
    metadata: RetrievalMetadata | None,
    ctx: OrchestratorContext,
    task: str,
    result: CompletionResult,
) -> None:
    if metadata is None or metadata.trace is None:
        return
    calls = metadata.trace["model_calls"]
    if len(calls) >= 50:
        return
    calls.append({
        "family": "core",
        "task": task,
        "provider": ctx.cfg.core_model.provider,
        "model": ctx.cfg.core_model.model_path,
        "provider_calls": max(1, result.provider_calls),
        "tokens_in": result.tokens_in,
        "tokens_out": result.tokens_out,
        "latency_ms": result.latency_ms,
        "outcome": "COMPLETE",
    })


def _record_frontier_metrics(
    ctx: OrchestratorContext,
    verdict: FrontierVerdict | None,
    *,
    outcome: str,
) -> None:
    provider = ctx.cfg.frontier_llm.provider
    model = ctx.cfg.frontier_llm.model_path
    metrics_mod.frontier_calls.labels(
        provider=provider,
        model=model,
        outcome=outcome,
    ).inc(max(1, verdict.provider_calls) if verdict is not None else 1)
    if verdict is None:
        return
    if verdict.tokens_in is not None:
        metrics_mod.frontier_tokens.labels(
            provider=provider,
            model=model,
            direction="in",
        ).inc(verdict.tokens_in)
    if verdict.tokens_out is not None:
        metrics_mod.frontier_tokens.labels(
            provider=provider,
            model=model,
            direction="out",
        ).inc(verdict.tokens_out)
    if verdict.latency_ms is not None:
        metrics_mod.frontier_latency.labels(
            provider=provider,
            model=model,
        ).observe(verdict.latency_ms / 1000.0)


def _trace_frontier_call(
    metadata: RetrievalMetadata,
    ctx: OrchestratorContext,
    verdict: FrontierVerdict | None,
    *,
    outcome: str,
) -> None:
    if metadata.trace is None:
        return
    calls = metadata.trace["model_calls"]
    if len(calls) >= 50:
        return
    calls.append({
        "family": "frontier",
        "task": "answer",
        "provider": ctx.cfg.frontier_llm.provider,
        "model": ctx.cfg.frontier_llm.model_path,
        "provider_calls": max(1, verdict.provider_calls) if verdict is not None else 1,
        "tokens_in": verdict.tokens_in if verdict is not None else None,
        "tokens_out": verdict.tokens_out if verdict is not None else None,
        "latency_ms": verdict.latency_ms if verdict is not None else None,
        "outcome": outcome,
    })


def _trace_hits(
    ctx: OrchestratorContext,
    md: RetrievalMetadata,
    hits: list[dict[str, Any]],
) -> None:
    if md.trace is None:
        return
    existing = {row["source_uri"] for row in md.trace["hits"]}
    for hit in hits:
        uri = hit.get("source_uri")
        if not uri or uri in existing or len(md.trace["hits"]) >= 100:
            continue
        existing.add(uri)
        md.trace["hits"].append(
            {
                "source_uri": str(uri),
                "score": hit.get("score"),
                "retrieval_level": hit.get("retrieval_level"),
                "source_turn_ids": _source_turn_ids_for(ctx, hit),
            }
        )


def _source_turn_ids_for(
    ctx: OrchestratorContext,
    hit: dict[str, Any],
) -> list[str]:
    supplied = hit.get("source_turn_ids") or []
    if supplied:
        return [str(value) for value in supplied if value]
    uri = hit.get("source_uri")
    if not uri:
        return []
    try:
        fm = frontmatter.parse(ctx.fs.read(str(uri))).frontmatter
    except (OSError, FrontmatterError):
        return []
    values = fm.get("source_turn_ids") or fm.get("provenance", {}).get(
        "source_turn_ids", []
    )
    return [str(value) for value in values if value]


def _trace_token_allocation(
    md: RetrievalMetadata,
    session_context: str | None,
    ltm_blocks: list[str],
    query: str,
) -> None:
    if md.trace is None:
        return
    md.trace["token_allocation"] = {
        "session_context": _est_tokens(session_context or ""),
        "retrieved_memory": sum(_est_tokens(block) for block in ltm_blocks),
        "user_query": _est_tokens(query),
        "total_context": md.total_context_tokens,
    }


def _bounded_trace_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key)[:100]: _bounded_trace_value(item)
            for key, item in list(value.items())[:20]
        }
    if isinstance(value, list):
        return [_bounded_trace_value(item) for item in value[:20]]
    if isinstance(value, str):
        return value[:500]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)[:500]
