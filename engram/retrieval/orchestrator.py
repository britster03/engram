"""Retrieval Orchestrator — full L0→L4 cascade with fused plan-judge (§4).

Pipeline:
  L0 gate (optional)  →  L1 plan + vector search  →  Ln plan-with-judge
    → Ln execute  →  (continue or terminate)  →  MSC assembly  →  Frontier
    → Frontier re-entry on NEED_MORE up to `max_reentries`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from engram import frontmatter, prompts, tracing
from engram import tokens as tok_mod
from engram.config import EngramConfig
from engram.frontmatter import FrontmatterError
from engram.models.core import CoreModelError, CoreModelProvider
from engram.models.embeddings import EmbeddingService
from engram.models.frontier import FrontierLLMProvider, FrontierVerdict
from engram.resilience import CircuitOpenError
from engram.retrieval.l0_gate import AlwaysClass0Classifier, L0Classifier, run_l0_gate
from engram.retrieval.rerank import NullReranker, Reranker
from engram.retrieval.templates import TemplateError, run_template
from engram.retrieval.tree_render import render_tree
from engram.storage.filesystem import FilesystemStore

log = logging.getLogger(__name__)


_DEPTH_ORDER = ["SESSION", "L0", "L1", "L2", "L3", "L4"]


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
    cascade_depth_reached: str = "L0"
    levels_visited: list[str] = field(default_factory=list)
    predicted_depth: str | None = None
    nodes_retrieved: int = 0
    total_context_tokens: int = 0
    reentries: int = 0
    latency_ms: dict[str, float] = field(default_factory=dict)
    l0_decision: str | None = None
    l0_reason: str | None = None
    forced_answer: bool = False  # final answer came from the best-effort fallback
    reranked_from: int | None = None  # candidates scored before the precision pass

    def to_dict(self) -> dict[str, Any]:
        return {
            "cascade_depth_reached": self.cascade_depth_reached,
            "levels_visited": self.levels_visited,
            "predicted_depth": self.predicted_depth,
            "nodes_retrieved": self.nodes_retrieved,
            "total_context_tokens": self.total_context_tokens,
            "reentries": self.reentries,
            "latency_ms": self.latency_ms,
            "l0_decision": self.l0_decision,
            "l0_reason": self.l0_reason,
            "forced_answer": self.forced_answer,
            "reranked_from": self.reranked_from,
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
    reranker: Any = None

    def get_reranker(self) -> Any:
        """Lazily build the reranker from config; cached on the context."""
        if self.reranker is None:
            rc = self.cfg.retrieval
            self.reranker = (
                Reranker(model_name=rc.rerank_model, device=rc.rerank_device)
                if getattr(rc, "rerank_enabled", False)
                else NullReranker()
            )
        return self.reranker


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
    max_reentries: int | None = None,
    on_step: Callable[[dict[str, Any]], None] | None = None,
) -> QueryResult:
    md = RetrievalMetadata()
    max_depth = max_depth or ctx.cfg.retrieval.max_depth
    max_reentries = max_reentries if max_reentries is not None else ctx.cfg.retrieval.max_reentries

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
                skip=ctx.cfg.retrieval.l0_skip,
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
        )
    md.latency_ms["l1_plan"] = (time.perf_counter() - t) * 1000
    md.levels_visited.append("L1")
    md.cascade_depth_reached = "L1"
    md.predicted_depth = plan.get("predicted_depth", "L4")
    _notify_step(on_step, md, "l1_plan")

    if plan.get("session_sufficient") and plan.get("session_answer_context"):
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
    _notify_step(on_step, md, "l1_execute")

    predicted_depth = plan.get("predicted_depth", "L4")
    target_depth = _depth_rank(predicted_depth)
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
            )
        md.latency_ms[f"{level_name.lower()}_plan"] = (time.perf_counter() - t) * 1000
        md.levels_visited.append(level_name)
        md.cascade_depth_reached = level_name
        _notify_step(on_step, md, f"{level_name.lower()}_plan")
        if ln_plan.get("terminate_cascade"):
            break
        t = time.perf_counter()
        with tracing.span(f"query.{level_name.lower()}_execute"):
            current_results = _execute_commands(
                ctx, ln_plan.get("commands", []),
                level=level_name, existing=current_results,
            )
        md.latency_ms[f"{level_name.lower()}_execute"] = (time.perf_counter() - t) * 1000
        md.nodes_retrieved = len(current_results)
        _notify_step(on_step, md, f"{level_name.lower()}_execute")
        prev_level = level_name

    # --- MSC assembly --------------------------------------------------------
    t = time.perf_counter()
    current_results = _rerank_hits(ctx, md, query, current_results)
    md.nodes_retrieved = len(current_results)
    ltm_blocks = _format_ltm_blocks(ctx, current_results, md.cascade_depth_reached)
    md.latency_ms["msc_assembly"] = (time.perf_counter() - t) * 1000
    msc = _assemble_msc(session_context=session_context, ltm_blocks=ltm_blocks, user_query=query)
    md.total_context_tokens = _est_tokens(msc)
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


# ----------------------------------------------------------------------
# Planning
# ----------------------------------------------------------------------

def _l1_plan(
    ctx: OrchestratorContext,
    *,
    query: str,
    session_context: str | None,
    memory_hit: dict | None,
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
        result = ctx.core.complete(
            system_prompt=prompt,
            user_prompt="Return the plan JSON.",
        )
        out = result.output if isinstance(result.output, dict) else {}
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
        result = ctx.core.complete(
            system_prompt=prompt,
            user_prompt="Return the fused plan-judge JSON.",
        )
        out = result.output if isinstance(result.output, dict) else {}
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
    out = list(existing)
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
            uri = params.get("uri")
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
            uri = params.get("uri")
            if uri and ctx.fs.exists(uri) and uri not in seen:
                body = _read_full_body(ctx, uri) or ""
                seen.add(uri)
                out.append({
                    "source_uri": uri,
                    "l0_abstract": body.splitlines()[0][:500] if body else "",
                    "retrieval_level": "L4",
                })
            continue

        # --- Vector search ----------------------------------------------
        if template in ("find", "t_top_k_vector"):
            query_text = params.get("query")
            k = int(params.get("k", 10))
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

        try:
            rows = run_template(ctx.neo4j, template, params, timeout_s=timeout)
        except TemplateError:
            log.warning("orchestrator received unknown template: %s", template)
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


def _rerank_hits(
    ctx: OrchestratorContext,
    md: RetrievalMetadata,
    query: str,
    hits: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Precision pass: score candidates against the query and keep the best.

    Runs after the cascade rather than inside a level, so hits from L1 vector
    search, L2 traversal and L3/L4 reads compete on one ranking instead of
    being concatenated in the order they happened to be found.
    """
    rc = ctx.cfg.retrieval
    if not hits:
        return hits
    candidates = hits[: getattr(rc, "rerank_candidates", 40)]
    top_k = getattr(rc, "rerank_top_k", 8)
    try:
        ordered = ctx.get_reranker().rerank(query, candidates, top_k=top_k)
    except Exception as err:  # never fail a query because ranking failed
        log.warning("rerank pass failed: %s", err)
        return hits[:top_k]
    md.reranked_from = len(candidates)
    return ordered


def _format_ltm_blocks(
    ctx: OrchestratorContext, results: list[dict[str, Any]], cascade_depth: str
) -> list[str]:
    """Build LTM context blocks. At deeper levels we load richer content."""
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
        if r.get("overview"):
            body = r["overview"]
            level = "L3"
        elif cascade_depth in ("L4",):
            body = _read_full_body(ctx, source_uri)
            if body is None:
                continue
            level = "L4"
        else:
            body = r.get("l0_abstract") or ""
            level = r.get("retrieval_level", cascade_depth)
        status, confidence = _status_for(ctx, source_uri)
        if status == "HISTORICAL":
            continue
        tokens = _est_tokens(body)
        if spent + tokens > budget:
            continue
        annotation = _annotate(status, confidence, source_uri, level=level)
        blocks.append(f"{annotation}\n{body.strip()}")
        spent += tokens
    return blocks


def _read_full_body(ctx: OrchestratorContext, source_uri: str) -> str | None:
    try:
        raw = ctx.fs.read(source_uri)
        mf = frontmatter.parse(raw)
        return mf.body
    except (OSError, FrontmatterError):
        return None


def _status_for(ctx: OrchestratorContext, source_uri: str) -> tuple[str, Any]:
    try:
        raw = ctx.fs.read(source_uri)
        mf = frontmatter.parse(raw)
        return (
            str(mf.frontmatter.get("status", "ACTIVE")),
            mf.frontmatter.get("provenance", {}).get("confidence"),
        )
    except (OSError, FrontmatterError):
        return ("ACTIVE", None)


def _annotate(status: str, confidence: Any, source_uri: str, *, level: str) -> str:
    status_tag = f"[{status}]"
    if status == "LOW_CONFIDENCE" and confidence is not None:
        status_tag = f"[LOW_CONFIDENCE: {float(confidence):.2f}]"
    return f"{status_tag} (source: {source_uri}) (level: {level})"


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
            _notify_step(on_step, md, "frontier_error")
            return QueryResult(
                answer=(
                    "The answer generator is temporarily unavailable. "
                    "Retrieval completed but no answer could be produced."
                ),
                retrieval_metadata=md,
            )
        md.latency_ms[f"frontier_answer_{reentries}"] = (time.perf_counter() - t) * 1000
        if verdict.verdict == "ANSWER" or not allow_more:
            md.reentries = reentries
            answer = (verdict.answer or "").strip()
            if not answer:
                # The re-entry budget is spent but the model still emitted
                # NEED_MORE (or an empty ANSWER), so it never wrote a reply.
                # Returning a placeholder wastes the retrieval we just did and
                # denies the caller even a partial answer, so force one.
                answer = _force_best_effort(ctx, current_msc, query, verdict)
                md.forced_answer = True
            _notify_step(on_step, md, "frontier_answer")
            return QueryResult(answer=answer, retrieval_metadata=md)
        reentries += 1
        md.reentries = reentries
        followups = verdict.suggested_queries or [query]
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
        hits = _rerank_hits(ctx, md, query, hits)
        ltm_blocks = _format_ltm_blocks(ctx, hits, md.cascade_depth_reached)
        current_msc = _assemble_msc(
            session_context=session_context,
            ltm_blocks=ltm_blocks,
            user_query=query,
        )
        md.nodes_retrieved = len(hits)
        md.total_context_tokens = _est_tokens(current_msc)
        _notify_step(on_step, md, f"reentry_{reentries}")


def _force_best_effort(
    ctx: OrchestratorContext,
    msc: str,
    query: str,
    verdict: FrontierVerdict,
) -> str:
    """Produce an answer when the frontier's final verdict carried no text.

    `answer()` asks for a JSON verdict, and a model can satisfy that schema with
    NEED_MORE and an empty `answer` even when told it is the final call — so the
    JSON contract itself is the escape hatch. `stream_answer()` asks for plain
    prose instead, removing the option of declining via a verdict field.

    Falls back to the model's own stated `reason` (an honest "why I can't
    answer" beats a placeholder), then to a fixed message.
    """
    try:
        chunks = ctx.frontier.stream_answer(
            system_prompt=(
                "You are the final-answer generator in Engram. Answer the user's "
                "question using the retrieved memory. If the memory is "
                "incomplete, give your best supported answer and say what is "
                "uncertain. Never reply with an empty response."
            ),
            msc=msc,
            user_query=query,
        )
        forced = "".join(chunks).strip()
        if forced:
            return forced
    except Exception as err:  # provider/transport failure — fall through
        log.warning("forced best-effort answer failed: %s", err)

    reason = (verdict.reason or "").strip()
    if reason:
        return reason
    return "No answer could be produced from the retrieved memory."


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
            )
        md.latency_ms[f"{level_name.lower()}_reentry_{reentry_idx}_plan"] = (
            time.perf_counter() - t
        ) * 1000
        md.levels_visited.append(level_name)
        md.cascade_depth_reached = level_name
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
        _notify_step(on_step, md, f"{level_name.lower()}_reentry_execute")
        previous_level = level_name
    return current_results
