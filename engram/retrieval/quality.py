"""Opt-in requirement-controlled cascade using the native retrieval adapters."""

from __future__ import annotations

import time

from engram.models.budget import BudgetExceededError, check_deadline
from engram.models.usage import QueryTokenUsage
from engram.retrieval.cascade import _stage, _try_conversation_answer, parametric_allowed
from engram.retrieval.domain import AnswerabilityState, EvidenceAssessment
from engram.retrieval.evidence import EvidenceGate
from engram.retrieval.frontier_tools import plan_next_tool
from engram.retrieval.planner import AdaptivePlanner
from engram.retrieval.sufficiency import POLICY_VERSION, SufficiencyController, canonical_items
from engram.retrieval.tools import ConversationContext, MemoryTools
from engram.tenancy import current_tenant_id


def run_quality_cascade(ctx, *, session_id, query, session_context, max_depth, on_step):
    from engram.retrieval.orchestrator import (
        AmbiguousEntityReferenceError,
        CanonicalRepositoryUnavailableError,
        QueryResult,
        RetrievalMetadata,
        _dedupe_verified,
        _notify_step,
    )

    started = time.perf_counter()
    md = RetrievalMetadata(
        quality_policy=POLICY_VERSION,
        token_usage=QueryTokenUsage.from_request(query, session_context),
    )
    plan = (ctx.planner or AdaptivePlanner()).plan(
        query, session_context=session_context, core=None
    )
    tenant = current_tenant_id()
    controller = SufficiencyController(ctx, md, plan, tenant)
    tools = MemoryTools(ctx, tenant_id=tenant, session_id=session_id)
    cap = {"L0": 0, "SESSION": 1, "L1": 1, "L2": 2, "L3": 3, "L4": 3}[
        str(max_depth or ctx.cfg.retrieval.max_depth).upper()
    ]
    all_items, verified = [], []
    decision = None
    context_text = ""
    md.temporal_scope = plan.temporal_scope.model_dump(mode="json")
    assessment = EvidenceAssessment(
        state=AnswerabilityState.INSUFFICIENT_EVIDENCE,
        missing_evidence=["complete question support"],
    )

    def finish(answer, state, reason):
        md.answerability_state, md.stop_reason = state, reason
        md.answer_mode = (
            "supported-quality"
            if state in {"ANSWERABLE", "PARTIALLY_ANSWERABLE"}
            else "evidence-terminal"
        )
        if decision and decision.basis == "deterministic-exact" and state == "ANSWERABLE":
            md.answer_mode = "deterministic-canonical"
        md.latency_ms["total"] = (time.perf_counter() - started) * 1000
        md.verified_evidence = len(verified)
        md.nodes_retrieved = len(verified)
        _notify_step(on_step, md, "answer")
        return QueryResult(answer, md, state, verified)

    _stage(md, "L0", on_step)
    allowed = parametric_allowed(query, plan) and not session_context and not session_id
    md.l0_decision = "TRY_PARAMETRIC" if allowed and not ctx.cfg.retrieval.l0_skip else "CONTINUE"
    if md.l0_decision == "TRY_PARAMETRIC":
        answer = _try_conversation_answer(
            ctx,
            md,
            query=query,
            context=ConversationContext(),
            stage="L0",
            general_allowed=True,
            on_step=on_step,
        )
        if answer:
            return answer

    stages = []
    if cap >= 1:
        stages.append(
            ("L1", "current_session_search", lambda: tools.current_session_search(session_context))
        )
        if session_id and ctx.control_plane is not None:
            stages.append(
                (
                    "L1",
                    "current_session_search_postgresql",
                    lambda: tools.current_session_originals(plan),
                )
            )
    if cap >= 2:
        stages.append(("L2", "past_session_search", lambda: tools.past_session_search(plan)))
    if cap >= 3 and ctx.repository() is not None:
        stages.extend(
            (level, name, None)
            for level, name in (
                ("L3_FACTS", "facts_search"),
                ("L3_VECTOR", "semantic_memory_search"),
                ("L3_GRAPH", "graph_search"),
            )
        )
    for level, name, call in stages:
        try:
            check_deadline()
        except BudgetExceededError:
            return finish(
                "The response deadline was reached before enough supporting information could be checked.",
                "INSUFFICIENT_EVIDENCE",
                "query-deadline",
            )
        if not md.levels_visited or md.levels_visited[-1] != level:
            _stage(md, level, on_step)
        if name in {
            "past_session_search",
            "facts_search",
            "semantic_memory_search",
            "graph_search",
        }:
            plan = plan_next_tool(
                ctx, md, plan, name, session_context=context_text, assessment=assessment
            )
        trace = {"name": name, "status": "ok"}
        md.tool_calls.append(trace)
        stamp = time.perf_counter()
        try:
            result = call() if call else getattr(tools, name)(plan)
            if isinstance(result, ConversationContext):
                all_items.extend(result.items)
                context_text += "\n" + result.text
                md.source_records.extend(result.sources)
                md.degraded_sources.extend(result.degraded)
                trace["records"] = len(result.sources)
            else:
                rows, count = result
                verified = _dedupe_verified([*verified, *rows])
                all_items.extend(canonical_items(rows, tenant))
                md.candidates_discovered += count
                trace["records"] = len(rows)
                if name in {"facts_search", "semantic_memory_search"}:
                    trace.update(tools.diagnostics)
                # Retain deterministic conflict detection outside model control.
                checked = (ctx.evidence_gate or EvidenceGate()).assess(plan, verified)
                if checked.state == AnswerabilityState.CONFLICTING_EVIDENCE:
                    md.conflicts = checked.conflicts
                    return finish(
                        "The stored sources conflict, so I cannot give one unqualified answer.",
                        "CONFLICTING_EVIDENCE",
                        "conflicting-canonical-evidence",
                    )
        except AmbiguousEntityReferenceError as error:
            return finish(
                f"I found multiple people or entities named {error.alias}. Please provide more identifying context.",
                "INSUFFICIENT_EVIDENCE",
                "ambiguous-entity-reference",
            )
        except (AttributeError, TypeError, KeyError, AssertionError):
            # Engine defects must surface as failed evaluations, rather than
            # masquerading as an ordinary unavailable search dependency.
            raise
        except Exception as error:
            trace.update(status="unavailable", error=type(error).__name__)
            md.degraded_sources.append(name)
            if isinstance(error, CanonicalRepositoryUnavailableError):
                break
            continue
        finally:
            md.latency_ms[name] = (time.perf_counter() - stamp) * 1000
        controller.prepare()
        decision = controller.assess(all_items, level)
        assessment = EvidenceAssessment(
            state=AnswerabilityState(decision.state),
            verified_evidence=verified,
            missing_evidence=[
                r.requirement_id for r in decision.requirements if r.status != "SUPPORTED"
            ],
            reason=decision.reason,
        )
        md.missing_evidence = assessment.missing_evidence
        _notify_step(on_step, md, "sufficiency")
        if decision.state == "CONFLICTING_EVIDENCE":
            return finish(
                "The supporting sources conflict. I cannot resolve them into one reliable answer.",
                decision.state,
                "conflicting-source-evidence",
            )
        if answer := controller.answer(decision):
            return finish(answer, decision.state, "sufficient-at-" + level)
    if decision and (answer := controller.answer(decision, terminal=True)):
        return finish(answer, decision.state, "supported-at-exhaustion")
    if md.degraded_sources:
        return finish(
            "Some retrieval or answer services were unavailable. I could not verify a complete answer.",
            "INSUFFICIENT_EVIDENCE",
            "dependency-unavailable",
        )
    return finish(
        "I don't have enough verified information to answer the complete question.",
        "INSUFFICIENT_EVIDENCE",
        "max-depth-reached" if cap < 3 else "cascade-exhausted",
    )
