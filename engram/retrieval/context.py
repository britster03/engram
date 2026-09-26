"""Deterministic model-context construction from verified evidence only."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from engram import tokens as tok_mod
from engram.retrieval.domain import EvidenceAssessment, RetrievalPlan, VerifiedEvidence


class ContextBuilder:
    """Render compact, explicit evidence blocks without filesystem access."""

    def __init__(
        self,
        *,
        context_window: int = 200_000,
        session_share: float = 0.30,
        evidence_share: float = 0.50,
    ) -> None:
        self.context_window = max(1, context_window)
        self.session_share = max(0.0, min(session_share, 1.0))
        self.evidence_share = max(0.0, min(evidence_share, 1.0))

    def build(
        self,
        plan: RetrievalPlan,
        assessment: EvidenceAssessment,
        *,
        session_context: str | None = None,
        user_query: str | None = None,
        context_window: int | None = None,
    ) -> str:
        """Build MSC from canonical evidence and explicit uncertainty labels."""

        window = max(1, context_window or self.context_window)
        session_budget = int(window * self.session_share)
        evidence_budget = int(window * self.evidence_share)
        session = _fit_to_tokens(session_context or "", session_budget)
        evidence = self._evidence_text(plan, assessment, evidence_budget)
        assessment_text = self._assessment_text(plan, assessment)
        query = _fit_to_tokens(user_query or plan.query, int(window * 0.10))
        parts = [assessment_text, "## Verified canonical memory\n" + (evidence or "(none)")]
        if session:
            parts.insert(0, f"## Session context (not canonical memory)\n{session}")
        parts.append(f"## User query\n{query}")
        return "\n\n".join(parts)

    def _assessment_text(self, plan: RetrievalPlan, assessment: EvidenceAssessment) -> str:
        lines = [
            "## Retrieval assessment",
            f"answerability_state: {assessment.state.value}",
            f"intent: {plan.intent.value}",
            f"primary_route: {plan.primary_route.value}",
            f"temporal_scope: {_temporal_text(plan)}",
            f"reason: {assessment.reason or plan.reason}",
        ]
        if assessment.missing_evidence:
            lines.append("missing_evidence: " + ", ".join(assessment.missing_evidence))
        if assessment.conflicts:
            lines.append("conflicts: " + ", ".join(assessment.conflicts))
        if assessment.can_escalate and assessment.next_route is not None:
            lines.append(f"recommended_escalation: {assessment.next_route.value}")
        return "\n".join(lines)

    def _evidence_text(
        self,
        plan: RetrievalPlan,
        assessment: EvidenceAssessment,
        budget: int,
    ) -> str:
        blocks: list[str] = []
        spent = 0
        for evidence in assessment.verified_evidence:
            block = _render_evidence(
                evidence,
                historical=plan.temporal_scope.kind.value == "history",
                include_provenance=plan.requires_evidence,
            )
            block_tokens = tok_mod.count_tokens(block)
            remaining = budget - spent
            if remaining <= 0:
                break
            if block_tokens > remaining:
                truncated = _fit_to_tokens(block, remaining)
                if truncated:
                    blocks.append(truncated)
                break
            blocks.append(block)
            spent += block_tokens
        return "\n\n---\n\n".join(blocks)


def build_context(
    plan: RetrievalPlan,
    assessment: EvidenceAssessment,
    *,
    session_context: str | None = None,
    user_query: str | None = None,
    context_window: int = 200_000,
) -> str:
    """Functional convenience wrapper for callers/tests."""

    return ContextBuilder(context_window=context_window).build(
        plan,
        assessment,
        session_context=session_context,
        user_query=user_query,
    )


def _render_evidence(
    evidence: VerifiedEvidence,
    *,
    historical: bool,
    include_provenance: bool,
) -> str:
    status = (
        "HISTORICAL" if historical or evidence.status not in {"ACTIVE", "CURRENT"} else "CURRENT"
    )
    lines = [
        f"[{status}] canonical=true memory_id={evidence.memory_id or '(none)'} "
        f"claim_id={evidence.claim_id or '(none)'}",
    ]
    if evidence.subject_id:
        subject = evidence.subject_name or evidence.subject_id
        lines.append(f"subject: {subject}")
    if evidence.predicate:
        lines.append(f"predicate: {evidence.predicate}")
    if evidence.object_entity_id:
        object_entity = evidence.object_entity_name or evidence.object_entity_id
        lines.append(f"object_entity: {object_entity}")
    if evidence.object_value is not None:
        lines.append(f"object_value: {_stringify(evidence.object_value)}")
    if evidence.object_type:
        lines.append(f"object_type: {evidence.object_type}")
    if evidence.content:
        lines.append(f"content: {evidence.content.strip()}")
    if evidence.valid_from or evidence.valid_until:
        lines.append(
            "validity: "
            f"{_datetime_text(evidence.valid_from) or '-'}"
            f"..{_datetime_text(evidence.valid_until) or 'open'}"
        )
    if evidence.asserted_at:
        lines.append(f"asserted_at: {_datetime_text(evidence.asserted_at)}")
    if evidence.claim_confidence is not None:
        lines.append(f"claim_confidence: {evidence.claim_confidence:.3f}")
    if evidence.retrieval_score is not None:
        lines.append(f"retrieval_score: {evidence.retrieval_score:.3f}")
    if evidence.canonical_revision is not None:
        lines.append(f"canonical_revision: {evidence.canonical_revision}")
    if include_provenance and evidence.evidence_ids:
        lines.append("evidence_ids: " + ", ".join(evidence.evidence_ids))
    if include_provenance and evidence.provenance is not None:
        lines.append(f"provenance: {_stringify(evidence.provenance)}")
    return "\n".join(lines)


def _temporal_text(plan: RetrievalPlan) -> str:
    scope = plan.temporal_scope
    fields = [scope.kind.value]
    if scope.as_of:
        fields.append(f"as_of={_datetime_text(scope.as_of)}")
    if scope.since:
        fields.append(f"since={_datetime_text(scope.since)}")
    if scope.until:
        fields.append(f"until={_datetime_text(scope.until)}")
    return ", ".join(fields)


def _datetime_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _stringify(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return repr(value)
    return str(value)


def _fit_to_tokens(text: str, budget: int) -> str:
    if not text or budget <= 0:
        return ""
    if tok_mod.count_tokens(text) <= budget:
        return text
    truncated = tok_mod.truncate_to_tokens(text, budget, from_end=True)
    return f"[… truncated to {budget} tokens …]\n{truncated}" if truncated else ""


__all__ = ["ContextBuilder", "build_context"]
