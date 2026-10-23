"""Previously supported facts must survive competing discovery when they fit."""

from unittest.mock import Mock

import pytest

from engram import tokens
from engram.models.core import CompletionResult
from engram.retrieval.domain import RetrievalPlan
from engram.retrieval.orchestrator import RetrievalMetadata
from engram.retrieval.sufficiency import (
    EvidenceItem,
    SufficiencyController,
    evidence_identity,
    pack_items,
    render_items,
)
from tests.unit.test_adaptive_retrieval import DirectRepository, RecordingFrontier, _context


def setup():
    ctx = _context(DirectRepository(), RecordingFrontier())
    ctx.cfg.retrieval.quality_mode = "enforce"
    control = SufficiencyController(
        ctx, RetrievalMetadata(),
        RetrievalPlan(query="What activities does Alice enjoy?"), "_default",
    )
    fact = EvidenceItem(
        id="source", text="Alice: We love hiking in the mountains.",
        source_kind="conversation_record", tenant_id="_default", role="user", revision=1,
    )
    noise = fact.model_copy(update={"id": "noise", "text": "Alice activities enjoy " * 8})
    ctx.cfg.retrieval.session_budget_tokens = (
        max(tokens.count_tokens(render_items([x])) for x in (fact, noise)) + 5
    )
    return control, fact, noise


def review(status, fact):
    return CompletionResult(
        output={"requirements": [
            {"requirement_id": requirement, "status": status,
             "citations": [{"evidence_id": fact.id, "quote": fact.text}]}
            for requirement in ("documented_items", "question")
        ]}, raw_text="{}", tokens_in=10, tokens_out=5,
    )


def test_later_search_keeps_supported_evidence_and_rechecks_new_packed_context():
    control, fact, noise = setup()
    control.ctx.core = Mock(complete=Mock(return_value=review("SUPPORTED", fact)))
    first = control.assess([fact], "L3_FACTS")
    assert first.state == "PARTIALLY_ANSWERABLE"
    old, _ = pack_items(
        [fact, noise], control.plan.query, control.ctx.cfg.retrieval.session_budget_tokens
    )
    assert [item.id for item in old] == ["noise"]
    second = control.assess([fact, noise], "L3_VECTOR")
    assert [item.id for item in control.items] == ["source"]
    assert second == first
    assert control.ctx.core.complete.call_count == 1
    control.ctx.cfg.retrieval.session_budget_tokens = 1000
    control.ctx.core.complete.return_value = review("CONTRADICTED", fact)
    assert control.assess([fact, noise], "L3_GRAPH").state == "CONFLICTING_EVIDENCE"
    assert control.ctx.core.complete.call_count == 2


@pytest.mark.parametrize(
    "change", [{"revision": 2}, {"text": "Alice never said that."}, {"tenant_id": "other"}]
)
def test_priority_does_not_transfer_to_changed_evidence(change):
    control, fact, noise = setup()
    changed = fact.model_copy(update=change)
    assert evidence_identity(changed) != evidence_identity(fact)
    chosen, _ = pack_items(
        [changed, noise], control.plan.query, control.ctx.cfg.retrieval.session_budget_tokens,
        supported={evidence_identity(fact): {"question"}},
    )
    assert [item.id for item in chosen] == ["noise"]


def test_supported_fact_cannot_break_a_new_graph_bundle_to_fit():
    control, fact, noise = setup()
    edge = fact.model_copy(update={"bundles": ["path"]})
    other = noise.model_copy(update={"bundles": ["path"], "text": "long " * 500})
    chosen, omitted = pack_items(
        [edge, other], control.plan.query, 200, supported={evidence_identity(fact): {"question"}}
    )
    assert not chosen
    assert {row["id"] for row in omitted} == {"source", "noise"}


def test_invalid_citation_never_acquires_retention_priority():
    control, fact, _ = setup()
    fabricated = fact.model_copy(update={"text": "fabricated quote"})
    control.ctx.core = Mock(complete=Mock(return_value=review("SUPPORTED", fabricated)))
    assert control.assess([fact], "L1").state == "INSUFFICIENT_EVIDENCE"
    assert control.supported_items == {}


def test_documented_items_can_be_supported_without_proving_an_exhaustive_list():
    control, fact, _ = setup()
    response = review("SUPPORTED", fact)
    response.output["requirements"][1].update(status="UNKNOWN", citations=[])
    control.ctx.core = Mock(complete=Mock(return_value=response))
    decision = control.assess([fact], "L2")
    assert decision.state == "PARTIALLY_ANSWERABLE"
    assert decision.requirements[0].status == "SUPPORTED"
    assert decision.requirements[1].status == "UNKNOWN"
    assert control.answer(decision, terminal=False) is None


def test_learning_priority_does_not_reorder_unchanged_inputs_and_repeat_assessment():
    control, fact, noise = setup()
    control.ctx.cfg.retrieval.session_budget_tokens = 1000
    control.ctx.core = Mock(complete=Mock(return_value=review("SUPPORTED", fact)))
    first = control.assess([noise, fact], "L3_FACTS")
    assert [item.id for item in control.items] == ["noise", "source"]
    assert control.assess([noise, fact, fact], "L3_VECTOR") == first
    assert control.ctx.core.complete.call_count == 1
