"""List scope and documented support are separate, without domain-specific rules."""

from engram.retrieval.domain import RetrievalPlan
from engram.retrieval.sufficiency import (
    AnswerRequirement,
    EvidenceItem,
    pack_items,
    render_items,
    requirements_for,
)
from engram.tokens import count_tokens_exact


def test_enumeration_has_documented_support_and_complete_question_requirements():
    requirements = requirements_for(
        RetrievalPlan(query="List Riley's activities.", entity_hints=["Riley"])
    )
    assert {r.id for r in requirements} == {"documented_items", "question"}
    assert next(r for r in requirements if r.id == "question").exhaustive
    observed = next(r for r in requirements if r.id == "documented_items")
    assert not observed.exhaustive and observed.entity == "Riley"


def test_enumeration_packing_does_not_fill_budget_with_one_session():
    query = "List Riley's activities."

    def item(identity, session, text):
        return EvidenceItem(
            id=identity,
            text=text,
            tenant_id="test",
            role="user",
            source_kind="conversation_record",
            source={"session_id": session, "turn_idx": identity},
        )

    noisy = [
        item(f"recent-{i}", "recent", "Riley discussed decorations and shopping.")
        for i in range(20)
    ]
    older = [
        item("old-1", "first", "Riley went swimming."),
        item("old-2", "first", "Riley plays drums."),
        item("old-3", "second", "Riley tried bouldering."),
    ]
    budget = count_tokens_exact(render_items([noisy[0], *older])) + 16
    selected, _ = pack_items(
        [*noisy, *older],
        query,
        budget,
        requirements=requirements_for(RetrievalPlan(query=query, entity_hints=["Riley"])),
    )
    assert {i.id for i in older} <= {i.id for i in selected}
    assert count_tokens_exact(render_items(selected)) <= budget


def history_items():
    return [
        EvidenceItem(id=f"{session}-{index}", text="Riley described an activity.",
                     tenant_id="test", role="user", source_kind="conversation_record",
                     timestamp=date, source={"session_id": session, "turn_idx": index})
        for session, date in [("middle", "2024-06-01"), ("recent", "2024-12-01"),
                              ("old", "2024-01-01")]
        for index in range(4)
    ]


def test_broad_enumeration_reaches_both_ends_before_middle_sources():
    items = history_items()
    ends = [item for item in items if item.source["session_id"] != "middle"]
    expected = [ends[0], ends[1], ends[4], ends[5]]
    budget = count_tokens_exact(render_items(expected)) + 4
    selected, _ = pack_items(items, "List Riley's activities.", budget, requirements=[
        AnswerRequirement(id="question", question_span="List Riley's activities.",
                          operation="enumeration")])
    assert {item.id for item in selected} == {item.id for item in expected}
    assert count_tokens_exact(render_items(selected)) <= budget


def test_explicit_time_scope_does_not_enable_unbounded_history_preference():
    items = history_items()
    budget = count_tokens_exact(render_items([items[0], items[4]])) + 4
    selected, _ = pack_items(items, "List Riley's activities since June.", budget, requirements=[
        AnswerRequirement(id="question", question_span="List Riley's activities since June.",
                          operation="enumeration", temporal_scope={"since": "2024-06-01"})])
    assert {item.source["session_id"] for item in selected} == {"middle", "recent"}
