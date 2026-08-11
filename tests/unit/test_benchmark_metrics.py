from __future__ import annotations

import pytest

from benchmarks.metrics import (
    adapted_fact_score,
    bleu,
    evidence_recall,
    evidence_recall_at_ks,
    mm_relevance,
    normalize_answer,
    partial_match_f1,
    qa_score,
    rouge_l,
    rouge_n,
    summary_metrics,
    token_f1,
)


def test_normalize_answer_matches_official_rules() -> None:
    assert normalize_answer("The Cats, and a DOG!") == "cats dog"


def test_token_f1_uses_partial_overlap_and_porter_stemming() -> None:
    assert token_f1("She enjoys painting", "paint") == pytest.approx(0.5)
    # Official normalization does not canonicalize ordinals (7th -> 7), so
    # only "May" and "2023" overlap here.
    assert token_f1("May 7th 2023", "7 May 2023") == pytest.approx(2 / 3)
    assert token_f1("", "anything") == 0.0


def test_multi_answer_partial_match_averages_each_gold_item() -> None:
    assert partial_match_f1("Oregon", "Oregon, Florida") == pytest.approx(0.5)
    assert partial_match_f1("Florida, Oregon", "Oregon, Florida") == pytest.approx(1.0)


def test_qa_score_category_rules() -> None:
    assert qa_score("Oregon", "Oregon, Florida", category=1) == pytest.approx(0.5)
    assert qa_score("Paris", "Paris; France", category=3) == pytest.approx(1.0)
    assert qa_score("It was not mentioned.", "", category=5) == 1.0
    assert qa_score("Rex", "", category=5) == 0.0
    with pytest.raises(ValueError, match="unsupported"):
        qa_score("x", "x", category=99)


def test_evidence_recall_is_ranked_unique_and_handles_unannotated_rows() -> None:
    retrieved = ["D1:1", "D1:1", "D1:2", "D1:3"]
    gold = ["D1:2", "D1:4"]
    assert evidence_recall(retrieved, gold, k=1) == 0.0
    assert evidence_recall(retrieved, gold, k=2) == pytest.approx(0.5)
    assert evidence_recall(retrieved, [], k=5) == 1.0
    assert evidence_recall_at_ks(retrieved, gold, ks=(1, 2)) == {
        "recall_at_1": 0.0,
        "recall_at_2": 0.5,
    }


def test_rouge_and_bleu_fixtures_are_hand_calculable() -> None:
    prediction = "Caroline likes painting"
    reference = "Caroline likes painting and swimming"
    assert rouge_n(prediction, reference, n=1) == pytest.approx({
        "precision": 1.0,
        "recall": 0.6,
        "f1": 0.75,
    })
    assert rouge_n(prediction, reference, n=2)["f1"] == pytest.approx(2 / 3)
    assert rouge_l(prediction, reference)["f1"] == pytest.approx(0.75)
    assert bleu(prediction, reference, max_n=1) == pytest.approx(0.513417119, abs=1e-8)
    assert summary_metrics("same words", "same words") == pytest.approx({
        "rouge_1": 1.0,
        "rouge_2": 1.0,
        "rouge_l": 1.0,
        "bleu_1": 1.0,
        "bleu_2": 1.0,
    })


def test_adapted_fact_score_uses_one_to_one_matching() -> None:
    score = adapted_fact_score(
        ["Caroline likes painting", "Caroline likes painting", "Melanie swims"],
        ["Caroline likes painting", "Melanie swims with her kids"],
        match_threshold=0.5,
    )
    assert score == pytest.approx({"precision": 2 / 3, "recall": 1.0, "f1": 0.8})


def test_mm_relevance_is_caption_conditioned_and_network_free() -> None:
    assert mm_relevance(
        "a red bicycle near a wall",
        ["a dog", "red bicycle beside the wall"],
    ) > 0.6
    assert mm_relevance("anything", []) == 0.0
