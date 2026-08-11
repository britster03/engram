"""Deterministic LoCoMo QA and retrieval metrics.

The answer scorer mirrors ``snap-research/locomo/task_eval/evaluation.py``:
lowercase, remove punctuation/articles (including ``and``), Porter-stem, and
calculate token-overlap F1.  Category 1 uses the official comma-separated
partial-match aggregation; category 3 ignores alternate answers after ``;``.

The LLM judge remains useful for diagnosis, but these functions are the
network-free primary benchmark metrics.
"""

from __future__ import annotations

import string
from collections import Counter
from collections.abc import Iterable, Sequence
from statistics import fmean

from nltk.stem import PorterStemmer

_STEMMER = PorterStemmer()
_ARTICLES = {"a", "an", "the", "and"}
_ABSTENTION_PHRASES = (
    "no information available",
    "not mentioned",
)


def normalize_answer(value: object) -> str:
    """Apply the normalization used by the official LoCoMo QA evaluator."""
    text = str(value).replace(",", "").lower()
    text = "".join(ch for ch in text if ch not in string.punctuation)
    return " ".join(token for token in text.split() if token not in _ARTICLES)


def token_f1(prediction: object, ground_truth: object) -> float:
    """Return Porter-stemmed token-overlap F1 for one answer pair."""
    predicted = [_STEMMER.stem(token) for token in normalize_answer(prediction).split()]
    expected = [_STEMMER.stem(token) for token in normalize_answer(ground_truth).split()]
    common = Counter(predicted) & Counter(expected)
    overlap = sum(common.values())
    if overlap == 0 or not predicted or not expected:
        return 0.0
    precision = overlap / len(predicted)
    recall = overlap / len(expected)
    return 2 * precision * recall / (precision + recall)


def partial_match_f1(prediction: object, ground_truth: object) -> float:
    """Official category-1 score for comma-separated multi-answer values."""
    predictions = [part.strip() for part in str(prediction).split(",")]
    expected = [part.strip() for part in str(ground_truth).split(",")]
    return fmean(max(token_f1(candidate, answer) for candidate in predictions) for answer in expected)


def qa_score(
    prediction: object,
    ground_truth: object,
    *,
    category: int,
) -> float:
    """Score one LoCoMo QA row according to its official category handling."""
    predicted = str(prediction)
    expected = str(ground_truth)
    if category == 1:
        return partial_match_f1(predicted, expected)
    if category in {2, 3, 4}:
        if category == 3:
            expected = expected.split(";", 1)[0].strip()
        return token_f1(predicted, expected)
    if category == 5:
        lowered = predicted.lower()
        return float(any(phrase in lowered for phrase in _ABSTENTION_PHRASES))
    raise ValueError(f"unsupported LoCoMo category: {category}")


def evidence_recall(
    retrieved_turn_ids: Sequence[str] | Iterable[str],
    gold_turn_ids: Sequence[str] | Iterable[str],
    *,
    k: int,
) -> float:
    """Return recall of unique gold evidence IDs in the first ``k`` unique hits.

    LoCoMo's official retrieval calculation assigns 1.0 when no evidence is
    annotated.  Duplicated retrieved IDs do not consume multiple ranks.
    """
    if k <= 0:
        raise ValueError("k must be positive")
    gold = {str(value) for value in gold_turn_ids if str(value)}
    if not gold:
        return 1.0
    ranked: list[str] = []
    seen: set[str] = set()
    for value in retrieved_turn_ids:
        turn_id = str(value)
        if not turn_id or turn_id in seen:
            continue
        seen.add(turn_id)
        ranked.append(turn_id)
        if len(ranked) == k:
            break
    return len(gold.intersection(ranked)) / len(gold)


def evidence_recall_at_ks(
    retrieved_turn_ids: Sequence[str] | Iterable[str],
    gold_turn_ids: Sequence[str] | Iterable[str],
    *,
    ks: Sequence[int] = (5, 10, 25),
) -> dict[str, float]:
    """Calculate the benchmark's standard evidence Recall@k fields."""
    retrieved = list(retrieved_turn_ids)
    gold = list(gold_turn_ids)
    return {f"recall_at_{k}": evidence_recall(retrieved, gold, k=k) for k in ks}
