"""Deterministic LoCoMo QA and retrieval metrics.

The answer scorer mirrors ``snap-research/locomo/task_eval/evaluation.py``:
lowercase, remove punctuation/articles (including ``and``), Porter-stem, and
calculate token-overlap F1.  Category 1 uses the official comma-separated
partial-match aggregation; category 3 ignores alternate answers after ``;``.

The LLM judge remains useful for diagnosis, but these functions are the
network-free primary benchmark metrics.
"""

from __future__ import annotations

import math
import re
import string
from collections import Counter
from collections.abc import Iterable, Sequence
from statistics import fmean

from nltk.stem import PorterStemmer  # type: ignore[import-untyped]

_STEMMER = PorterStemmer()
_ARTICLES = {"a", "an", "the", "and"}
_ABSTENTION_PHRASES = (
    "no information available",
    "not mentioned",
)
SUMMARY_EVALUATOR_VERSION = "engram-locomo-summary-v1"
MM_RELEVANCE_EVALUATOR_VERSION = "engram-caption-mm-relevance-v1"


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


def _summary_tokens(value: object) -> list[str]:
    """Tokenize natural-language summaries without QA article removal."""
    return re.findall(r"[a-z0-9]+", str(value).casefold())


def _ngrams(tokens: Sequence[str], n: int) -> Counter[tuple[str, ...]]:
    if n <= 0:
        raise ValueError("n must be positive")
    return Counter(tuple(tokens[index:index + n]) for index in range(len(tokens) - n + 1))


def _prf(overlap: int, predicted: int, expected: int) -> dict[str, float]:
    precision = overlap / predicted if predicted else 0.0
    recall = overlap / expected if expected else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall
        else 0.0
    )
    return {"precision": precision, "recall": recall, "f1": f1}


def rouge_n(prediction: object, reference: object, *, n: int) -> dict[str, float]:
    """Deterministic ROUGE-N overlap precision/recall/F1."""
    predicted = _ngrams(_summary_tokens(prediction), n)
    expected = _ngrams(_summary_tokens(reference), n)
    overlap = sum((predicted & expected).values())
    return _prf(overlap, sum(predicted.values()), sum(expected.values()))


def rouge_l(prediction: object, reference: object) -> dict[str, float]:
    """ROUGE-L based on the token-level longest common subsequence."""
    predicted = _summary_tokens(prediction)
    expected = _summary_tokens(reference)
    previous = [0] * (len(expected) + 1)
    for p_token in predicted:
        current = [0]
        for index, e_token in enumerate(expected, start=1):
            if p_token == e_token:
                current.append(previous[index - 1] + 1)
            else:
                current.append(max(current[-1], previous[index]))
        previous = current
    return _prf(previous[-1], len(predicted), len(expected))


def bleu(prediction: object, reference: object, *, max_n: int) -> float:
    """Sentence BLEU-N with modified precision and no smoothing."""
    if max_n <= 0:
        raise ValueError("max_n must be positive")
    predicted = _summary_tokens(prediction)
    expected = _summary_tokens(reference)
    if not predicted or not expected:
        return 0.0
    precisions: list[float] = []
    for n in range(1, max_n + 1):
        pred_ngrams = _ngrams(predicted, n)
        expected_ngrams = _ngrams(expected, n)
        denominator = sum(pred_ngrams.values())
        if not denominator:
            return 0.0
        precisions.append(sum((pred_ngrams & expected_ngrams).values()) / denominator)
    if any(value == 0 for value in precisions):
        return 0.0
    brevity_penalty = (
        1.0
        if len(predicted) > len(expected)
        else math.exp(1 - len(expected) / len(predicted))
    )
    return brevity_penalty * math.exp(fmean(math.log(value) for value in precisions))


def adapted_fact_score(
    predicted_facts: Sequence[str],
    gold_facts: Sequence[str],
    *,
    match_threshold: float = 0.5,
) -> dict[str, float]:
    """Lexical, one-to-one FactScore adaptation for LoCoMo observations.

    This intentionally avoids an LLM judge. Candidate fact pairs are ranked
    by official normalized token F1, then greedily matched once when their
    score reaches ``match_threshold``.
    """
    if not 0 <= match_threshold <= 1:
        raise ValueError("match_threshold must be between 0 and 1")
    candidates = sorted(
        (
            (token_f1(predicted, expected), p_index, e_index)
            for p_index, predicted in enumerate(predicted_facts)
            for e_index, expected in enumerate(gold_facts)
        ),
        reverse=True,
    )
    matched_predicted: set[int] = set()
    matched_expected: set[int] = set()
    for score, p_index, e_index in candidates:
        if score < match_threshold:
            break
        if p_index in matched_predicted or e_index in matched_expected:
            continue
        matched_predicted.add(p_index)
        matched_expected.add(e_index)
    return _prf(len(matched_predicted), len(predicted_facts), len(gold_facts))


def mm_relevance(prediction: object, evidence_captions: Sequence[str]) -> float:
    """Pinned caption-conditioned MM-Relevance proxy.

    Upstream LoCoMo does not ship an executable MM-Relevance evaluator. This
    network-free v1 score is the maximum normalized token F1 between the
    prediction and any gold evidence caption and must be reported as adapted,
    never as an upstream official metric.
    """
    if not evidence_captions:
        return 0.0
    return max(token_f1(prediction, caption) for caption in evidence_captions)


def summary_metrics(prediction: object, reference: object) -> dict[str, float]:
    """Return the deterministic lexical summary metric bundle."""
    return {
        "rouge_1": rouge_n(prediction, reference, n=1)["f1"],
        "rouge_2": rouge_n(prediction, reference, n=2)["f1"],
        "rouge_l": rouge_l(prediction, reference)["f1"],
        "bleu_1": bleu(prediction, reference, max_n=1),
        "bleu_2": bleu(prediction, reference, max_n=2),
    }
