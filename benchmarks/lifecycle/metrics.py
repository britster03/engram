from __future__ import annotations

import math
import random

TOP_K = (1, 3, 5, 10, 20)


def unique(values):
    return list(dict.fromkeys(values))


def evidence_metrics(gold, retrieved, *, evaluable=True):
    gold, retrieved = set(gold), unique(retrieved)
    result = {"any_evidence_recalled": None, "all_evidence_recalled": None, "evidence_fraction": None, "mrr": None}
    result.update({f"{name}_at_{k}": None for name in ("recall", "precision", "ndcg") for k in TOP_K})
    if not evaluable or not gold:
        return result
    hits = gold.intersection(retrieved)
    result.update(any_evidence_recalled=bool(hits), all_evidence_recalled=hits == gold, evidence_fraction=len(hits) / len(gold), mrr=next((1 / i for i, d in enumerate(retrieved, 1) if d in gold), 0.0))
    for k in TOP_K:
        count = len(gold.intersection(retrieved[:k]))
        dcg = sum(1 / math.log2(i + 2) for i, d in enumerate(retrieved[:k]) if d in gold)
        ideal = sum(1 / math.log2(i + 2) for i in range(min(len(gold), k)))
        result.update({f"recall_at_{k}": count / len(gold), f"precision_at_{k}": count / k, f"ndcg_at_{k}": dcg / ideal})
    return result


def compatible(expected, executed, gold, canonical_ids):
    if expected not in {"L1", "L2", "L1+L2"}:
        return None
    return set(expected.split("+")).issubset(executed) or (bool(gold) and set(gold).issubset(canonical_ids))


def quantile(values, fraction):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lower = int(position)
    return values[lower] + (values[min(lower + 1, len(values) - 1)] - values[lower]) * (position - lower)


def bootstrap_conversations(rows, key, *, samples=2000, seed=42):
    grouped = {}
    for row in rows:
        if row.get(key) is not None:
            grouped.setdefault(row["conversation_id"], []).append(row[key])
    if len(grouped) < 2:
        return None, None
    rng, names, values = random.Random(seed), sorted(grouped), []
    for _ in range(samples):
        resampled = [v for name in rng.choices(names, k=len(names)) for v in grouped[name]]
        values.append(sum(resampled) / len(resampled))
    return quantile(values, .025), quantile(values, .975)
