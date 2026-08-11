"""L0 binary gate (§3.1).

Three-tier decision:

  1. Regex OR-gate (§3.1.1) on the first sentence — deixis, anaphora,
     sentence-initial conjunctions, personal possessives, pronoun-led
     queries, or syntactic incompleteness force CONTINUE.
  2. Optional trained binary classifier (33M-param BGE head per §14.1).
     Until traces are collected and the classifier is trained, the
     `AlwaysClass0Classifier` placeholder leaves decisions to the other
     two tiers — this matches the SDD's bootstrap recommendation.
  3. Memory-hit fallback (§3.1.2): one embedding + vector lookup on the
     KG; cosine ≥ `memory_hit_threshold` overrides BYPASS → CONTINUE.

Bias is toward recall: uncertain queries go to L1.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from engram.models.embeddings import EmbeddingService
from engram.storage.neo4j_store import Neo4jStore

# ---- Regex OR-gate (§3.1.1). Deployment-tunable. --------------------------------

_FIRST_SENTENCE = re.compile(r"^[^.!?]*[.!?]?")

_REGEX_PATTERNS: list[re.Pattern[str]] = [
    # Sentence-initial conjunctions (anaphora to prior turn)
    re.compile(r"^\s*(but|and|so|or|then|also|actually|wait)\b", re.IGNORECASE),
    # Deictic references to prior conversation
    re.compile(
        r"\b(previously|earlier|last turn|as i said|as i mentioned|you mentioned|you said|we discussed|above|before)\b",
        re.IGNORECASE,
    ),
    # Possessives typically referencing user memory
    re.compile(
        r"\bmy\s+(wife|husband|partner|kids?|children|family|address|birthday|password|schedule|calendar|team|manager|boss)\b",
        re.IGNORECASE,
    ),
    # First/second-person pronouns implying prior context
    re.compile(r"^\s*(he|she|they|it|that|this|these|those)\s", re.IGNORECASE),
    # Syntactic incompleteness (ends with coordinator/dangling prep)
    re.compile(r"\b(and|or|but|because|since|if|when|while|about|for|with)\s*\.?\s*$", re.IGNORECASE),
]


@dataclass
class GateDecision:
    decision: str  # "BYPASS" | "CONTINUE"
    reason: str
    memory_hit: dict | None = None  # filled when the fallback triggered
    classifier_probability: float | None = None
    classifier_mode: str = "off"


class L0Classifier(Protocol):
    def predict(self, query: str) -> float:  # pragma: no cover - interface
        """Returns Class-1 probability (needs context)."""
        ...


class AlwaysClass0Classifier:
    """Placeholder for when the trained classifier isn't available.

    Forces the regex+memory-hit path to do all the work. This is fine in
    practice; §3.1.2 observes the fallback catches most failure cases.
    """

    def predict(self, _query: str) -> float:
        return 0.0


def run_l0_gate(
    query: str,
    *,
    classifier: L0Classifier,
    embed: EmbeddingService,
    neo4j: Neo4jStore,
    threshold: float = 0.3,
    memory_hit_threshold: float = 0.75,
    skip: bool = False,
    skip_reason: str = "retrieval.l0_skip=true",
    classifier_mode: str = "off",
    classifier_available: bool = True,
) -> GateDecision:
    """Apply the L0 binary gate and memory-hit fallback."""
    if skip:
        return GateDecision(
            decision="CONTINUE",
            reason=skip_reason,
            classifier_mode=classifier_mode,
        )

    first = _FIRST_SENTENCE.match(query or "")
    head = (first.group(0) if first else query or "").strip()

    for pat in _REGEX_PATTERNS:
        if pat.search(head):
            return GateDecision(
                decision="CONTINUE",
                reason=f"regex:{pat.pattern[:30]}",
                classifier_mode=classifier_mode,
            )

    prob: float | None = None
    if classifier_mode in {"shadow", "active"} and classifier_available:
        try:
            prob = classifier.predict(query)
        except Exception:
            if classifier_mode == "active":
                return GateDecision(
                    decision="CONTINUE",
                    reason="classifier_error_fail_open",
                    classifier_mode=classifier_mode,
                )
    elif classifier_mode == "active":
        return GateDecision(
            decision="CONTINUE",
            reason="classifier_unavailable_fail_open",
            classifier_mode=classifier_mode,
        )
    if classifier_mode == "active" and prob is not None and prob >= threshold:
        return GateDecision(
            decision="CONTINUE",
            reason=f"classifier:{prob:.2f}",
            classifier_probability=prob,
            classifier_mode=classifier_mode,
        )

    # BYPASS so far — run the memory-hit fallback (§3.1.2).
    emb = embed.embed(query)
    hits = neo4j.vector_search(emb, k=1, dormant_floor=0.0)
    if hits and float(hits[0].get("score", 0.0)) >= memory_hit_threshold:
        return GateDecision(
            decision="CONTINUE",
            reason=f"memory_hit:{hits[0]['score']:.2f}",
            memory_hit=hits[0],
            classifier_probability=prob,
            classifier_mode=classifier_mode,
        )
    return GateDecision(
        decision="BYPASS",
        reason="no_match",
        classifier_probability=prob,
        classifier_mode=classifier_mode,
    )
