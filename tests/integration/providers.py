"""Deterministic test providers.

These are **real, working implementations** of the CoreModelProvider,
FrontierLLMProvider, and EmbeddingService interfaces — not mocks. They
produce output deterministically so tests can assert on pipeline behaviour
without requiring live LLM APIs or GPU-accelerated embedders.

If you need to test against real providers, use the Ollama Cloud live smoke
test and run it only with a sandbox account.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

from engram.models.core import CompletionResult, CoreModelProvider
from engram.models.frontier import FrontierLLMProvider, FrontierVerdict


class DeterministicCoreProvider(CoreModelProvider):
    """Rule-based Core Model.

    Dispatches on the `[TASK_TAG]` at the start of the system prompt (every
    Engram prompt template begins with one). Each handler produces a
    well-formed response for that task type derived purely from the input
    text — reproducible across runs, no randomness, no network.
    """

    def complete(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        output_schema: dict[str, Any] | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> CompletionResult:
        tag = system_prompt.split("]", 1)[0].lstrip("[") if system_prompt.startswith("[") else ""
        handler = _HANDLERS.get(tag, _default)
        out = handler(system_prompt)
        return CompletionResult(output=out, raw_text="(deterministic)")


def _extract_turn_pair(prompt: str) -> tuple[str, str]:
    user_m = re.search(r"^USER:\s*(.*)", prompt, flags=re.MULTILINE)
    asst_m = re.search(r"^ASSISTANT:\s*(.*)", prompt, flags=re.MULTILINE)
    return (
        (user_m.group(1).strip() if user_m else ""),
        (asst_m.group(1).strip() if asst_m else ""),
    )


def _handle_gate(prompt: str) -> dict[str, Any]:
    user, asst = _extract_turn_pair(prompt)
    combined = f"{user}\n{asst}".lower()
    if any(t in combined for t in ["hello", "thanks", "ok"]) and len(combined) < 50:
        return {"store": False, "reason": "pleasantry"}
    return {"store": True, "reason": "contains user facts"}


def _handle_extract(prompt: str) -> dict[str, Any]:
    user, asst = _extract_turn_pair(prompt)
    triplets: list[dict[str, Any]] = []
    patterns = [
        (r"I (?:just )?(?:accepted a|work(?:ing)?|started|took) (?:a )?(?:job|role|position) at ([A-Z][\w\s]+)", "works_at"),
        (r"I (?:will be|am|'m) in ([A-Z][\w\s]+?)(?: by| on|\.|$)", "located_in"),
        (r"renting (?:a place )?in ([A-Z][\w\s]+)", "renting_in"),
        (r"(?:my )?wife's birthday is ([A-Z][\w\s0-9]+)", "wife_birthday"),
        (r"moved to ([A-Za-z][\w\s\-]+?)(?:\.|,|$)", "moved_to"),
        (r"city-?(\d+)", "lives_in_city"),
    ]
    for pat, rel in patterns:
        for match in re.finditer(pat, user, flags=re.IGNORECASE):
            obj = match.group(1).strip()
            triplets.append({
                "subject": "user", "relation": rel, "object": obj, "confidence": 0.85,
            })
    abstract = user or asst
    if len(abstract) > 200:
        abstract = abstract[:200]
    return {
        "resolved_text": user,
        "triplets": triplets,
        "l0_abstract": abstract or "empty",
    }


def _handle_l1_plan(prompt: str) -> dict[str, Any]:
    m = re.search(r"User query:\s*\n(.*)", prompt)
    q = m.group(1).strip() if m else "?"
    return {
        "session_sufficient": False,
        "predicted_depth": "L4",
        "mode": "KG",
        "entry_points": [],
        "vector_queries": [q],
        "commands": [
            {"template": "t_top_k_vector", "params": {"query": q, "k": 10}},
        ],
    }


def _handle_ln_plan(_prompt: str) -> dict[str, Any]:
    return {
        "previous_level_sufficient": True,
        "terminate_cascade": True,
        "commands": [],
        "coverage": {"aspects_covered": [], "aspects_missing": []},
    }


def _handle_link(_prompt: str) -> dict[str, Any]:
    return {"matched_id": None, "confidence": 0.0, "reason": "no-match"}


def _handle_overview(_prompt: str) -> dict[str, Any]:
    return {"overview": "# Overview\n\n(deterministic generation)"}


def _handle_compact(_prompt: str) -> dict[str, Any]:
    return {"compacted": "(deterministic compaction)", "key_facts": [], "key_entities": []}


def _handle_dedup(_prompt: str) -> dict[str, Any]:
    return {"case": "CO_EXISTENCE", "existing_edge_id": None,
            "reason": "deterministic-coexistence"}


def _default(_prompt: str) -> dict[str, Any]:
    return {"store": False, "reason": "default"}


_HANDLERS = {
    "GATE": _handle_gate,
    "EXTRACT": _handle_extract,
    "L1_PLAN": _handle_l1_plan,
    "LN_PLAN": _handle_ln_plan,
    "LINK": _handle_link,
    "OVERVIEW": _handle_overview,
    "COMPACT": _handle_compact,
    "DEDUP": _handle_dedup,
}


class DeterministicFrontierProvider(FrontierLLMProvider):
    """Frontier provider that returns the retrieved MSC verbatim as the answer."""

    def answer(
        self,
        *,
        system_prompt: str,
        msc: str,
        user_query: str,
        allow_need_more: bool = True,
    ) -> FrontierVerdict:
        lines = [line.strip() for line in msc.splitlines() if line.strip()]
        citations = [
            line for line in lines
            if line and not line.startswith("#")
            and not line.startswith("[")
            and "(source:" not in line
        ][:3]
        answer = " ".join(citations) or "No relevant memory was retrieved."
        return FrontierVerdict(verdict="ANSWER", answer=answer)


@dataclass
class DeterministicEmbeddingService:
    """SHA-256-derived 384-dim unit-normalised vectors — reproducible across runs."""

    dim: int = 384

    def embed(self, text: str) -> list[float]:
        return _hash_vec(text)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [_hash_vec(t) for t in texts]


def _hash_vec(text: str) -> list[float]:
    raw = hashlib.sha256(text.lower().encode("utf-8")).digest()
    while len(raw) < 384:
        raw = raw + hashlib.sha256(raw).digest()
    raw = raw[:384]
    vec = [(b - 128) / 128.0 for b in raw]
    norm = sum(v * v for v in vec) ** 0.5 or 1.0
    return [v / norm for v in vec]
