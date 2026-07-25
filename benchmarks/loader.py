"""LoCoMo dataset loader.

Parses `locomo10.json` into a shape the Engram harness can consume:
conversations of chronologically-ordered turns, plus their QA probes.

Dataset quirks handled here (all verified against the real file):

1. `conversation` holds both `session_N` (turn lists) and `session_N_date_time`
   (strings). The two sets do NOT always line up — conv0 has 35 date_time keys
   but only 19 sessions with turns. We iterate the turn lists and look the
   timestamp up, never the reverse.
2. Session keys must be sorted NUMERICALLY. Lexicographic sort puts
   `session_10` before `session_2`, which scrambles chronology and breaks the
   321 temporal-reasoning questions.
3. Adversarial probes (category 5) carry `adversarial_answer` instead of
   `answer`; 444 of them have `answer: None`. Their correct behaviour is
   abstention, so they are flagged rather than dropped.
4. `answer` is an int in a handful of rows — coerced to str.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# LoCoMo category codes -> human labels, used to break the baseline down by
# question type. Cat 5 is the abstention set.
CATEGORY_NAMES = {
    1: "multi_hop",
    2: "temporal",
    3: "open_domain",
    4: "single_hop",
    5: "adversarial",
}

_SESSION_NUM = re.compile(r"^session_(\d+)$")

# e.g. "1:56 pm on 8 May, 2023"
_DATE_FMT = "%I:%M %p on %d %B, %Y"


@dataclass
class Turn:
    """One utterance by one speaker."""

    speaker: str
    text: str
    dia_id: str          # e.g. "D1:3" — what `evidence` refers to
    session_idx: int
    timestamp: str | None  # ISO-8601 when parseable, else the raw string

    def attributed(self) -> str:
        """Text prefixed with the speaker name.

        Engram ingests user/assistant pairs, so the original speaker identity
        would otherwise be lost in the slot assignment. Questions ask things
        like "When did Caroline go to...", so attribution must survive into the
        extracted memory. Prefixing is the cheapest way to preserve it.
        """
        return f"{self.speaker}: {self.text}"


@dataclass
class QAProbe:
    """One benchmark question."""

    question: str
    answer: str
    category: int
    evidence: list[str]        # dia_ids that contain the answer
    is_adversarial: bool       # category 5 — correct behaviour is abstention

    @property
    def category_name(self) -> str:
        return CATEGORY_NAMES.get(self.category, f"unknown_{self.category}")


@dataclass
class Conversation:
    sample_id: str
    speaker_a: str
    speaker_b: str
    turns: list[Turn] = field(default_factory=list)
    qa: list[QAProbe] = field(default_factory=list)

    @property
    def n_sessions(self) -> int:
        return len({t.session_idx for t in self.turns})


def _parse_timestamp(raw: str | None) -> str | None:
    """Normalise "1:56 pm on 8 May, 2023" to ISO-8601.

    Engram's TurnContent.timestamp is a free-form string, but ISO makes the
    temporal Cypher templates usable downstream. Falls back to the raw string
    rather than dropping data if the format ever varies.
    """
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), _DATE_FMT).isoformat()
    except ValueError:
        return raw


def _session_keys(conversation: dict) -> list[tuple[int, str]]:
    """Return (session_number, key) for sessions that actually hold turns.

    Sorted numerically — see quirk 2 in the module docstring.
    """
    found = []
    for key, value in conversation.items():
        m = _SESSION_NUM.match(key)
        if m and isinstance(value, list):
            found.append((int(m.group(1)), key))
    return sorted(found)


def _load_turns(conversation: dict) -> list[Turn]:
    turns: list[Turn] = []
    for idx, key in _session_keys(conversation):
        timestamp = _parse_timestamp(conversation.get(f"{key}_date_time"))
        for raw in conversation[key]:
            text = (raw.get("text") or "").strip()
            if not text:
                # Some turns are image-only in the multimodal variant.
                continue
            turns.append(
                Turn(
                    speaker=raw.get("speaker", "unknown"),
                    text=text,
                    dia_id=raw.get("dia_id", ""),
                    session_idx=idx,
                    timestamp=timestamp,
                )
            )
    return turns


def _load_qa(rows: list[dict]) -> list[QAProbe]:
    probes: list[QAProbe] = []
    for row in rows:
        question = (row.get("question") or "").strip()
        if not question:
            continue
        category = row.get("category")
        # Quirk 3: adversarial rows answer under a different key.
        raw_answer = row.get("answer")
        is_adversarial = raw_answer is None or category == 5
        if raw_answer is None:
            raw_answer = row.get("adversarial_answer")
        probes.append(
            QAProbe(
                question=question,
                answer="" if raw_answer is None else str(raw_answer),  # quirk 4
                category=category if isinstance(category, int) else -1,
                evidence=list(row.get("evidence") or []),
                is_adversarial=bool(is_adversarial),
            )
        )
    return probes


def load_locomo(path: str | Path) -> list[Conversation]:
    """Load every conversation from locomo10.json."""
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)

    conversations = []
    for sample in raw:
        convo = sample.get("conversation", {})
        conversations.append(
            Conversation(
                sample_id=sample.get("sample_id", "unknown"),
                speaker_a=convo.get("speaker_a", "speaker_a"),
                speaker_b=convo.get("speaker_b", "speaker_b"),
                turns=_load_turns(convo),
                qa=_load_qa(sample.get("qa") or []),
            )
        )
    return conversations


def pair_turns(turns: list[Turn]) -> list[tuple[Turn, Turn | None]]:
    """Group a flat turn list into consecutive pairs.

    Engram's /ingest takes a `turn_pair` (user + assistant), so a two-speaker
    transcript has to be chunked two at a time. Speaker identity is carried in
    the text via `Turn.attributed()` rather than the slot, so an odd number of
    turns or the same speaker twice in a row is harmless.

    A trailing unpaired turn yields (turn, None); the caller decides how to
    pad it.
    """
    return [
        (turns[i], turns[i + 1] if i + 1 < len(turns) else None)
        for i in range(0, len(turns), 2)
    ]


if __name__ == "__main__":
    import sys
    from collections import Counter

    src = sys.argv[1] if len(sys.argv) > 1 else "benchmarks/data/locomo10.json"
    convos = load_locomo(src)

    print(f"loaded {len(convos)} conversations from {src}\n")
    total_turns = total_qa = 0
    for i, c in enumerate(convos):
        print(
            f"  conv{i} {c.sample_id:>8}  {c.speaker_a} <-> {c.speaker_b}  "
            f"sessions={c.n_sessions:>3}  turns={len(c.turns):>4}  "
            f"pairs={len(pair_turns(c.turns)):>4}  qa={len(c.qa):>4}"
        )
        total_turns += len(c.turns)
        total_qa += len(c.qa)

    cats = Counter(q.category_name for c in convos for q in c.qa)
    adv = sum(q.is_adversarial for c in convos for q in c.qa)
    print(f"\n  totals: turns={total_turns}  qa={total_qa}")
    print(f"  categories: {dict(cats)}")
    print(f"  adversarial (abstention expected): {adv}")

    sample = convos[0]
    print(f"\n  first turn of {sample.sample_id}:")
    t = sample.turns[0]
    print(f"    dia_id={t.dia_id}  ts={t.timestamp}")
    print(f"    {t.attributed()}")
