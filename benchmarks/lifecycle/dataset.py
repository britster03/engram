from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from . import DATASET_SHA256

CATEGORIES = {1: "multi_hop", 2: "temporal", 3: "open_domain", 4: "single_hop", 5: "adversarial"}
DEFAULT_DATA = Path(__file__).resolve().parents[1] / "data/locomo10.json"
METHODS = ("bm25", "vector", "hybrid", "kg", "hybrid_kg")


@dataclass(frozen=True)
class Turn:
    dia_id: str
    session: int
    index: int
    speaker: str
    text: str
    caption: str
    timestamp: str
    raw_timestamp: str

    @property
    def content(self):
        value = f"[{self.raw_timestamp}] {self.speaker}: {self.text}"
        return value + (f"\nShared image caption: {self.caption}" if self.caption else "")


@dataclass(frozen=True)
class Question:
    conversation_id: str
    index: int
    question: str
    answer: str | None
    category: int
    raw_evidence: tuple[str, ...]
    evidence: tuple[str, ...]
    evidence_sessions: tuple[int, ...]
    annotation_status: str
    annotation_notes: tuple[str, ...]

    @property
    def qa_id(self):
        return f"{self.conversation_id}:qa:{self.index:04d}"

    @property
    def positive(self):
        return self.category != 5 and self.annotation_status in {"VALID", "NORMALIZED"}


@dataclass(frozen=True)
class Conversation:
    id: str
    turns: tuple[Turn, ...]
    questions: tuple[Question, ...]

    @property
    def sessions(self):
        return sorted({t.session for t in self.turns})


@dataclass(frozen=True)
class Evaluation:
    question: Question
    track: str
    cutoff: int
    variant: str
    expected_level: str
    method: str = "native"

    @property
    def key(self):
        return f"{self.track}:{self.question.qa_id}:{self.variant}:{self.method}"


def normalize_evidence(values, known):
    ids, notes = [], []
    invalid = False
    pattern = r"D\s*:?\s*(\d+)\s*:\s*(\d+)"
    for raw in values:
        matches = list(re.finditer(pattern, raw))
        residue = re.sub(pattern, "", raw).strip(" ,;")
        if not matches or residue:
            notes.append(f"invalid syntax: {raw}")
            invalid = True
        for match in matches:
            identity = f"D{int(match[1])}:{int(match[2])}"
            if identity not in known:
                notes.append(f"missing dialogue: {identity}")
                invalid = True
            if identity not in ids:
                ids.append(identity)
    status = "INVALID" if invalid else "MISSING" if not ids else "NORMALIZED" if list(values) != ids else "VALID"
    return tuple(ids), status, tuple(notes)


def load(path=DEFAULT_DATA, *, verify_hash=True):
    path = Path(path)
    if verify_hash and hashlib.sha256(path.read_bytes()).hexdigest() != DATASET_SHA256:
        raise ValueError("Dataset checksum differs from the audited LoCoMo release")
    result, seen = [], set()
    for sample in json.loads(path.read_text()):
        identity = sample["sample_id"]
        if identity in seen:
            raise ValueError("Duplicate conversation ID")
        seen.add(identity)
        source = sample["conversation"]
        sessions = sorted(int(k.split("_")[1]) for k, v in source.items() if re.fullmatch(r"session_\d+", k) and isinstance(v, list))
        turns, known, dates = [], {}, []
        for session in sessions:
            raw_date = source[f"session_{session}_date_time"]
            date = datetime.strptime(raw_date, "%I:%M %p on %d %B, %Y").replace(tzinfo=timezone.utc)
            dates.append(date)
            for index, raw in enumerate(source[f"session_{session}"]):
                dia = raw["dia_id"]
                if dia in known or raw["speaker"] not in {source["speaker_a"], source["speaker_b"]}:
                    raise ValueError("Duplicate dialogue or unknown speaker")
                known[dia] = session
                turns.append(Turn(dia, session, index, raw["speaker"], raw.get("text") or "", raw.get("blip_caption") or "", date.isoformat(), raw_date))
        if dates != sorted(dates):
            raise ValueError("Session timestamps are not chronological")
        questions = []
        for index, qa in enumerate(sample["qa"]):
            raw_evidence = tuple(qa.get("evidence") or [])
            evidence, status, notes = normalize_evidence(raw_evidence, known)
            category = int(qa["category"])
            if category not in CATEGORIES:
                raise ValueError("Unknown category")
            if category == 5:
                status = "NO_GOLD_EVIDENCE"
            answer = None if qa.get("answer") is None else str(qa["answer"])
            questions.append(Question(identity, index, qa["question"], answer, category, raw_evidence, evidence, tuple(sorted({known[e] for e in evidence if e in known})), status, notes))
        result.append(Conversation(identity, tuple(turns), tuple(questions)))
    return result


def select(conversations, profile, seed=42):
    if profile == "full":
        return {q.qa_id for c in conversations for q in c.questions}
    names = {"conv-26"} if profile == "pilot" else {"conv-26", "conv-30"}
    per_category = 5 if profile == "pilot" else 10
    rng, selected = random.Random(seed), set()
    for category in CATEGORIES:
        pool = [q for c in conversations if c.id in names for q in c.questions if q.category == category]
        rng.shuffle(pool)
        if len(pool) < per_category:
            raise ValueError("Insufficient questions for stratification")
        selected.update(q.qa_id for q in pool[:per_category])
    return selected


def evaluations(conversation, selected):
    result = []
    final = conversation.sessions[-1]
    for q in conversation.questions:
        if q.qa_id not in selected:
            continue
        result.append(Evaluation(q, "CANONICAL", final, "completed", "UNDETERMINED"))
        for method in METHODS:
            result.append(Evaluation(q, "SEARCH_COMPARISON", final, "completed", "UNDETERMINED", method))
        if not q.positive:
            result.append(Evaluation(q, "SESSION_ROUTING", final, "annotation_limited", q.annotation_status))
            continue
        cutoff = max(q.evidence_sessions)
        expected = "L1+L2" if len(q.evidence_sessions) > 1 else "L1"
        result.append(Evaluation(q, "SESSION_ROUTING", cutoff, "evidence_session", expected))
        following = conversation.sessions[conversation.sessions.index(cutoff) + 1:]
        if following:
            result.append(Evaluation(q, "SESSION_ROUTING", following[0], "following_session", "L2"))
    return result


def audit(conversations):
    return {"conversations": len(conversations), "sessions": sum(len(c.sessions) for c in conversations), "turns": sum(len(c.turns) for c in conversations), "captions": sum(bool(t.caption) for c in conversations for t in c.turns), "questions": sum(len(c.questions) for c in conversations), "valid_positive_questions": sum(q.positive for c in conversations for q in c.questions), "categories": {CATEGORIES[k]: sum(q.category == k for c in conversations for q in c.questions) for k in CATEGORIES}}
