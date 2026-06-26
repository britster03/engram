"""Synthetic training-data generator for every Core-Model task.

Bootstrap training sets sit at roughly the sizes the SDD prescribes
(§14.2.2). We generate each task's JSONL in the format the SFT/DPO
scripts consume:

    { "task_type": "gate_write",
      "system_prompt": "[GATE] …",
      "user_prompt": "…",
      "output": { … } }          # strict JSON matching the task schema

Variety is important — each generator combines a grammar of entities +
actions + relations so the model sees diverse surfaces, not
near-duplicate sentences.

Usage:
    python -m engram.training.synthetic_data --out ./data/train --counts default
    python -m engram.training.synthetic_data --out ./data/train \
        --count gate_write=5000 --count extract=3000

Output layout:
    ./data/train/
        gate_write.jsonl
        extract.jsonl
        l1_plan.jsonl
        ln_plan.jsonl
        dedup.jsonl
        entity_link.jsonl
        overview.jsonl
        session_compact.jsonl
        unmerge.jsonl
        gate_classifier.jsonl    # flat {query, label} for the §14.1 head
"""

from __future__ import annotations

import argparse
import json
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

# ----------------------------------------------------------------------
# Grammar building blocks
# ----------------------------------------------------------------------

_PEOPLE = [
    "Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Henry",
    "Iris", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Priya",
    "Quinn", "Ravi", "Sam", "Tara", "Uma", "Vera", "Will", "Xavier",
    "Yara", "Zane",
]
_COMPANIES = [
    "Meta", "Google", "Ollama", "Stripe", "Figma", "OpenAI",
    "Cloudflare", "Shopify", "Vercel", "Snowflake", "Databricks",
    "GitHub", "DeepMind", "Notion", "Ramp",
]
_CITIES = [
    "San Francisco", "New York", "Seattle", "Austin", "Berlin", "London",
    "Tokyo", "Menlo Park", "Palo Alto", "Boston", "Chicago", "Toronto",
    "Sydney", "Amsterdam",
]
_ROLES = [
    "software engineer", "staff ML engineer", "product manager",
    "data scientist", "designer", "researcher", "technical writer",
    "engineering manager", "CTO",
]
_PROJECTS = [
    "Project Atlas", "Project Helios", "Project Nebula", "Project Aurora",
    "Project Beacon", "the ranking pipeline", "the onboarding flow",
    "the billing service",
]
_PETS = [
    "a golden retriever named Rex", "a siamese cat named Ada",
    "a parrot named Mango", "a rabbit named Juno",
]
_DATES = [
    "March 5th", "April 12th", "June 21st", "July 12th", "October 3rd",
    "2026-05-04", "2026-07-12", "2026-09-01",
]

_PLEASANTRIES = [
    ("thanks!", "anytime."),
    ("hello", "hi."),
    ("good morning", "morning."),
    ("ok", "got it."),
    ("thank you so much", "of course, anytime."),
    ("great, thanks", "happy to help."),
    ("bye", "goodbye!"),
]

_QUESTIONS_STANDALONE = [
    "What is the capital of France?",
    "How does a transformer work?",
    "What is 3417 times 98?",
    "Translate 'hello' to Japanese.",
    "What year did the first iPhone launch?",
    "Explain quantum entanglement briefly.",
    "Is Python dynamically typed?",
    "What's the boiling point of water in Fahrenheit?",
]

_QUESTIONS_CONTEXT_DEPENDENT = [
    "And what was the next step?",
    "What did you say about the project?",
    "When is my wife's birthday?",
    "Remind me of my manager's name.",
    "What is my home city again?",
    "As I mentioned earlier, what's the timeline?",
    "You said we'd talk about the budget — what are the numbers?",
    "Previously we discussed the migration — when does it ship?",
]


def _seed(seed: int) -> random.Random:
    return random.Random(seed)


# ----------------------------------------------------------------------
# Per-task generators
# ----------------------------------------------------------------------

def gen_gate_write(n: int, *, seed: int = 0) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out: list[dict[str, Any]] = []
    for i in range(n):
        if i % 3 == 0:
            user, asst = rnd.choice(_PLEASANTRIES)
            store = False
            reason = "pleasantry / no durable content"
        else:
            company = rnd.choice(_COMPANIES)
            city = rnd.choice(_CITIES)
            kind = rnd.choice(["job", "move", "project", "event", "preference", "fact"])
            if kind == "job":
                user = f"I just accepted a {rnd.choice(_ROLES)} role at {company}."
                asst = rnd.choice(["Congrats!", "That's exciting.", "When do you start?"])
            elif kind == "move":
                user = f"I'm moving to {city} by {rnd.choice(_DATES)}."
                asst = "Noted."
            elif kind == "project":
                user = f"I started working on {rnd.choice(_PROJECTS)} this week."
                asst = "Tell me more."
            elif kind == "event":
                user = f"My wife's birthday is {rnd.choice(_DATES)}."
                asst = "Remembered."
            elif kind == "preference":
                user = f"I prefer {rnd.choice(['Rust', 'Python', 'tea', 'coffee', 'dark mode'])} over the alternative."
                asst = "Got it."
            else:
                user = f"My manager is {rnd.choice(_PEOPLE)}."
                asst = "Noted."
            store = True
            reason = "contains durable user-specific information"
        sys_p = (
            f"[GATE] Write-path gate — §5.4.2.\n\nPreceding session summary (if any):\n"
            f"(none)\n\nTurn pair:\nUSER: {user}\nASSISTANT: {asst}\n"
        )
        out.append({
            "task_type": "gate_write",
            "system_prompt": sys_p,
            "user_prompt": "Respond with a JSON object matching the schema.",
            "output": {"store": store, "reason": reason},
        })
    return out


def gen_extract(n: int, *, seed: int = 1) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    for _ in range(n):
        person = rnd.choice(_PEOPLE)
        company = rnd.choice(_COMPANIES)
        city = rnd.choice(_CITIES)
        role = rnd.choice(_ROLES)
        template = rnd.choice([
            (
                f"I just accepted a {role} role at {company}.",
                "Congrats!",
                f"User accepted a {role} role at {company}.",
                [
                    {"subject": "user", "relation": "works_at",
                     "object": company, "confidence": 0.92},
                    {"subject": "user", "relation": "role",
                     "object": role, "confidence": 0.88},
                ],
                f"User accepted a {role} role at {company}.",
            ),
            (
                f"I'm moving to {city} next month.",
                "Noted.",
                f"User is moving to {city} next month.",
                [
                    {"subject": "user", "relation": "moving_to",
                     "object": city, "confidence": 0.9},
                ],
                f"User is moving to {city} next month.",
            ),
            (
                f"My manager's name is {person}.",
                "Got it.",
                f"User's manager is {person}.",
                [
                    {"subject": "user", "relation": "reports_to",
                     "object": person, "confidence": 0.95},
                ],
                f"User reports to {person}.",
            ),
        ])
        user, asst, resolved, triplets, l0 = template
        sys_p = (
            f"[EXTRACT] S-R-O extraction + L0 abstract — §5.4.3.\n\n"
            f"Preceding session context:\n(none)\n\nTurn pair:\nUSER: {user}\nASSISTANT: {asst}\n"
        )
        out.append({
            "task_type": "extract",
            "system_prompt": sys_p,
            "user_prompt": "Respond with a JSON object matching the schema.",
            "output": {
                "resolved_text": resolved,
                "triplets": triplets,
                "l0_abstract": l0,
            },
        })
    return out


def gen_l1_plan(n: int, *, seed: int = 2) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    depths = ["L1", "L2", "L3", "L4"]
    modes = ["AGFS", "KG", "HYBRID"]
    questions = [
        *_QUESTIONS_CONTEXT_DEPENDENT,
        "Where does the user work?",
        "What's the user's home city?",
        "Who is the user's manager?",
        "What projects does the user contribute to?",
    ]
    for _ in range(n):
        q = rnd.choice(questions)
        depth = rnd.choice(depths)
        mode = rnd.choice(modes)
        sys_p = f"[L1_PLAN] Level-1 retrieval planner — §3.2.\n\n…User query:\n{q}\n"
        out.append({
            "task_type": "l1_plan",
            "system_prompt": sys_p,
            "user_prompt": "Return the plan JSON.",
            "output": {
                "session_sufficient": False,
                "predicted_depth": depth,
                "mode": mode,
                "entry_points": [],
                "vector_queries": [q],
                "commands": [
                    {"template": "t_top_k_vector",
                     "params": {"query": q, "k": 10}},
                ],
            },
        })
    return out


def gen_ln_plan(n: int, *, seed: int = 3) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    for _ in range(n):
        terminate = rnd.random() < 0.4
        sys_p = "[LN_PLAN] Fused plan-with-judge — §4.2.\n\n…Previous level (L1) results:\n(stub)\n"
        body = {
            "previous_level_sufficient": terminate,
            "terminate_cascade": terminate,
            "commands": [] if terminate else [
                {"template": "t_neighbours_by_relation",
                 "params": {"node_id": "mem://user/entities/alice/",
                            "relation": "works_at", "hops": 1}},
            ],
            "coverage": {
                "aspects_covered": ["who is alice", "where she works"] if terminate else ["who is alice"],
                "aspects_missing": [] if terminate else ["when did the project start"],
            },
        }
        out.append({
            "task_type": "ln_plan", "system_prompt": sys_p,
            "user_prompt": "Return the fused plan-judge JSON.", "output": body,
        })
    return out


def gen_dedup(n: int, *, seed: int = 4) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    for _ in range(n):
        case = rnd.choices(
            ["DUPLICATE", "CONTRADICTION", "CO_EXISTENCE"],
            weights=[1, 1, 2],
        )[0]
        sys_p = "[DEDUP] Dedup + conflict classification — §6.5.\n\n…"
        out.append({
            "task_type": "dedup", "system_prompt": sys_p,
            "user_prompt": "Return the dedup JSON.",
            "output": {
                "case": case,
                "existing_edge_id": 42 if case != "CO_EXISTENCE" else None,
                "reason": f"{case.lower()} by construction",
            },
        })
    return out


def gen_entity_link(n: int, *, seed: int = 5) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    for _ in range(n):
        person = rnd.choice(_PEOPLE)
        match = rnd.random() < 0.4
        sys_p = f"[LINK] Entity-link disambiguation — §5.4.4.\n\nEntity mention: {person}\n"
        out.append({
            "task_type": "entity_link", "system_prompt": sys_p,
            "user_prompt": "Return the disambiguation JSON.",
            "output": {
                "matched_id": f"mem://user/entities/{person.lower()}/" if match else None,
                "confidence": 0.9 if match else 0.2,
                "reason": "name + role match" if match else "names alone don't decide",
            },
        })
    return out


def gen_overview(n: int, *, seed: int = 6) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    for _ in range(n):
        person = rnd.choice(_PEOPLE)
        sys_p = f"[OVERVIEW] Directory overview generation — §7.3.\n\nDirectory URI: mem://user/entities/{person.lower()}/"
        out.append({
            "task_type": "overview", "system_prompt": sys_p,
            "user_prompt": "Return the overview as Markdown.",
            "output": {
                "overview": (
                    f"# {person}\n\n"
                    f"{person} works at {rnd.choice(_COMPANIES)}.\n\n"
                    f"## Relationships\n- reports_to → {rnd.choice(_PEOPLE)}\n"
                ),
            },
        })
    return out


def gen_session_compact(n: int, *, seed: int = 7) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    for _ in range(n):
        sys_p = "[COMPACT] Session compaction — §8.3.\n\nTurn history (oldest first):\n…"
        out.append({
            "task_type": "session_compact", "system_prompt": sys_p,
            "user_prompt": "Return the compaction JSON.",
            "output": {
                "compacted": f"User discussed {rnd.choice(_PROJECTS)} and moving to {rnd.choice(_CITIES)}.",
                "key_facts": [f"user plans to move to {rnd.choice(_CITIES)}"],
                "key_entities": [rnd.choice(_PEOPLE), rnd.choice(_COMPANIES)],
            },
        })
    return out


def gen_unmerge(n: int, *, seed: int = 8) -> list[dict[str, Any]]:
    rnd = _seed(seed)
    out = []
    for _ in range(n):
        p1, p2 = rnd.sample(_PEOPLE, 2)
        sys_p = "[UNMERGE] Manual unmerge — §8.6.\n\n…"
        out.append({
            "task_type": "unmerge", "system_prompt": sys_p,
            "user_prompt": "Return the split JSON.",
            "output": {
                "splits": [
                    {"name": p1, "l0_abstract": f"{p1} — ML engineer at Meta.",
                     "triplets": [{"subject": p1, "relation": "works_at",
                                   "object": "Meta", "confidence": 0.9}]},
                    {"name": p2, "l0_abstract": f"{p2} — user's sister.",
                     "triplets": []},
                ]
            },
        })
    return out


def gen_gate_classifier(n: int, *, seed: int = 9) -> list[dict[str, Any]]:
    """Flat dataset for §14.1 training (the separate 33M BGE head).

    Format: { "query": "...", "label": 0|1 } — compatible with
    `engram.training.gate_classifier` out of the box. Always returns
    exactly `n` records; class balance is 50/50 (with one extra class-0
    record when `n` is odd).
    """
    rnd = _seed(seed)
    out = []
    half = n // 2
    for _ in range(half):
        out.append({"query": rnd.choice(_QUESTIONS_STANDALONE), "label": 0})
        out.append({"query": rnd.choice(_QUESTIONS_CONTEXT_DEPENDENT), "label": 1})
    if n % 2:
        out.append({"query": rnd.choice(_QUESTIONS_STANDALONE), "label": 0})
    rnd.shuffle(out)
    return out


# ----------------------------------------------------------------------
# Task registry
# ----------------------------------------------------------------------

_TASKS: dict[str, tuple[Callable[..., list[dict[str, Any]]], int]] = {
    "gate_write":       (gen_gate_write, 8_000),
    "extract":          (gen_extract, 15_000),
    "l1_plan":          (gen_l1_plan, 10_000),
    "ln_plan":          (gen_ln_plan, 10_000),
    "dedup":            (gen_dedup, 5_000),
    "entity_link":      (gen_entity_link, 5_000),
    "overview":         (gen_overview, 3_000),
    "session_compact":  (gen_session_compact, 2_000),
    "unmerge":          (gen_unmerge, 500),
    "gate_classifier":  (gen_gate_classifier, 70_000),
}


# ----------------------------------------------------------------------
# Writer + validator
# ----------------------------------------------------------------------

def validate_record(rec: dict[str, Any]) -> None:
    """Raise ValueError if a record doesn't satisfy its declared shape.

    Two accepted shapes:
      1. SFT record: {task_type, system_prompt, user_prompt, output}
      2. Gate-classifier record: {query, label} (label ∈ {0, 1})
    """
    # Shape 2 — gate classifier
    if "query" in rec and "label" in rec:
        if rec["label"] not in (0, 1):
            raise ValueError(f"invalid label {rec['label']}")
        return

    # Shape 1 — SFT record
    tt = rec.get("task_type")
    for k in ("task_type", "system_prompt", "user_prompt", "output"):
        if k not in rec:
            raise ValueError(f"missing required key {k!r} in training record")
    if not isinstance(rec["output"], (dict, list, str)):
        raise ValueError(
            f"output must be dict/list/str for task {tt!r}, got {type(rec['output']).__name__}"
        )


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            validate_record(rec)
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(records)


def generate_all(
    out_dir: Path, *, counts: dict[str, int] | None = None, seed: int = 0,
) -> dict[str, int]:
    counts = counts or {}
    summary: dict[str, int] = {}
    for idx, (task, (generator, default_n)) in enumerate(_TASKS.items()):
        n = counts.get(task, default_n)
        records = generator(n, seed=seed + idx)
        wrote = write_jsonl(out_dir / f"{task}.jsonl", records)
        summary[task] = wrote
    return summary


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate synthetic training data for Engram's Core Model tasks.",
    )
    parser.add_argument("--out", required=True, type=Path,
                        help="Output directory (e.g. ./data/train)")
    parser.add_argument(
        "--count", action="append", default=[],
        help="Per-task override: --count gate_write=1000 (repeatable)",
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--tasks", default="all",
        help=f"Comma-separated subset; default=all. Available: {','.join(_TASKS)}",
    )
    args = parser.parse_args(argv)

    counts: dict[str, int] = {}
    for pair in args.count:
        if "=" not in pair:
            parser.error(f"--count expects task=N, got {pair!r}")
        task, n = pair.split("=", 1)
        counts[task.strip()] = int(n)

    if args.tasks != "all":
        selected = {t.strip() for t in args.tasks.split(",") if t.strip()}
        unknown = selected - set(_TASKS)
        if unknown:
            parser.error(f"unknown task(s): {sorted(unknown)}")
        subset = {k: v for k, v in _TASKS.items() if k in selected}
    else:
        subset = _TASKS

    summary: dict[str, int] = {}
    for idx, (task, (generator, default_n)) in enumerate(subset.items()):
        n = counts.get(task, default_n)
        records = generator(n, seed=args.seed + idx)
        wrote = write_jsonl(args.out / f"{task}.jsonl", records)
        summary[task] = wrote
    print(json.dumps({"out_dir": str(args.out), "generated": summary}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
