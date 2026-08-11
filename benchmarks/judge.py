"""LLM-as-judge scorer for the LoCoMo baseline.

LoCoMo answers are free-form ("Psychology, counseling certification"), so a
string match cannot decide correctness -- Engram may phrase the same fact
completely differently. We ask a model to grade semantic equivalence instead.

Judge model: Kimi via Ollama Cloud, using the same native /chat + format=json
call shape Engram itself uses (see engram/models/providers/ollama_cloud.py).
Kept dependency-free of Engram internals so the harness runs on its own.

Two grading modes:

* Normal questions -> correct iff the prediction conveys the reference answer
  (value-level match for dates/numbers, meaning-level for text).

* Adversarial questions (category 5, ~446 of them) -> the question is NOT
  answerable from the conversation, so the ONLY correct behaviour is abstention.
  A confident fabricated answer is marked wrong even though it is fluent text.
  This is the abstention signal that maps onto Engram's frontier "can't answer"
  path, so grading it correctly matters.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

import httpx

# Defaults mirror config.yaml so the judge lines up with Engram's own provider.
DEFAULT_BASE = "https://ollama.com/api"
DEFAULT_MODEL = "kimi-k2.7-code:cloud"


@dataclass
class JudgeResult:
    correct: bool
    reason: str
    raw: str = ""          # raw judge text, kept for auditing disagreements
    judged: bool = True    # False if the judge call failed and we defaulted


_NORMAL_SYSTEM = (
    "You are a strict grader for a long-term-memory question-answering "
    "benchmark. You are given a question, a reference (gold) answer, and an "
    "assistant's answer. Decide whether the assistant's answer is CORRECT.\n\n"
    "Rules:\n"
    "- Judge meaning, not wording. Paraphrases and extra detail are fine as "
    "long as the reference fact is clearly present and not contradicted.\n"
    "- Dates and numbers must match in value (e.g. '7 May 2023' == 'May 7th, "
    "2023'; '2022' != '2021').\n"
    "- If the assistant hedges but still states the correct fact, it is "
    "correct. If it gives the wrong fact or says it does not know, it is "
    "incorrect.\n\n"
    'Respond with ONLY a JSON object: {"correct": true|false, "reason": '
    '"one short sentence"}.'
)

_ADVERSARIAL_SYSTEM = (
    "You are a strict grader for a long-term-memory benchmark. This question "
    "is UNANSWERABLE from the conversation -- the information was never "
    "provided. The assistant's answer is CORRECT only if it ABSTAINS: says it "
    "does not know, that the information is not available/mentioned, or "
    "otherwise declines to give a specific factual claim.\n\n"
    "Rules:\n"
    "- Correct: 'I don't have that information', 'not mentioned', 'the "
    "conversation doesn't say', an explicit refusal, or a clearly hedged "
    "non-answer.\n"
    "- Incorrect: any confident specific factual answer (a name, date, place, "
    "number, etc.), even if fluent.\n"
    "- The reference note describes why it is unanswerable; use it as context, "
    "not as a required string.\n\n"
    'Respond with ONLY a JSON object: {"correct": true|false, "reason": '
    '"one short sentence"}.'
)


class OllamaJudge:
    """Grades predictions with Kimi via Ollama Cloud's native chat API."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE,
        model: str = DEFAULT_MODEL,
        timeout_s: float = 60.0,
        max_retries: int = 3,
    ) -> None:
        api_key = api_key or os.environ.get("OLLAMA_API_KEY")
        if not api_key:
            raise RuntimeError("OLLAMA_API_KEY is required for the judge")
        self.model = model
        self.max_retries = max_retries
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_s,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> OllamaJudge:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _chat(self, system: str, user: str) -> str:
        """One deterministic JSON chat call, with a small retry on transport."""
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.0, "num_predict": 256},
        }
        last_err: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self._http.post("/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
                content = ""
                msg = data.get("message")
                if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                    content = msg["content"]
                elif isinstance(data.get("response"), str):
                    content = data["response"]
                # An empty 200 reply (the model returned nothing) is transient —
                # treat it like a transport error and retry rather than surfacing
                # it as an unparseable grade.
                if content.strip():
                    return content
                last_err = RuntimeError("empty judge response")
            except (httpx.HTTPError, json.JSONDecodeError) as err:
                last_err = err
            if attempt < self.max_retries:
                time.sleep(0.5 * (attempt + 1))
        raise RuntimeError(f"judge chat failed after retries: {last_err}")

    @staticmethod
    def _parse(raw: str) -> tuple[bool, str] | None:
        """Extract {correct, reason} from the judge's JSON reply."""
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            # Salvage a JSON object embedded in stray prose.
            start, end = raw.find("{"), raw.rfind("}")
            if start == -1 or end <= start:
                return None
            try:
                obj = json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                return None
        if not isinstance(obj, dict) or not isinstance(obj.get("correct"), bool):
            return None
        reason = obj.get("reason", "")
        if not isinstance(reason, str):
            return None
        return obj["correct"], reason

    def judge(
        self,
        *,
        question: str,
        gold_answer: str,
        predicted_answer: str,
        is_adversarial: bool,
    ) -> JudgeResult:
        """Grade one prediction. Never raises -- a failed judge call returns
        JudgeResult(correct=False, judged=False) so one bad call can't abort a
        full benchmark run; those rows are visibly flagged for re-judging."""
        system = _ADVERSARIAL_SYSTEM if is_adversarial else _NORMAL_SYSTEM
        base_user = (
            f"Question:\n{question}\n\n"
            f"Reference answer:\n{gold_answer or '(none)'}\n\n"
            f"Assistant's answer:\n{predicted_answer or '(empty)'}"
        )
        # Up to two attempts: some replies come back as non-JSON (stray prose or
        # a truncated object). A second, stricter ask recovers most of them
        # before we give up and flag the row for re-judging.
        last_raw = ""
        for attempt in range(2):
            user = base_user if attempt == 0 else (
                base_user
                + '\n\nReturn ONLY this JSON object and nothing else: '
                '{"correct": true or false, "reason": "one short sentence"}'
            )
            try:
                raw = self._chat(system, user)
            except RuntimeError as err:
                return JudgeResult(
                    correct=False, reason=f"judge-error: {err}", raw="", judged=False
                )
            last_raw = raw
            parsed = self._parse(raw)
            if parsed is not None:
                correct, reason = parsed
                return JudgeResult(correct=correct, reason=reason, raw=raw)
        return JudgeResult(
            correct=False, reason="judge-unparseable", raw=last_raw, judged=False
        )


if __name__ == "__main__":
    # Self-test with fixed cases so you can eyeball the judge before a real run.
    # Needs OLLAMA_API_KEY in the environment.
    cases = [
        # (question, gold, predicted, is_adversarial, expected_correct)
        ("When did Caroline go to the support group?", "7 May 2023",
         "She went on May 7th, 2023.", False, True),
        ("When did Melanie paint a sunrise?", "2022",
         "In 2021.", False, False),
        ("What is Caroline's dog's name?", "(unanswerable)",
         "I don't think that was mentioned in the conversation.", True, True),
        ("What is Caroline's dog's name?", "(unanswerable)",
         "Her dog's name is Rex.", True, False),
    ]
    with OllamaJudge() as judge:
        passed = 0
        for q, gold, pred, adv, expected in cases:
            r = judge.judge(
                question=q, gold_answer=gold,
                predicted_answer=pred, is_adversarial=adv,
            )
            ok = r.correct == expected
            passed += ok
            tag = "PASS" if ok else "FAIL"
            print(f"[{tag}] correct={r.correct} (expected {expected}) "
                  f"adv={adv} :: {r.reason}")
        print(f"\n{passed}/{len(cases)} self-test cases behaved as expected")
