"""Bounded rolling summaries. Original messages remain authoritative."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from pydantic import BaseModel, ConfigDict, field_validator

from engram import prompts
from engram.models.core import CoreModelError
from engram.models.request_context import model_request_session
from engram.tokens import count_tokens_exact


class CompactionOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    compacted: str
    key_facts: list[str]
    key_entities: list[str]

    @field_validator("compacted")
    @classmethod
    def nonempty(cls, value):
        if not value.strip():
            raise ValueError("summary must not be empty")
        return value


USER_PROMPT = "Return the complete rolling session summary JSON."


def render_prompt(history, previous, budget):
    return prompts.render("session_compact", turn_history=history,
                          previous_summary=previous, compaction_budget=budget)


def input_tokens(prompt):
    # Providers append the schema to their instructions. Reserve framing too.
    return count_tokens_exact(prompt + USER_PROMPT + json.dumps(CompactionOutput.model_json_schema(), indent=2)) + 256


def summarize_turns(core, turns, *, session_id, cfg, core_output_tokens=2048, previous=None):
    """Greedily batch whole messages, segmenting only a message that cannot fit.

    Character offsets refer to the unmodified original message. Publication is
    the caller's responsibility, after every segment has produced valid output.
    """
    history = []
    output = previous or {"compacted": "", "key_facts": [], "key_entities": []}
    calls = 0
    if previous and count_tokens_exact(json.dumps(previous, ensure_ascii=False)) > cfg.compaction_summary_tokens:
        output, calls = summarize_turns(core, [_SummaryTurn(str(previous))],
            session_id=session_id, cfg=cfg, core_output_tokens=core_output_tokens)

    def prompt_for(items):
        return render_prompt(items, output, cfg.compaction_summary_tokens)

    def flush():
        nonlocal output, calls
        if not history:
            return
        prompt = prompt_for(history)
        if input_tokens(prompt) > cfg.compaction_input_tokens:
            raise CoreModelError("session compaction prompt exceeds its input budget")
        with model_request_session(session_id):
            result = core.complete(system_prompt=prompt, user_prompt=USER_PROMPT,
                output_schema=CompactionOutput.model_json_schema(),
                max_tokens=max(core_output_tokens, 4096, 2 * cfg.compaction_summary_tokens))
        try:
            parsed = CompactionOutput.model_validate(result.output)
        except Exception as error:
            raise CoreModelError("invalid session compaction output") from error
        output = parsed.model_dump()
        if count_tokens_exact(json.dumps(output, ensure_ascii=False)) > cfg.compaction_summary_tokens:
            raise CoreModelError("session summary payload exceeds its token budget")
        calls += 1
        history.clear()

    for turn in turns:
        item = asdict(turn)
        item.update(char_start=0, char_end=len(turn.content))
        if input_tokens(prompt_for([*history, item])) <= cfg.compaction_input_tokens:
            history.append(item)
            continue
        flush()
        if input_tokens(prompt_for([item])) <= cfg.compaction_input_tokens:
            history.append(item)
            continue
        offset = 0
        while offset < len(turn.content):
            lo, hi, best = 1, len(turn.content) - offset, 0
            while lo <= hi:
                mid = (lo + hi) // 2
                segment = {**item, "content": turn.content[offset:offset + mid],
                           "char_start": offset, "char_end": offset + mid}
                if input_tokens(prompt_for([segment])) <= cfg.compaction_input_tokens:
                    best, lo = mid, mid + 1
                else:
                    hi = mid - 1
            if not best:
                raise CoreModelError("session compaction input budget cannot fit a source segment")
            history.append({**item, "content": turn.content[offset:offset + best],
                            "char_start": offset, "char_end": offset + best})
            flush()
            offset += best
    flush()
    return output, calls


@dataclass
class _SummaryTurn:
    content: str
    role: str = "derived_summary"
    turn_idx: int = -1
    timestamp: str = ""
