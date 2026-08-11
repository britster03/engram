from __future__ import annotations

from benchmarks.judge import OllamaJudge


def test_judge_parser_accepts_only_json_booleans() -> None:
    assert OllamaJudge._parse('{"correct": false, "reason": "wrong"}') == (
        False,
        "wrong",
    )
    assert OllamaJudge._parse('{"correct": true, "reason": "right"}') == (
        True,
        "right",
    )
    assert OllamaJudge._parse('{"correct": "false", "reason": "wrong"}') is None
    assert OllamaJudge._parse('{"correct": 1, "reason": "wrong"}') is None
    assert OllamaJudge._parse('{"correct": false, "reason": 3}') is None
