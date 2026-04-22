"""Collect Core-Model call traces from structured logs (§14.2.6).

Reads the JSON-line log produced by the CoreModelProvider wrapper and emits a
JSONL file suitable as SFT input:

    { "task_type": "l1_plan",
      "system_prompt": "...",
      "user_prompt": "...",
      "output": { ... },
      "downstream_outcome": "ANSWER" | "NEED_MORE" | "FAILED",
      "latency_ms": 1120 }

Usage:
    python -m engram.training.trace_collector \
        --log-dir ./logs/core_model \
        --out ./data/traces.jsonl \
        --task gate_write,extract,l1_plan
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def collect(log_dir: Path, out: Path, tasks: set[str]) -> int:
    out.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out.open("w", encoding="utf-8") as fh:
        for log_file in sorted(log_dir.rglob("*.jsonl")):
            for line in log_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("task_type") not in tasks:
                    continue
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
                count += 1
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect Core Model traces for SFT")
    parser.add_argument("--log-dir", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--task",
        default="gate_write,extract,l1_plan,ln_plan,dedup,entity_link,overview,session_compact,unmerge",
    )
    args = parser.parse_args(argv)
    tasks = {t.strip() for t in args.task.split(",") if t.strip()}
    n = collect(args.log_dir, args.out, tasks)
    print(json.dumps({"traces_written": n, "out": str(args.out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
