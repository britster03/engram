"""Direct Preference Optimisation for the Core Model (§14.2.5).

Generates 4 completions per held-out query at temperature 0.7, scores each
by downstream outcome (did the frontier emit ANSWER on the first pass?),
and trains a DPO pass over (winner, loser) pairs.

Acceptance criteria after DPO (§14.2.5):
  - end-to-end answer correctness ≥ 90% of the Phase-A frontier-as-Core baseline
  - frontier NEED_MORE re-entry rate < 10%
  - shallow-bias under-prediction rate ≤ 15%

Usage:
    python -m engram.training.core_dpo \
        --sft-model ./models/engram-core-v1 \
        --held-out ./data/dpo_queries.jsonl \
        --out ./models/engram-core-dpo
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def train(sft_model: Path, held_out: Path, out_dir: Path, *,
          beta: float = 0.1, lr: float = 5e-6, epochs: int = 1) -> None:
    # Deferred imports — ML deps are optional.
    from datasets import Dataset  # type: ignore
    from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
    from trl import DPOTrainer, DPOConfig  # type: ignore

    rows = [json.loads(line) for line in held_out.read_text().splitlines() if line]
    # Each row is expected to carry (prompt, chosen, rejected) after the
    # sampling step scored 4 completions per query by downstream outcome.
    ds = Dataset.from_list(rows)

    tokenizer = AutoTokenizer.from_pretrained(sft_model)
    model = AutoModelForCausalLM.from_pretrained(sft_model)

    cfg = DPOConfig(
        output_dir=str(out_dir), beta=beta, learning_rate=lr, num_train_epochs=epochs,
        per_device_train_batch_size=2, gradient_accumulation_steps=16,
        warmup_ratio=0.1, lr_scheduler_type="cosine", logging_steps=10, save_steps=500,
        bf16=True, report_to="none", max_prompt_length=1024, max_length=2048,
    )
    trainer = DPOTrainer(model=model, args=cfg, train_dataset=ds, tokenizer=tokenizer)
    trainer.train()
    trainer.save_model(str(out_dir))
    print(f"saved DPO model to {out_dir}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sft-model", required=True, type=Path)
    parser.add_argument("--held-out", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--beta", type=float, default=0.1)
    parser.add_argument("--lr", type=float, default=5e-6)
    parser.add_argument("--epochs", type=int, default=1)
    args = parser.parse_args(argv)
    train(args.sft_model, args.held_out, args.out, beta=args.beta, lr=args.lr,
          epochs=args.epochs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
