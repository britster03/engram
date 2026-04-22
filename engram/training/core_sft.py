"""Supervised fine-tune for the Core Model (§14.2.4).

Applies LoRA to Qwen3.5-0.8B using trace data. Accepts either:

  * a single JSONL (`--traces file.jsonl`)
  * a directory of per-task JSONLs (`--traces ./data/train/`) — every
    `<task>.jsonl` file is concatenated (gate_classifier.jsonl is skipped
    because its shape is different)

Each input record must look like:

    { "task_type": "l1_plan",
      "system_prompt": "...",
      "user_prompt": "...",
      "output": { ... } }

Reasoning fields are stripped — the deployed model never emits chain of
thought. Target format is `<|system|>...<|user|>...<|assistant|>{output_json}`.

Heavy ML deps are imported lazily. Install with:

    pip install 'engram[training]'

Usage:
    python -m engram.training.core_sft \
        --traces ./data/train \
        --base Qwen/Qwen3.5-0.8B \
        --out ./models/engram-core-v1 \
        --epochs 3
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_traces(traces: Path) -> list[dict]:
    """Return every record across a file or directory of JSONLs."""
    rows: list[dict] = []
    if traces.is_file():
        files = [traces]
    elif traces.is_dir():
        files = [
            p for p in sorted(traces.glob("*.jsonl"))
            if p.stem != "gate_classifier"
        ]
    else:
        raise FileNotFoundError(f"--traces path not found: {traces}")

    from engram.training.synthetic_data import validate_record

    for f in files:
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            validate_record(rec)
            rows.append(rec)
    return rows


def _format_record(rec: dict) -> dict[str, str]:
    out = rec.get("output")
    if isinstance(out, dict):
        out = {k: v for k, v in out.items() if k not in {"reasoning", "chain_of_thought"}}
        out_text = json.dumps(out, ensure_ascii=False)
    elif isinstance(out, str):
        out_text = out
    else:
        out_text = json.dumps(out, ensure_ascii=False)
    return {
        "text": (
            f"<|system|>{rec['system_prompt']}\n"
            f"<|user|>{rec['user_prompt']}\n"
            f"<|assistant|>{out_text}"
        )
    }


def train(
    traces_path: Path, base_model: str, out_dir: Path, *,
    epochs: int = 3, lr: float = 1e-4, batch_size: int = 8,
    grad_accum: int = 8, lora_rank: int = 64, max_seq: int = 2048,
) -> None:
    try:
        from datasets import Dataset  # type: ignore
        from peft import LoraConfig, get_peft_model  # type: ignore
        from transformers import (  # type: ignore
            AutoModelForCausalLM, AutoTokenizer, Trainer, TrainingArguments,
        )
    except ImportError as err:
        raise SystemExit(
            "Training deps not installed. Run:\n"
            "    pip install 'engram[training]'\n"
            f"(missing: {err.name})"
        ) from err

    rows = load_traces(traces_path)
    if not rows:
        raise SystemExit(f"no training records found under {traces_path}")
    print(f"loaded {len(rows)} training records")
    ds = Dataset.from_list([_format_record(r) for r in rows])

    tokenizer = AutoTokenizer.from_pretrained(base_model)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(base_model)
    lora = LoraConfig(
        r=lora_rank, lora_alpha=2 * lora_rank, bias="none",
        task_type="CAUSAL_LM", target_modules="all-linear",
    )
    model = get_peft_model(model, lora)

    def tokenize(ex):
        return tokenizer(ex["text"], truncation=True, max_length=max_seq)

    ds_tok = ds.map(tokenize, batched=True, remove_columns=["text"])
    args = TrainingArguments(
        output_dir=str(out_dir), num_train_epochs=epochs, learning_rate=lr,
        per_device_train_batch_size=batch_size, gradient_accumulation_steps=grad_accum,
        warmup_ratio=0.1, lr_scheduler_type="cosine", logging_steps=20, save_steps=500,
        bf16=True, report_to="none",
    )
    trainer = Trainer(
        model=model, args=args, train_dataset=ds_tok, tokenizer=tokenizer,
    )
    trainer.train()
    model.save_pretrained(out_dir)
    tokenizer.save_pretrained(out_dir)
    print(f"saved SFT adapter to {out_dir}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--traces", required=True, type=Path,
                        help="JSONL file or directory of JSONL files")
    parser.add_argument("--base", default="Qwen/Qwen3.5-0.8B")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--lr", type=float, default=1e-4)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--grad-accum", type=int, default=8)
    parser.add_argument("--lora-rank", type=int, default=64)
    parser.add_argument("--max-seq", type=int, default=2048)
    args = parser.parse_args(argv)
    train(
        args.traces, args.base, args.out,
        epochs=args.epochs, lr=args.lr,
        batch_size=args.batch_size, grad_accum=args.grad_accum,
        lora_rank=args.lora_rank, max_seq=args.max_seq,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
