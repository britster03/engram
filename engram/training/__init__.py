"""Engram training pipeline.

Five scripts, each runnable standalone:

  * `engram.training.synthetic_data`  — generate training JSONL (no ML deps).
  * `engram.training.trace_collector` — dump production traces to JSONL.
  * `engram.training.gate_classifier` — train the §14.1 33M BGE head (torch + transformers).
  * `engram.training.core_sft`        — LoRA fine-tune Qwen3.5-0.8B (torch + transformers + peft).
  * `engram.training.core_dpo`        — Direct Preference Optimisation (trl).

Heavy ML dependencies (torch, transformers, peft, trl, accelerate) are
declared in the `[training]` extras of pyproject.toml. The scripts import
those deps lazily so the rest of Engram stays light.

Quick-start end-to-end using only synthetic data (no production traces
needed):

  python -m engram.training.synthetic_data --out ./data/train
  python -m engram.cli train gate  --data   ./data/train/gate_classifier.jsonl --out ./models/engram-gate-v1
  python -m engram.cli train sft   --traces ./data/train                       --out ./models/engram-core-v1
  python -m engram.cli train dpo   --sft-model ./models/engram-core-v1         --out ./models/engram-core-dpo
"""

from engram.training import synthetic_data

__all__ = ["synthetic_data"]
