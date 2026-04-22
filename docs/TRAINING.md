# Training

Engram runs fully in **bootstrap mode** out of the box (§14.2): an API
frontier model stands in for the Core Model, and the L0 classifier is
replaced by the regex OR-gate + vector memory-hit fallback. Training is
optional and becomes worthwhile once you have production traces.

You don't need production traces to *try* training — the repo ships a
**synthetic data generator** that produces a realistic training corpus
for every task. Use it to verify the training pipeline runs on your
hardware, then swap in real traces when they accumulate.

```bash
# Install training extras (torch, transformers, peft, trl, accelerate)
pip install -e '.[training]'

# 1. Generate a full synthetic corpus (~60k records across all tasks)
engram train synth --out ./data/train

# 2. Train the L0 gate classifier (§14.1)
engram train gate --data ./data/train/gate_classifier.jsonl \
                  --out  ./models/engram-gate-v1 \
                  --epochs 5

# 3. SFT on Qwen3.5-0.8B (§14.2.4)
engram train sft --traces ./data/train \
                 --out    ./models/engram-core-v1 \
                 --epochs 3

# 4. Swap production over to the local model in config.yaml:
#      gating.classifier_path: ./models/engram-gate-v1
#      core_model.provider: local
#      core_model.model_path: ./models/engram-core-v1
```

## When to train

| Signal | Implication |
|---|---|
| Sustained L0 false-negatives on personal-memory queries | Train the edge-case L0 classifier (§14.1.4) |
| Core Model API spend > training TCO | Train the local Qwen core (§14.2) |
| Frontier `NEED_MORE` rate > 10% sustained | Core Model under-plans depth — retrain with DPO (§14.2.5) |
| Consolidation dedup wrong-merges climbing | Retrain the dedup prompt or classifier head |

## Training pipeline

```
                     ┌──────────────────────────┐
                     │  Trace collector         │
 production logs ───▶│  (engram.training.       │
                     │   trace_collector)       │
                     └──────────┬───────────────┘
                                │ traces.jsonl
             ┌──────────────────┼──────────────────┐
             ▼                                     ▼
  ┌───────────────────┐                ┌────────────────────┐
  │ L0 gate training  │                │ Core SFT           │
  │ (BGE-Small head)  │                │ (Qwen3.5-0.8B LoRA)│
  └─────────┬─────────┘                └─────────┬──────────┘
            │ models/engram-gate-v1              │ models/engram-core-v1
            │                                    ▼
            │                       ┌────────────────────┐
            │                       │ DPO                │
            │                       │ (winner/loser pairs)│
            │                       └─────────┬──────────┘
            │                                 │ models/engram-core-dpo
            ▼                                 ▼
          update config.yaml:        update config.yaml:
          gating.classifier_path     core_model.provider=local
                                     core_model.model_path=...
```

## Scripts

### Collect traces

```bash
python -m engram.training.trace_collector \
    --log-dir ./logs/core_model \
    --out ./data/traces.jsonl \
    --task gate_write,extract,l1_plan,ln_plan,dedup,entity_link,overview,session_compact,unmerge
```

Each log line is a JSON object with `task_type`, `system_prompt`,
`user_prompt`, `output`, `latency_ms`, and the downstream outcome (did the
frontier emit ANSWER on the first pass?).

### Train the L0 gate classifier (§14.1)

Per §14.1.2, combine these sources:

| Source | Size | Label |
|---|---|---|
| CANARD rewritten vs original | ~40K | 0 (standalone) / 1 (context-dependent) |
| Synthetic standalone factuals | ~10K | 0 |
| Synthetic conversational follow-ups | ~10K | 1 |
| Hard negatives (looks-dependent-is-standalone) | ~5K | 0 |
| Personal-memory probes | ~5K | 1 |

```bash
python -m engram.training.gate_classifier \
    --data ./data/gate_train.jsonl \
    --out ./models/engram-gate-v1 \
    --epochs 5 --lr 2e-5
```

Target metrics per §14.1.3:

* Recall on Class 1 at threshold 0.3: ≥ 0.97
* F1 at threshold 0.5: ≥ 0.92

Then update `config.yaml`:

```yaml
gating:
  configuration: "dual"
  classifier_path: "./models/engram-gate-v1"
  classification_threshold: 0.3
```

### Train the local Core Model (§14.2)

Phase B — SFT with LoRA:

```bash
python -m engram.training.core_sft \
    --traces ./data/traces.jsonl \
    --base Qwen/Qwen3.5-0.8B \
    --out ./models/engram-core-v1 \
    --epochs 3 --lr 1e-4 --lora-rank 64
```

Phase C — DPO over held-out queries (§14.2.5):

```bash
# 1. Sample 4 completions per query with the SFT model, score each by
#    downstream outcome (did the frontier emit ANSWER on the first pass?),
#    form (prompt, chosen, rejected) triples, and write them as jsonl.
python -m your.sampling.script \
    --model ./models/engram-core-v1 \
    --queries ./data/holdout_queries.txt \
    --out ./data/dpo_pairs.jsonl

# 2. DPO
python -m engram.training.core_dpo \
    --sft-model ./models/engram-core-v1 \
    --held-out ./data/dpo_pairs.jsonl \
    --out ./models/engram-core-dpo \
    --beta 0.1 --lr 5e-6 --epochs 1
```

Acceptance criteria (§14.2.5):

* End-to-end answer correctness ≥ 90% of the bootstrap baseline.
* Frontier NEED_MORE re-entry rate < 10%.
* Shallow-bias under-prediction rate ≤ 15%.

Update `config.yaml`:

```yaml
core_model:
  provider: "local"                        # swap from "anthropic"
  model_path: "./models/engram-core-dpo"
  temperature: 0.1
  max_tokens: 2048
```

Restart Engram. No data migration needed — the Core Model is just another
completion endpoint to the ingest + orchestrator code.

## Continuous improvement loop (§14.2.6)

* **Weekly**: collect frontier-NEED_MORE / FAILED cases. Rerun the frontier
  as the Core Model on those cases to produce new SFT data.
* **Monthly**: re-run SFT on the accumulated data. Re-run DPO if the
  under-prediction rate has drifted above 15%.
* **Per-task metrics**: track schema validity per task type. If any task
  degrades beyond baseline, increase its sampling weight in the next
  training run.

## Data privacy

Traces contain user prompts and assistant responses. Scrub them before
training:

* Replace `source_session_id` with a hashed surrogate.
* PII-redact the user content (name/email/phone patterns).
* Drop any event where the user invoked `DELETE /api/v1/sessions/{id}` with
  a privacy flag.

Engram does not ship a scrubber — bring your own based on deployment
policy.
