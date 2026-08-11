# Engram LoCoMo benchmark

The LoCoMo runner is fail-closed: it creates an expected-work manifest before
execution, waits for exact event IDs, writes one terminal row per expected
question, and marks headline metrics valid only when the result is complete.
Normalized partial-match answer F1 and evidence Recall@5/10/25 are primary;
the optional LLM judge is a secondary diagnostic.

## Required gates

Run the same seeded/category-balanced question IDs at every retrieval gate:

1. `no_memory`: frontier-only quality floor.
2. `vector_only`: one raw-query vector search, no semantic planner/cascade.
3. `forced` with `--max-depth L2`: planned L1 plus graph/AGFS L2.
4. `forced` with `--max-depth L4`: full forced cascade.
5. `adaptive`: normal L0 routing, used to compare classifier off/shadow/active
   on separately configured servers.

Do not compare modes that use different dataset hashes, selected question IDs,
corpus tenants, model/prompt/config hashes, or seeds.

## Corpus and query runs

Create a corpus once with a stable run ID:

```bash
python benchmarks/run_locomo.py \
  --data benchmarks/data/locomo10.json \
  --dataset-commit 3eb6f2c585f5e1699204e3c3bdf7adc5c28cb376 \
  --limit-convs 1 \
  --limit-questions 25 \
  --retrieval-mode forced \
  --max-depth L2 \
  --seed 42 \
  --tenant-prefix locomo \
  --run-id gate2-corpus
```

After the manifest confirms the corpus is memory-ready, reuse it without
reingestion:

```bash
python benchmarks/run_locomo.py \
  --data benchmarks/data/locomo10.json \
  --dataset-commit 3eb6f2c585f5e1699204e3c3bdf7adc5c28cb376 \
  --limit-convs 1 \
  --limit-questions 25 \
  --corpus-run-id gate2-corpus \
  --retrieval-mode vector_only \
  --seed 42 \
  --tenant-prefix locomo \
  --run-id gate2-vector-only
```

The runner needs `ENGRAM_ADMIN_KEY` in its environment to create a tenant or
mint a key for a versioned corpus tenant. Provider credentials remain server
side unless the optional secondary judge is enabled.

## Validity rules

- Never report a row or aggregate from a manifest whose status is
  `INCOMPLETE`.
- Never calculate a headline score from an undocumented partial subset.
- Treat `memory_ready` and `overview_ready` as separate signals.
- Preserve raw predictions, redacted traces, expected-work manifest, summary,
  and completeness report together.
- Record the Engram commit/dirty flag and LoCoMo dataset hash/upstream commit.
- Keep LoCoMo use noncommercial and follow `benchmarks/NOTICE.md`.
