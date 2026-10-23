# Engram LoCoMo lifecycle benchmark

## Descriptive question reports

`question_report.html` is an offline searchable question index. Each question
links its original wording, reference and Engram answers, official score, source
records, complete store returns, exact model requests, raw responses, and token
usage. Related session-routing and retrieval comparisons have their own detail
pages. Core and Frontier roles are recorded separately even when both use the
same model. Full Unicode and multiline text is retained in per-evaluation JSON.

`management.csv` retains its original columns and adds explicit input/output,
known, estimated and unknown usage, answer-producing consumption and detail
links. `management_compact.csv` bounds only context previews for spreadsheet
applications; complete data remains in the linked detail file. `model_calls.csv`
contains one row per SDK attempt, including retries and prior submissions.

Whole-query consumption includes planning, semantic checks and generation.
Answer-producing consumption includes all attempts in operations observed to
return an answer. Final-answer consumption requires verified linkage to the
actual returned answer; deterministic rendering uses no Frontier generation.
Provider usage is authoritative. Estimates remain separate, unknown billing is
never zero, and reasoning/cache tokens are subsets. Report generation uses no
additional model calls. New runs require capture and token reconciliation before
publication; older records label unavailable fields rather than inventing them.

Regenerate a historical run into a separate directory without modifying its
original journals or reports:

```bash
.venv/bin/python -m benchmarks.lifecycle.descriptive \
  benchmarks/results/lifecycle/20261008-token-compaction-v4/pilot \
  --output benchmarks/results/descriptive/20261008-token-compaction-v4
```

For fresh runs, use the existing `--profile pilot-then-full` command with
`--build` and a new `--image-tag`. This executes 193 pilot, 387 validation, and
15,387 full evaluations with seed 42. Native configuration remains the
experimental baseline; image/source identities must match to resume.

The reviewed [hqfang/MemoryBench](https://huggingface.co/datasets/hqfang/memorybench)
dataset evaluates robotic spatial/action memory. It is excluded from this
conversational QA protocol; these results are Engram LoCoMo results.

This harness measures native ingestion and independent, non-persistent queries. All adapters and instrumentation live here; production APIs, schema, prompts and models are unchanged. Services have private Compose networks, volumes, tags and credentials, with no published ports. LoCoMo utterances contain both human speakers as user-side records; the assistant side is empty. Original timestamps, speaker names and BLIP captions are retained. Observations, supplied summaries, QA questions and answers are excluded from ingestion.

Use the repository virtual environment for the host CLI (PyYAML is required), Docker Compose, and existing configured model credentials. Build the separate images once:

```bash
python -m benchmarks.lifecycle --build --profile pilot-then-full --config config.prod.yaml
```

Subsequent runs use the same command without `--build`. Where `python` is not on PATH, use `.venv/bin/python`. Each phase receives a separate result directory under `benchmarks/results/lifecycle/`. To resume an existing phase:

```bash
.venv/bin/python -m benchmarks.lifecycle --profile pilot --config config.prod.yaml --run-dir benchmarks/results/lifecycle/20261001-120000/pilot
```

Source, image and configuration identities must match to resume. A host lock prevents duplicate controllers. Accepted event IDs are deterministic per tenant/session/turn. Resume skips committed sessions and reuses accepted events. Completed evaluation rows are never repeated. With writers stopped, torn journal tails are preserved in diagnostic files and interrupted provider attempts receive explicit terminal records with unknown usage. Their known consumption is included in subsequent per-question totals. Credentials are stored with mode 0600 under the ignored `.runtime/` directory and never copied into reports.

The rollout runs a 25-question pilot, 50-question validation and full benchmark with seed 42. Full coverage is 1,986 canonical questions, 3,471 session-routing checkpoints and 9,930 retrieval-only comparisons. Pilot and validation execute the same three tracks for their selected questions. Low answer scores do not stop evaluation. Failed isolation, readiness, state fingerprints or accounting block publication and later rollout stages.

The host controls Temporal worker/dispatcher quiescence. Ingestion supplies the native ten-turn conversation tail to extraction and native compaction to the gate. Read-only Temporal histories verify that dispatched workflows completed and record native activity durations, last attempts and folded-retry limitations in `activity_timings.csv`. Model-operation latencies remain separate. Active session snapshots are saved before native session commit; completed snapshots follow final commit and ready projection. Active checkpoint questions run immediately while writers are paused, then ingestion resumes; the completed-conversation questions run after final commit. Retrieval restores a verified checkpoint, clears private Redis caches and runs PostgreSQL reads in a read-only repeatable-read transaction. Regeneration enqueue requests are recorded and suppressed. Semantic PostgreSQL/Neo4j fingerprints are compared after every question and after semantic audits. Future sessions must be absent. Readiness checks completed native outbox publications and online indexes; ancestor revision changes without a new publication request are recorded as canonical projection lag, leaving native rejection behavior observable. Snapshots preserve SQL NULL separately from JSON null. Routing compatibility uses actual source-store reads and equivalent canonical evidence, rather than counting an unsupported tool invocation as a read.

Native tracks use `run_query` and preserve native retries, timing, routing and prompts. Comparison tracks use the deterministic planner, 20 candidates per discovery channel, RRF 60, native hydration, canonical verification, ranking and whole-claim/path packing. BM25 and vector are isolated by hiding the other discovery capability. KG uses native bounded paths. Hybrid+KG merges hybrid results first, then shortest path bundles, preserving shared-claim bundle membership. No comparison answers or learned reranker are added.

Each actual SDK attempt is journalled before parsing. Successful responses use returned usage, including malformed outputs and JSON corrections. Responses without usage use explicitly labelled tokenizer estimates; those estimates are excluded from known provider subtotals. Transport failures with unknown billing keep usage unavailable. Reasoning, cached reads and cache writes are subsets of output/input, never extra totals. Ingestion, query and semantic-audit consumption are separate. Native usage metadata is retained in traces but never added to SDK totals. Local BERTScore encoder calls record actual padded and unpadded input positions, separately from provider billing. Shared batch embeddings are not assigned invented per-question token costs.

Headline accuracy uses the unchanged official scorer at [upstream revision 3eb6f2c](https://github.com/snap-research/locomo/blob/3eb6f2c585f5e1699204e3c3bdf7adc5c28cb376/task_eval/evaluation.py), vendored with attribution and its CC BY-NC 4.0 license. Native abstention wording is not rewritten to improve scores. ROUGE-L, pinned-snapshot RoBERTa-large BERTScore and composite `.4*ROUGE-L + .6*clipped BERTScore` are secondary diagnostics. Adversarial secondary similarities are unavailable. Separate model judgments compare original gold dialogue to sourced canonical artifacts, validate exact cited quotes, and do not affect retrieval or official scores.

`management.csv` has one canonical row per selected question: expected/actual answers, official score, evidence coverage, semantic preservation, terminal stage, total query tokens, seconds and status. Technical CSVs add token phases, retries, models, routes, IDs and phase timings. Ingestion counts use the actual canonical `extract-v1` and `typed-v1` artifacts; absent extraction measurements remain unavailable. Post-packing evidence maps the exact observed model-context suffix. Partly truncated dialogue blocks remain incomplete, and earlier failed sufficiency calls are excluded from final supplied-context metrics. Unknown scalars are empty cells with status fields; arrays/maps are JSON strings. All expected questions, including failures and unexecuted blocks, remain visible. Exact evidence recall requires exact event/claim/version lineage; summaries with only session provenance produce labelled lower bounds. Actual model claim attribution and scanned graph node counts are unavailable.

The `Retrieved Data / Frontier Context` management column is JSON. Its `postgresql`, `neo4j`, and `redis` lists show source-labelled operations with readable returned text, IDs, and ranking values. Repeated calls to the same operation at one stage are grouped with `read_count` and `distinct_result_count`; lists show their total and up to ten preview items, with omitted items available in `trace_archive`. `frontier_calls[].context` is the exact text passed to each frontier answer call, including earlier answerability calls; `submitted` indicates whether an SDK attempt was observed. A database candidate may be filtered before the model sees it. Empty lists mean no read or model call occurred; failed reads have an error and unknown returned count. `trace_archive` links the lossless raw store results for that question.

Open `summary.html` or `summary.md` for linked category results, search comparisons and reports. `progress.json` is atomic and updates after events/evaluations; console heartbeats run every 30 seconds. Service failures are recorded promptly. Provisional reports regenerate at evaluation boundaries at least five minutes apart and explicitly state completion denominators. Final confidence intervals use 2,000 conversation bootstrap samples; provisional intervals use 200, with seed 42. `provider_attempts.*.jsonl`, `evaluation_attempts.jsonl` and trace files are append-only. `accounting.json` reconciles attempts and question rows. `local_scoring_accounting.json` records local evaluation work. `verified_blocks/` contains semantic-state checks. Isolated writers and the driver stop after each phase; data services and volumes remain available for diagnostics.

Heartbeats and service checks continue during CPU secondary scoring. `unknown_usage_calls` covers all lifecycle provider attempts; `query_unknown_usage_calls` covers completed question rows separately. Recovery preserves interrupted local encoder inputs and labels their missing latency, alongside interrupted provider attempts with unknown billing. Neither is silently recorded as a successful call.

`lifecycle_metrics.json` separates ingestion and query embedding/cache activity and reports ingestion outcomes, known/unknown extracted counts and p50/p95/p99 event/stage timings. Event throughput uses verified per-session acceptance-to-completion spans, excluding QA pauses between sessions; it does not claim to include later session commit or consolidation readiness.

On successful phase completion, private data services stop to release memory before the next phase; their volumes and snapshots remain intact. The terminal progress status retains `COMPLETE` or `COMPLETE_WITH_ERRORS`. Interrupted blocks remain `INCOMPLETE` or `STOPPED`.

Large question/audit traces are stored losslessly in `trace_payloads/*.json.xz`, with checksums and relative links in the append-only `trace.jsonl` index. Compression uses standard-library LZMA2, fast preset 1 and an 8 MiB dictionary, after native query timing ends. The index retains usage and embedding/cache events for accounting. Final reporting verifies archive checksums and index parity. Use `benchmarks.lifecycle.ledger.expand_trace(run_directory, index_row)` to verify and read all events. Earlier inline records remain readable and unchanged.

When provenance leaves unknown dialogue positions, reports retain only unbounded any/all/fraction coverage as lower bounds. Ranked lower bounds at K and MRR are unavailable because unknown preceding evidence can change the cutoff order. Exact metrics keep their original eligibility rules.

Run validation inside the isolated stack through the CLI. Unit tests cover dataset fidelity, official scoring parity, annotation limitations, coverage/packing, SDK corrections and unknown usage, secret exclusion, complete denominators and serialization. Integration checks cover PostgreSQL and Neo4j, checkpoint restoration, future-session absence and read-only queries. The manifest captures dirty source hashes, config/dataset/upstream/model identities, native timeout settings, dependencies, cache policy and hardware. Published completion requires reconciled accounting, validated isolation, full coverage and all expected state proofs. Fully executed runs with query or audit failures are labelled `COMPLETE_WITH_ERRORS`; those failures stay visible and do not prevent later rollout phases. Missing coverage, scoring work or state proofs is `INCOMPLETE` and stops rollout. Low scores remain measured outcomes. Retrieval coverage remains available when retrieval succeeded and answer generation failed.
