# Token-based session compaction

Session compaction counts the rolling summary and every uncompacted turn with `cl100k_base`. It starts at **64,000 tokens**, including role labels and separators. The old turn-count and character-count triggers are ignored and warn when explicitly configured.

## Configuration

```dotenv
ENGRAM_SESSION_COMPACTION_TRIGGER_TOKENS=64000
ENGRAM_SESSION_COMPACTION_TARGET_RATIO=0.5
ENGRAM_SESSION_COMPACTION_INPUT_TOKENS=64000
ENGRAM_SESSION_COMPACTION_SUMMARY_TOKENS=2000
ENGRAM_RETRIEVAL_QUALITY_MODE=enforce
```

These variables map to `session.compaction_trigger_tokens`, `session.compaction_target_ratio`, `session.compaction_input_tokens`, `session.compaction_summary_tokens`, and `retrieval.quality_mode`.

The loader resolves **process environment → selected dotenv file → YAML → typed defaults**. Set `ENGRAM_ENV_FILE` to select a dotenv file; otherwise the loader optionally reads `.env` beside the selected YAML. An explicitly selected missing or empty path fails. Dotenv values are used for existing `${VAR}` interpolation without changing process environment variables. Invalid or empty settings fail configuration validation. Integer budgets, ratio, and summary headroom are validated.

Restart APIs and workers after configuration changes. Retrieval packing and Frontier output limits are separate settings. The general retrieval default remains `legacy`; local dotenv, staging, production, and deployment examples enable `enforce`.

## Compaction and durability

The oldest contiguous complete exchanges are selected until the remaining tail plus reserved summary space fits the target. At the default ratio the target is 32,000 tokens. A native chat's accepted user message remains untouched while its response is running. The rolling summary replaces its predecessor; the entire structured payload must fit the summary budget.

The helper measures instructions, previous summary, source records, output schema, and framing reserve against the input budget. It batches large ranges and splits oversized messages by Unicode character boundaries, preserving source indices and character offsets. Final session summaries use the same bounded helper. Each request allows at least `max(core_model.max_tokens, 4096, 2 × summary_budget)` output tokens, while the accepted payload remains limited by the summary budget.

Accepted turns and tenant-scoped `SESSION_COMPACT` work are recorded in the same PostgreSQL transaction. The existing consolidation outbox, Temporal workflow, retries, generations, and duplicate suppression handle the task. Model calls run outside the session lock. Publication checks the source hash, complete previous summary, boundary, and session status under the session lock. Newer appended turns survive; stale results are discarded. Summary publication and the idempotent raw-turn ingestion handoff commit atomically.

Original turns and their ingestion eligibility remain unchanged. Compacted originals are excluded from active-context rendering. Derived answers remain in history without becoming canonical evidence. A failed model call, invalid output, or failed handoff leaves the published boundary and summary unchanged.

The snapshot records covered positions and source indices, encoding, source hash, before/after token counts, model-call count, and summary policy. Prometheus metrics record outcomes, token counts, and latency. Logs contain metadata rather than source text.

## Retrieval and replay

Evidence-controlled retrieval assesses support before generation and validates support afterward. Current-session-first ordering remains in place; an incomplete activity answer continues to historical original records. Summaries guide discovery. Explicit retrieval-depth limits still constrain the search.

Enumeration assessments distinguish cited, documented items from proof that a list is exhaustive. A supported subset can produce a qualified answer at the allowed search boundary; it cannot establish a complete list. Enumeration hits from derived conversation claims are resolved to their original speaker records in bounded, tenant-scoped batches. Missing or summary origins remain discovery context. For broad lists without a time window, packing reaches both the older and newer ends of history before moving inward. Explicit time scopes keep their existing ordering. Validated supporting records retain priority, and graph evidence bundles remain intact.

Benchmark preparation resolves effective environment values before experimental variants, then freezes typed settings into its isolated runtime YAML and manifest. Frozen configurations require an isolated LoCoMo project. Ambient compaction and quality variables do not alter a replay. Credentials remain in an untracked, permission-restricted runtime environment file.

Verified ingestion can be reused after an interrupted evaluation. Pass the source's frozen runtime YAML when verifying configuration identity. The default source guard rejects any changed engine file; an explicit `allow_sufficiency_policy_change=True` permits only the read-only sufficiency controller and records both hashes. Compaction, ingestion, models, schemas, and all other engine files must match. Every required checkpoint must be ready, free of projection lag, and checksum-verified. Replays link original ingestion accounting without billing or counting it twice.

## Tokenizer and rollout

`tiktoken==0.14.0` is declared and locked. The production image includes the `cl100k_base` cache and enables offline tokenizer initialization. Missing or corrupt cache files fail startup/readiness before a runtime download. Compaction never falls back to character estimates.

Deploy workers capable of processing `SESSION_COMPACT` **before deploying APIs that enqueue it**. APIs, durable chat, manual compaction, consolidation workers, and benchmarks use the shared session-manager factory. Kubernetes and Helm forward the same five allowed nonsecret settings. Keep dotenv files untracked and preserve credentials during edits.

`cl100k_base` counts estimate Muse Spark usage. Provider-reported usage is authoritative for benchmark accounting.

## Validation artifacts

Focused tests cover configuration precedence, frozen replay settings, exact threshold boundaries, a synthetic session above 64K, pair preservation, Unicode segmentation, rolling summary growth, malformed output, partial-batch failures, stale publication, concurrent appends, close fencing, atomic outbox and ingestion handoff, tenant isolation, duplicate execution, and Temporal activity recovery after a simulated worker restart.

The row-5 regression uses current-session adoption/figurine noise and historical pottery, camping, painting, and swimming evidence. Gold answers stay in tests and benchmark data.

The final candidate pilot is saved under `benchmarks/results/lifecycle/20261008-token-compaction-v4/pilot`. Its manifest, validation log, checkpoints, prompts, evidence, answers, provider usage, latency, and scores are retained there. Consult its final status and report before treating the pilot as accepted. Its 19 verified ingestion checkpoints come from the fresh v2 run, which retained all 419 original turns and completed all 19 sessions. The replay proof permits only the read-only sufficiency-policy change; original ingestion accounting is linked separately. Superseded v1–v3 artifacts remain available, including failed and successful full-conversation diagnostics.

Final validation: **70 focused checks and 211 harness checks passed**. The pilot executed **193/193 evaluations across 25 selected questions**, verified all 19 required checkpoint blocks, and reconciled provider and local scoring accounting. Its status is **COMPLETE_WITH_ERRORS**: one query exceeded its deadline; no semantic-audit errors were recorded. There were 22 failed provider calls without usage (20 query, 2 audit), so known token totals remain lower bounds. A 4 GiB benchmark-driver OOM was recovered by increasing only that isolated container to 10 GiB; the interrupted attempt remains accounted. The default benchmark driver now has the same 10 GiB limit. Production services were not modified.

The official completed-conversation row-5 answer includes painting, swimming, camping, and pottery, with no adoption/figurine substitution. Its official F1 is **0.45**, versus **0.0** in the saved failing row. See `row5-acceptance/acceptance.json`, `frontier-model-context.json`, and `original-records-supplied.txt` in the final pilot directory. This is one regression result, not full-dataset accuracy.

After the frozen run finished, a reporting correction added evidence-envelope support for the current and earlier quality policies, including original records reached through canonical discovery. All 68 model-backed rows were re-attributed offline, with **zero model submissions**. Answers, official scores, usage, latency, status, configuration, and trace hashes were protected and unchanged. Original rows and reporting sources remain archived; `analysis_manifest.json` and `attribution_correction.json` record the correction. The original run manifest remains preserved.
