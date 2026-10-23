# Retrieval, ingestion and consolidation quality policy

The new policy is opt-in. Existing configuration defaults to `legacy`; enabling
it is a measured rollout, not an assertion that LoCoMo accuracy has improved.
PostgreSQL remains authoritative, Neo4j remains a projection, and there are no
new tables, migrations or endpoints in this change.

## Configuration

Merge these settings into a complete configuration for an **isolated** run:

```yaml
canonical_memory:
  evidence_mode: enforce
  recover_transient_failures: true
consolidation:
  quality_mode: enforce
retrieval:
  quality_mode: enforce
  query_deadline_seconds: 300
  provider_attempt_limit: 32
  sufficiency_max_assessments: 6
  sufficiency_input_tokens: 6000
  sufficiency_output_tokens: 2048
  conversation_candidates: 30
```

`retrieval.quality_mode: shadow` records deterministic canonical coverage
observations alongside the legacy answer. It does not run a second semantic
evaluator or change that answer. It is not a complete semantic shadow trial.

## Runtime behavior

The enforced path keeps the ordered L0, current-session, past-session, facts,
hybrid, and graph cascade. Trusted cached/session turns retain role, timestamp
and turn index; original ledger search is available at the next checkpoint.
Conversation discovery separates entity and topic terms, expands a bounded set
of known predicate aliases, and ranks original utterances rather than copied
session context. For at most two hits containing references such as “she” or
“that”, it can fetch one preceding and one following turn from the same tenant
and session. These turns remain a unit during final packing; fetching a neighbor
does not itself establish what the reference means. Explicit time bounds are
ranking hints and do not discard later utterances describing earlier events.
Caller text and unresolved summaries do not establish historical evidence.
Assistant messages can support questions about what the assistant said, but
cannot independently establish a user fact.

The controller checks each entity/predicate requirement and the whole question.
Complex questions may use one decomposition call. Packing preserves whole
graph bundles and prefers complementary entity/predicate bindings. Assessments
are memoized by requirements and packed evidence. An unchanged context does not
trigger another semantic assessment or generation. Semantic support is a model
judgment with validated exact quotations, not a proof of truth.

Previously validated citations receive priority in later packing, bound to the
same source content, role, tenant and revision. This is a ranking preference,
not a carried-forward support verdict: new packed context is reassessed, and
whole-path and token limits still apply. Unchanged candidate inputs reuse their
packing result so learning that preference does not itself trigger another call.

A narrowly recognized exact current lookup can render a canonical value without
generation. Other answers require support assessment and an output check. The
maximum is six evidence assessments, two generations and one output check per
generation. Exhaustive questions remain qualified unless a source has an
explicit completeness signal. Current adapters do not infer that signal from
a top-K search. Temporal interpretation still uses the native query clock.

OpenAI-compatible SDK calls share two admission slots per endpoint per process;
this is not a cluster-wide rate limiter. Provider calls and retry waits respect
the remaining request budget. Late results fail the deadline check and durable
chat still rejects late completion. Database operations retain their native
timeouts. Provider usage is captured before parsing; missing usage remains
unknown. Admission/deadline failures do not poison circuit-breaker health.

## Consolidation and provenance

New strict extractions must cite exact original message text and role. Repeated
quotes require valid offsets. Original timestamps, source roles, offsets and
relative-year precision are stored in existing metadata. Occurrence time is
kept separate from assertion and validity. Other relative expressions remain
unresolved source text; no exact date is invented.

Strict consolidation reads canonical descendants and includes claim history,
validity, assertion time, source events and evidence. The model selects claim
IDs; the server renders their values and qualifiers. Unsupported generated
prose is discarded. Metadata lists included and omitted claims and labels
partial coverage. This overview is a derived view; original records survive.

Publication validates scope and dependency revisions, source status and source
version. Source changes invalidate derived views. Oversized scopes fail
explicitly with `OverviewInputBudgetError`; automatic hierarchy partitioning is
not included. Existing overviews at a current revision remain immutable. This
change does not rewrite production memories or upgrade old overviews in place;
use fresh isolated ingestion to evaluate the strict policy.

Final affected-node and ancestor revisions are queued transactionally. Native
maintenance also repairs missing revision dispatches in tenant-scoped batches
of at most 200. It does not erase failed dispatches. Native Temporal retries and
the existing tenant-scoped failed-event retry endpoint remain available;
automatic terminal-event replay is opt-in through
`canonical_memory.recover_transient_failures`. Maintenance permits one recovery
generation for recognized timeout, connection, rate-limit, server, or
circuit-open provider failures after a successful SDK call to the configured
Core endpoint/model. That success must be newer than the failure and less than
five minutes old; failures also wait at least 30 seconds. Event reset, dispatch
and audit record commit atomically. Permanent and unknown errors stay terminal.
Concurrent maintenance cannot duplicate recovery. Health is process-local: an
idle or restarted worker does not assume a provider recovered without observing
a successful call. Default production configuration leaves recovery disabled.

Failure bookkeeping preserves recognized SDK error types through Core-provider
and Temporal wrappers. A Temporal patch marker preserves historical workflow
replay behavior. Benchmark readiness retains failed events and their history,
but allows the native recovery policy its bounded waiting window. It never
treats a pending recovery as successful ingestion. After recovery, the latest
ingestion generation governs readiness; failed projection work still blocks it.

## Evaluation and release gates

Use a fresh image tag and result directory; do not rebuild an old benchmark tag:

```bash
python -m benchmarks.lifecycle --profile pilot \
  --config /path/to/isolated-quality-config.yaml \
  --image-tag quality-candidate-v1 --build \
  --run-dir benchmarks/results/lifecycle/quality-candidate-v1/pilot
```

The runner validates its private stack, preserves original dialogue/captions,
waits for readiness, freezes checkpoints, and checks unchanged query-state
fingerprints. Strict runs additionally require canonical projection alignment.
Reports distinguish decomposition, sufficiency, generation and output checking;
packing, source rejection and declared-support details are added to technical
rows. Declared support is not actual causal answer attribution. Exact dialogue
recall remains unavailable wherever provenance cannot be established. A
single-conversation pilot has no conversation-bootstrap confidence interval.

Do not use `pilot-then-full` as an automatic quality-release decision: its
existing coverage gate alone does not establish improvement. Compare baseline,
retrieval-only, ingestion/consolidation-only and combined settings on the same
selected questions. Retrieval comparisons require identical verified snapshots.
Advance from pilot to the 50-question validation only after operational issues
and accounting mismatches are resolved. Full runs and canary deployment remain
gated on paired quality/packed-evidence improvement and an independent audit of
unsupported complete answers. The controller's own output check is not that
independent audit.

Compare completed reports without changing their scores:

```bash
python -m benchmarks.lifecycle.compare_quality BASELINE_RUN CANDIDATE_RUN OUTPUT_DIR
```

This writes question-level answer, score, packed-coverage, token and latency
deltas with evaluable denominators and conversation confidence intervals. It
retains failures and unknown usage, and does not automatically approve release
or establish snapshot equivalence without matching checkpoint proofs. Both
native-answer and retrieval-only rows must have complete, equal denominators.

The four-policy runner is implemented:

```bash
python -m benchmarks.lifecycle.paired_quality \
  --baseline-source /path/to/completed-legacy-ingestion-run \
  --candidate-source /path/to/completed-strict-ingestion-run \
  --config /path/to/isolated-quality-config.yaml \
  --profile pilot --image-tag 20261001-quality-v9 \
  --run-dir benchmarks/results/lifecycle/quality-paired-pilot
```

It requires completed source coverage, reconciled accounting, matching dataset,
models and sampling settings, and verified checkpoints without projection lag.
It restores identical checkpoints within each retrieval pair and executes all
four variants in separate stacks. Snapshot hashes are rechecked by comparison.
Ingestion consumption is linked to each source run rather than duplicated as
new replay usage. Tests cover orchestration and actual checkpoint restoration;
the full four-variant live experiment has **not** completed.

The planned production canary is a stable 10% tenant cohort, at least 24 hours
and 100 requests. Tenant selection/automatic canary promotion is not implemented
here. Roll back by restoring `legacy` policy flags and the previous application
image, retaining all source, tenant, lifecycle and revision guards. Preserve
candidate traces and strict source metadata for investigation; do not delete
memories to roll back policy.

## Validation recorded on 2026-10-01

- Selected native unit/integration suites: **368 passed**, including isolated
  PostgreSQL/Neo4j, overview lineage, recovery concurrency, chat runs and session
  history. These counts do not represent every test in the repository.
- Candidate stack validation: **160 passed**, including real checkpoint
  restoration, read-only evaluation, benchmark accounting, and scoring checks.
- Frontend transport tests: **22 passed**; the browser recovery test and
  TypeScript client build also passed earlier in this change.
- A three-evaluation replay across two questions completed without query errors
  or semantic-state mutation. Its 11 provider attempts reconcile to 41,541
  reported tokens with no unknown usage. See the
  [scoped regression summary](../benchmarks/results/lifecycle/20261001-quality-v5/regression-replay/summary.md).
- A real-provider consolidation check passed in its own temporary database
  schema: selected claim IDs were valid, original temporal metadata survived,
  and a subsequent source change invalidated the overview. Its one provider
  attempt used **896 reported tokens**, accounted separately from the pilot.
- Final targeted validation after the remaining recovery, comparison and
  packing changes: **184 tests passed**. The standalone run passed 182 tests;
  its two private-stack checks then passed separately with fresh PostgreSQL,
  Neo4j and Redis services. These include checkpoint restoration, absent future
  data, tenant isolation and read-only query enforcement. Test counts overlap
  earlier validation and must not be summed with it.
- Final runtime image `20261001-quality-v9` matches all **252** native source and
  template files. It has not been deployed to production.

The fresh combined-policy pilot is tracked in
`benchmarks/results/lifecycle/20261001-quality-v7/pilot/`. It uses the verified
native/benchmark image tag `20261001-quality-v6`; the v7 run identifier records
the subsequent benchmark readiness fix. Do not treat live/provisional reports
as completed coverage. The production flags remain unchanged and no quality
release has been approved by these component checks.

The v7 pilot was moved to a byte-verified frozen source copy after 22 completed
routing evaluations, with no provider call interrupted. It continues using its
original code and images, not the final v9 fixes. The legacy comparison pilot
was stopped to reduce shared-provider load; its database, completed results and
unknown-usage attempts are preserved for sequential resumption. No complete
paired accuracy result or full-dataset result is available yet.

Live inspection also found semantic assessment instability: an activity-list
question changed from partial support to missing support even though its three
earlier cited evidence IDs remained packed. The new retention regression does
not establish a fix for that example. Source quote validation also does not
establish that extraction bound the right person or relationship. Those quality
limitations require measured evaluation and independent support review.

Final component evidence is recorded under
`benchmarks/results/lifecycle/20261001-quality-v9/component-validation/`.

The local campaign supervisor records its phase every 30 seconds in
`benchmarks/results/lifecycle/20261001-quality-v9/campaign_progress.json`.
It waits for the current candidate, resumes the preserved baseline using its
exact frozen v7 source, and then runs the four-policy pilot with frozen final
source and v9 images. Each completed source must have full expected coverage
and reconciled accounting. A failed gate stops the campaign with diagnostics.
The supervisor does not advance to validation, full runs or production canary
without the required quality and support review.

Paired-source metadata also records observed consolidation counts. If native
scheduling has produced no canonical overviews at the saved checkpoints, the
report explicitly states that consolidation quality was not measured by that
replay. The separate live consolidation check remains component validation.

The existing `test_vector_search_is_tenant_scoped` integration fixture fails
against both the saved starting code and this work because it omits the required
canonical repository. Verification was not weakened to make that fixture pass.
The new real-repository tenant isolation test passes.
