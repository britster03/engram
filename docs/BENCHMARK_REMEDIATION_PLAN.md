# Engram benchmark remediation and production release plan

**Date:** 12 October 2026. **Status:** implementation plan; candidate fixes and production rollout remain pending.

Engram needs more reliable answer validation, complete source provenance, and better handling of questions that combine multiple conversation records. Fix these before tuning retrieval rankings or changing models. Preserve PostgreSQL as the canonical store, Neo4j as a retrieval projection, Redis session state, Temporal workers, and the ordered retrieval cascade.

The evidence is the [question review](../benchmarks/results/reviews/20261012-engram-qa/analysis.html), its [lossless review data](../benchmarks/results/reviews/20261012-engram-qa/review.json), and the implementation referenced below. This plan supplements [ordered retrieval delivery](ORDERED_RETRIEVAL_PLAN.md) and [quality rollout](QUALITY_ROLLOUT.md). It replaces neither the broader product roadmap nor the benchmark protocol. For this release, rollback means restoring the exact previous configuration and image; the older instruction to set every policy to `legacy` does not match the present production baseline.

## What the report establishes

The immutable review snapshot was captured on **12 October 2026 at 21:00 IST**. It contains **31 executed routing evaluations covering 17 unique questions in one conversation**. Of 193 expected pilot evaluations, 162 were still pending. The live benchmark has continued beyond this snapshot; the numbers below deliberately refer to this one fixed report.

| Measure | Recorded result | Interpretation |
| --- | --- | --- |
| Official mean LoCoMo F1 | 27.02% | Token-overlap score over the 31 executed routing evaluations, including the execution failure; not a factual accuracy percentage |
| Execution outcomes | 30 complete, 1 dependency error | A completed execution can still abstain or answer poorly |
| Terminal abstentions | 9 | Includes the dependency-error response; these are not nine additional execution failures |
| Known whole-query tokens per executed evaluation | 25,704 average | Provider-reported subtotal; unknown consumption prevents an exact total |
| Median query latency | 32.41 seconds | Routing sample only; insufficient to establish a production latency SLO |
| Matched earlier and later checkpoints | 8 F1 decreases across 14 pairs | Lexical regressions require individual review before calling them factual regressions |
| Canonical and retrieval-comparison coverage | 0 executed in this snapshot | No result yet for these tracks |

The review includes every expected row, preserves pending scores as unavailable, separates retrieval candidates from supplied evidence, and links full request and response records. Its [verification record](../benchmarks/results/reviews/20261012-engram-qa/verification.json) records 202 checked query SDK attempts, matching request hashes, reconciled query usage, CSV/JSON round trips, and working offline navigation. The [accounting snapshot](../benchmarks/results/reviews/20261012-engram-qa/snapshot_accounting.json) still has interrupted or in-flight consumption. Successful reconciliation does not make unknown billing known or make the pilot complete.

### Questions that determine the repair order

| Priority and case | Question and reference | Observed answer or behavior | Engineering conclusion |
| --- | --- | --- | --- |
| P1 validation, [0032 later checkpoint](../benchmarks/results/reviews/20261012-engram-qa/reviews/25d2999b31306fc85135e54c2bb7878b61cd99b0c970229050081d7210268022.html) | “What LGBTQ+ events has Caroline participated in?” Reference: pride parade, school speech, support group | “Some retrieval or answer services were unavailable. I could not verify a complete answer.” A candidate existed, but both validation attempts returned empty text at a 2,048-token output limit. Query subtotal: 57,003 tokens. | Confirmed recovery weakness. A generated candidate is not a verified answer; preserve the output check while improving its bounded recovery. |
| P1 temporal provenance, [0049 later checkpoint](../benchmarks/results/reviews/20261012-engram-qa/reviews/b9c1c704a78f69b248d7548e1457f0008b926f54a3a3387603e109c07b6f4fe5.html) | “When did Caroline and Melanie go to a pride fesetival together?” Reference: 2022 | Abstained. A submitted canonical record said “last year” without an original timestamp or event/session IDs. Ten SDK attempts consumed 37,063 known tokens. | Missing provenance is confirmed. Its contribution to the abstention must be isolated through a fixed-evidence comparison. |
| P1 support assessment, [0066 earlier checkpoint](../benchmarks/results/reviews/20261012-engram-qa/reviews/f80f8f97a68294787603f9afc7ff57a65d54fca118131c3e689fc1b9a180686c.html) | “What does Melanie do with her family on hikes?” Reference: roast marshmallows, tell stories | “I don't have enough verified information to answer the complete question.” Four assessments returned `MISSING` despite relevant hiking and campfire passages in their inputs. | Cross-turn support and time-scope handling need controlled regression tests. A current-versus-historical policy mismatch is a hypothesis, not an established cause. |
| P1 count scope, [0040 earlier checkpoint](../benchmarks/results/reviews/20261012-engram-qa/reviews/8c5dc2c3c02737d58c99d37e1188a78c40af895f290fb5906de5960f2bbbca5d.html) | “How many times has Melanie gone to the beach in 2023?” Reference: 2 | Abstained although both annotated records reached assessment. | Distinguish documented events, duplicate mentions, and proof of an exhaustive count. Two retrieved records alone do not establish two distinct events or complete coverage. |
| P2 answer representation, [0049 earlier checkpoint](../benchmarks/results/reviews/20261012-engram-qa/reviews/9c09deac0ddc4508bd4a901005fc527e1c67a74904857cb7c507c0929019f3cb.html) | Same pride-date question; reference: 2022 | “Last year, indicating 2022 - Caroline stated this on 17 August 2023.” Official F1: 0.167. | The requested year agrees with the reference. Treat this separately from retrieval failure. |
| P2 temporal representation, [0068 earlier checkpoint](../benchmarks/results/reviews/20261012-engram-qa/reviews/0aad32e953c3f5f96bd4f61351a77a470e910dd34fa2cde467a4c5e470852dbe.html) | “How long has Melanie been practicing art?” Reference: since 2016 | “Seven years.” Official F1: zero. The original statement is dated September 2023. | Duration and start-year representations are compatible at the source date. They do not establish a current duration in 2026. |

Broad-question coverage is a separate diagnostic. The transition-journey answer mentions friendships and omits the reference's body-change aspect, but the question asks for **some** changes. Missing a reference aspect is not sufficient to declare an otherwise supported answer incorrect. Likewise, strict abstention on counterfactual career choices or inferred political views may reflect product policy rather than retrieval failure. Keep factual-memory behavior as the default; an inferential mode is outside this repair release.

## Implementation boundaries

The current baseline uses `muse-spark-1.3-contributor` for both Core and Frontier, retrieval quality `enforce`, canonical evidence `legacy`, and consolidation quality `legacy`. Preserve those identities in the baseline. Model switches, new databases, reranking experiments, and changes to the official scorer are outside the first remediation release.

The running benchmark bind-mounts harness source. Create an isolated candidate checkout containing the exact current source, including required untracked files; a plain worktree from `HEAD` would omit substantial current implementation. Record a file-hash manifest, sanitized configuration identity, image digests, dependencies, prompts, dataset hash, and scorer revision. Build fresh candidate images and use new private services and report directories. Never modify an active baseline's hashed source, checkpoints, configuration, or historical artifacts.

Public API responses, database schemas, and Python/TypeScript clients need no initial changes. Use existing source metadata, internal evidence objects, diagnostics, and additive report fields. If an implementation reveals a required public contract or migration, split it into an explicitly versioned change with compatibility tests.

## Delivery sequence

Owner names below are responsibilities to assign, not staffed commitments. Effort estimates are engineering days and exclude provider-dependent benchmark runtime. Execute correctness changes before cost optimization; keep each change independently revertible.

| Work package | Owner role | Dependency | Estimated effort | Exit artifact |
| --- | --- | --- | --- | --- |
| R0 Freeze evidence and protect execution | Platform and evaluation | None | 1–2 days | Reproducible candidate environment and controller recovery test |
| R1 Recover bounded validation failures | Model runtime | R0 | 1–2 days | Typed failure diagnostics and bounded recovery with tests |
| R2 Carry original temporal provenance | Memory and retrieval | R0 | 3–5 days | Source-bound evidence and audited migration design |
| R3 Correct support and count semantics | Retrieval | R2 for temporal cases | 3–5 days | Fixed-evidence regression suite and supported-scope behavior |
| R4 Reduce unnecessary model work | Retrieval and runtime | R1–R3 correctness gates | 2–3 days | Paired cost and latency comparison |
| R5 Finish reporting and answer diagnostics | Evaluation | R0 | 1–2 days | Additive native metrics and complete descriptive reports |
| R6 Prove release and operate canary | Platform and evaluation | R1–R5 | 2–3 days plus observation | Release evidence, rollback drill, and cohort runbook |

Total planning estimate: **13–22 engineering days**, plus live evaluation and canary observation. Re-estimate after R0; a completed pilot is insufficient evidence for a calendar promise about the full run.

### R0 Freeze evidence and protect execution

Build on [the benchmark controller](../benchmarks/lifecycle/__main__.py), [paired replay](../benchmarks/lifecycle/paired_quality.py), and [the review generator](../scripts/review_lifecycle_benchmark.py).

1. Retain the original dataset, seed 42, pinned official scorer, ingestion chronology, session compaction, checkpoint restoration, projection readiness, and read-only evaluation. Gold questions, answers, and evidence annotations remain outside ingestion and model prompts.
2. Record process health separately from meaningful progress: last committed evaluation, ingestion checkpoint, backlog, provider attempt, and readiness reason. A healthy idle container is not proof that the host controller is alive; a long readiness phase is not automatically a deadlock.
3. Supervise the existing controller under a durable service/job and preserve its lock and resume protocol. Expose terminal `COMPLETE`, `COMPLETE_WITH_ERRORS`, or `FAILED` status; after an interruption, explicitly account for attempts whose billing is unknown. Do not introduce a second ingestion queue.
4. Test controller termination and restart in a disposable stack at ingestion, query, and report-write boundaries. Reuse committed results; retain distinct resumed SDK attempts and their usage. Enforce one active controller per run.
5. Add a phase decision record containing coverage, isolation, read-only fingerprints, accounting, and unresolved errors. Baseline coverage completion and candidate quality promotion are distinct decisions. A low answer score is a result; missing coverage, mismatched journals, or failed isolation blocks advancement.

**Acceptance:** reproducible source/image identities, one expected row per evaluation, no duplicated committed answers, preserved interrupted attempts, no production-service access, and successful checkpoint recovery without semantic-state mutation.

### R1 Recover bounded validation failures

Change [the provider adapter](../engram/models/providers/openai_compat.py), [request budgets](../engram/models/budget.py), and [answer validation](../engram/retrieval/sufficiency.py) in the candidate checkout.

`_correction_max_tokens` currently preserves the same cap for `decomposition`, `sufficiency`, and `output_support`. The observed failure exhausted 2,048 output tokens twice. The first corrective experiment should change **output-support recovery only**, leaving other phase budgets unchanged so its effect is measurable.

- Classify empty output, output-limit exhaustion, malformed JSON, invalid schema, unsupported claims, transport failure, and deadline/attempt exhaustion separately. Preserve the actual provider status and incomplete reason when present; an empty response does not itself prove a particular provider failure mode.
- For empty or output-limited output-support responses, permit at most one correction attempt with a configurable ceiling of 4,096 output tokens, inside the existing request deadline, attempt limit, and endpoint admission control. This is a proposed configuration field, not an existing production setting. Keep ordinary malformed-JSON correction bounded; semantic rejection must not trigger repeated calls until an answer is accepted.
- Preserve the original candidate, full instructions, submitted evidence, output schema, and operation/attempt lineage. Record usage before parsing; retain failed and interrupted attempts. If recovery fails, return an explicit service/verification limitation, not an unvalidated candidate.
- Distinguish transport health from usable structured output. A successful HTTP/SDK response containing no visible JSON can establish transport availability without establishing validator success.
- Update the existing configured-cap regression in [quality pipeline tests](../tests/unit/test_quality_pipeline.py). Add output-support-specific recovery tests instead of globally doubling semantic budgets.

**Acceptance:** scripted empty-first/valid-second responses recover; empty-twice, unsupported output, invalid citations, insufficient budget, and expired deadlines stop safely. Each real SDK attempt is charged once. Live replay measures whether the larger recovery cap actually helps case 0032; it must not claim success from mocked tests alone.

### R2 Carry original temporal provenance

Connect existing strict extraction in [provenance validation](../engram/ingest/provenance.py) to [repository hydration](../engram/storage/memory_repository.py), [verified evidence](../engram/retrieval/evidence.py), [quality evidence](../engram/retrieval/quality.py), and [sufficiency input](../engram/retrieval/sufficiency.py).

The repository already has exact-span validation and relative-year metadata. The gap is end-to-end availability: `canonical_items` does not populate an evidence timestamp, and original-event hydration currently runs for `documented_items` requirements only. Enabling strict flags cannot retroactively supply missing metadata in legacy memories.

1. Carry source event/session/turn identity, speaker and role, original timestamp, exact quote and offsets, canonical revision, temporal precision, and any source-bound normalized occurrence into supplied evidence. Treat assertion validity, event occurrence, ingestion time, and query time as separate concepts.
2. Preserve multiple origins independently. Do not assign one arbitrary timestamp to a claim supported by several statements. Link each quotation and temporal anchor to its own source; retain graph bundles and canonical revision bindings during packing.
3. Extend bounded original-event hydration to temporal and count requirements when reliable event identifiers exist. Preserve tenant filtering, cache keys, batch bounds, cancellation, and whole-bundle token limits. A missing origin stays unavailable; fuzzy textual similarity must not create provenance.
4. Resolve “last year” from the original utterance date. Preserve year precision; do not invent a day or month. Missing or conflicting anchors require qualification or abstention. Historical habits must not become unqualified present-day assertions.
5. Evaluate strict ingestion and consolidation in separate candidate variants. For historical data, produce an idempotent dry-run inventory of verifiable, missing, and ambiguous origins. Backfill only from exact original records, with checkpoints and before/after audit records. Rebuild derived projections through existing outbox/Temporal paths and wait for revision alignment. Retention-expired originals remain unavailable.

**Acceptance:** case 0049 retains its 2023 source anchor when that source is verifiable and can support the year 2022 at both checkpoints. Tests cover multiple origins, missing dates, repeated quotes, mixed time zones, year boundaries, revised claims, assistant-only assertions, other tenants, and future sessions. A query performs no repair writes; backfill is a separate controlled job.

### R3 Correct support and count semantics

Extend [requirements and support assessment](../engram/retrieval/sufficiency.py), [planning](../engram/retrieval/planner.py), and existing enumeration/activity tests. Existing exact-citation checks, whole-question requirements, original-record hydration, source diversity, and support-priority retention remain required.

- Build fixed-evidence fixtures from the observed cases before changing prompts. Run separate comparisons for source anchoring, temporal-scope selection, adjacent-turn binding, and completeness wording. Change one factor at a time; record the Core input and verdict so a plausible explanation does not become an assumed root cause.
- Distinguish current-state questions, events in a stated interval, historical habits, and questions about documented conversation history. For unqualified habits supported only by old messages, answer within the documented period when justified instead of asserting the habit still holds. Do not set the runtime clock to dataset dates to improve a score.
- Permit cross-turn support only with a defensible speaker/entity/session relationship and original quotations. Neighbor retrieval is context, not automatic proof that two statements refer to the same activity.
- For counts, distinguish mentions from distinct events. Deduplicate repeated descriptions using source-bound event identity or an explicitly uncertain event grouping. Use “the records document two visits” when that bounded statement is supported; claim an exhaustive total only with an established coverage scope. Top-k retrieval is not an exhaustion proof.
- Separate claim support, requested-clause coverage, and exhaustive scope. A request for examples need not list every fact in the dataset. A complete multi-clause answer must satisfy every requested clause or explicitly identify the missing part.
- Preserve justified abstentions. Counterfactual speculation, inferred personal attributes, unsupported entities, contradictory dates, and facts from future sessions must not become answerable merely to increase LoCoMo F1.

**Acceptance:** hiking, beach-count, and broad-activity fixtures improve only where the supplied evidence supports the response. Adversarial variants with a different person, negation, duplicate events, ambiguous references, or missing scope do not inherit the positive verdict. Review the returned answer against its actual submitted evidence, independently of the controller's own support decision.

### R4 Reduce unnecessary model work

[Sufficiency assessment](../engram/retrieval/sufficiency.py) already memoizes identical packed context and requirements. Extend that implementation only after R1–R3 pass; adding a second generic cache would obscure attribution.

Record the change in eligible and packed evidence, requirement coverage, contradiction signals, and answerability at each stage. Skip a semantic reassessment only when a deterministic check establishes that its inputs and support-relevant bindings are unchanged. Any changed role, tenant, revision, quote, temporal anchor, conflict, policy version, or relevant requirement invalidates reuse. Preserve original graph bundles and exact-citation checks.

Retain the current limits of six semantic assessments, two generation attempts, the absolute query deadline, and the configured SDK-attempt ceiling unless a separately measured change justifies a different bound. The existing endpoint semaphore is per process; load tests must account for all API and worker replicas sharing provider capacity. Add deployment-level admission control if replica tests demonstrate overload.

**Acceptance:** unchanged-evidence replay avoids redundant calls; relevant new or contradictory evidence is reassessed. The initial efficiency target is **25% fewer mean verified whole-query tokens on matched evaluations with complete usage**, with no material quality or p95 latency regression. Also publish all-row known subtotals, the matched denominator, unknown-usage rates, and lifecycle costs so incomplete billing cannot create a false saving. This target is proposed, not an achieved result.

### R5 Finish reporting and answer diagnostics

Port the proven presentation behavior from [the review generator](../scripts/review_lifecycle_benchmark.py) into candidate versions of [native reporting](../benchmarks/lifecycle/report.py) and [descriptive reporting](../benchmarks/lifecycle/descriptive.py). Preserve existing journal and CSV field meanings; add a schema version and explicitly named metrics.

- Publish expected/executed/failed/pending counts, executed-only F1, and final coverage-adjusted results separately. Keep failures in the final expected denominator. Pending evaluations have no observed answer score; a track with zero executions must not appear to have measured 0% quality.
- Retain per-question reference, exact answer, execution status, answer mode, evidence quotations, source reads, ranked candidates, packed context, all Core/Frontier attempts, full request bodies, raw outputs, errors, timing, and trace links. Mark deterministic answers and unverifiable answer-operation attribution explicitly.
- Separate answer-producing operation usage, final returned-answer attribution, whole-query usage including earlier resumed submissions, lifecycle consumption, verified subtotals, estimates, and unknown billing. Reasoning and cached tokens remain subsets of totals. Tokenizer section counts are not provider billing.
- Keep official scoring unchanged. Add source-date-aware temporal agreement and manual unsupported-answer/false-abstention diagnostics with explicit evaluable denominators. These must not silently replace the official score or introduce judge-model calls.
- The generation prompt already asks for concise answers. Test a narrow answer-shape contract for dates, durations, names, and counts, with source-supported qualifications and citation details separately displayed where the existing interface permits it. Preserve the original model answer for scoring; never rewrite it using the gold answer. “Seven years as of September 2023” should not be silently presented as a current duration.
- Stream traces and retain complete long text in linked detail files. Keep HTML escaped, navigation offline, CSV previews linked to complete JSON, and spreadsheet formula-like content treated as text in exports without altering the lossless raw record.

**Acceptance:** all expected rows and all observed attempts are represented; payload hashes and provider journals reconcile; Unicode, multiline and large requests round-trip; no broken local links or script injection; partial and resumed usage remains visible. Existing review checks validate the archived review, not these future native-report changes.

## Validation and experimental design

Use the existing benchmark image for scorer/report validation and an isolated candidate image for application tests. Do not weaken tests because the host lacks evaluation dependencies. Extend the following regression suites rather than reproducing implementation internals in new tests:

| Contract | Existing test location | Required additions |
| --- | --- | --- |
| Provider recovery and usage | [quality pipeline](../tests/unit/test_quality_pipeline.py), [provider adapter](../tests/unit/test_openai_compat.py), [token usage](../tests/unit/test_token_usage.py) | Empty response recovery, bounded retries, partial/interrupted billing, deadline admission, phase-specific caps |
| Original evidence | [enumeration origins](../tests/unit/test_enumeration_origins.py), [enumeration support](../tests/unit/test_enumeration_support.py) | Temporal/count hydration, multiple anchors, missing origins, event deduplication, complete-scope boundaries |
| Support and packing | [activity regressions](../tests/unit/test_locomo_activity_regression.py), [support retention](../tests/unit/test_quality_support_retention.py), [profile recovery](../tests/unit/test_profile_answer_recovery.py) | Historical habits, cross-turn binding, contradictory later evidence, cache invalidation, safe deterministic fallback |
| Integration | [quality retrieval](../tests/integration/test_quality_retrieval.py), [tenant isolation](../tests/integration/test_tenant_isolation.py), [overview lineage](../tests/integration/test_overview_lineage.py) | Real PostgreSQL/Neo4j provenance flow, projection readiness, query immutability, bounded backfill and rollback |
| Benchmark contracts | [benchmark tests](../tests/benchmarks), [review tests](../tests/unit/test_benchmark_review.py) | Scorer completeness/parity, resumed attempts, all denominators, lossless details, offline exports, controller restart |

Cases taken from this report become development regressions. Freeze a disjoint conversation/question holdout before prompt tuning, retain its predictions, and report tuned versus held-out results separately. The requested full LoCoMo run still covers all 1,986 canonical questions; its aggregate after tuning is a benchmark result, not an untouched generalization estimate. Use independent human review of a preselected mix of successful answers, failures, abstentions, and adversarial cases, with source quotes and reviewer disagreement recorded. Blind reviewers to candidate identity where practicable.

Preserve the three-track seed-42 protocol:

| Phase | Canonical | Session routing | Retrieval only | Total |
| --- | ---: | ---: | ---: | ---: |
| Pilot | 25 | 43 | 125 | 193 |
| Validation | 50 | 87 | 250 | 387 |
| Full | 1,986 | 3,471 | 9,930 | 15,387 |

BM25, vector, hybrid, KG, and hybrid+KG remain retrieval-only comparisons. Do not mix their rows into an answer F1 average. Canonical and routing results also stay separate; routing includes repeated questions at different checkpoints.

Run the four-policy comparison using the existing [paired runner](../benchmarks/lifecycle/paired_quality.py) and [comparison tool](../benchmarks/lifecycle/compare_quality.py):

| Variant | Ingestion and consolidation | Retrieval and answering |
| --- | --- | --- |
| A baseline | Current legacy policies | Current enforced retrieval |
| B retrieval candidate | Same source snapshots as A | R1–R4 candidate behavior |
| C memory candidate | Strict provenance/consolidation candidate | Baseline retrieval behavior |
| D combined candidate | Same source snapshots as C | R1–R4 candidate behavior |

A/B and C/D require identical verified checkpoints within each pair. A/C are fresh chronological ingestions of identical input data; their memory states are expected to differ. The current paired runner was designed primarily for policy-flag comparisons: extend its manifests and image selection if necessary to represent baseline versus candidate **code** independently of flags. Do not label two runs with identical code and flags as a code-fix experiment. Account for reused ingestion once and disclose when a replay exercised no consolidated overviews.

Keep models and sampling settings identical. Record that seed 42 fixes question selection, not provider determinism. Repeat a fixed representative subset to expose response variation, using the same predeclared repetition count for each variant. Compute paired differences by track and category, with conversation-cluster intervals where there are enough conversations; the one-conversation pilot cannot support such an interval.

## Release gates

The following thresholds are **proposed release criteria**, to freeze before inspecting candidate holdout results. They are not current performance claims.

| Gate | Required evidence | Failure action |
| --- | --- | --- |
| Integrity | All expected evaluations represented; every observed attempt retained; full Frontier payloads available; request hashes and verified usage reconcile; unknown usage explicit | Stop the phase; repair the harness or capture before quality comparison |
| Isolation and state | Zero cross-tenant/future-session evidence in the test suite; unchanged semantic state during evaluation; aligned canonical projections | Stop promotion immediately; investigate the affected data path |
| Correctness | All deterministic regression contracts pass; no unsupported complete answer in the targeted adversarial suite; independent audited unsupported-answer rate does not worsen | Reject candidate behavior; inspect examples rather than relaxing evidence checks |
| Quality | On held-out canonical and routing tracks separately, paired mean F1 does not decrease; 95% paired conversation-cluster lower bound exceeds −0.02 where estimable; investigate every category decline greater than two percentage points | Collect more evidence or revise candidate; an unestimable interval is not a pass |
| Improvement target | Aim for at least five F1 percentage points over the completed baseline on the affected native track, or a separately demonstrated reduction in operational failures with quality non-inferiority | State which benefit was demonstrated; do not promise this gain from the provisional 27.02% result |
| Reliability | At least 1,000 representative staging requests; final operational failure rate at most 1% and no worse than the matched baseline; all interrupted-query recovery tests pass | Diagnose provider/runtime errors before canary; report failure counts and intervals |
| Efficiency | Report R4's 25% token-reduction target; candidate p95 query latency no more than 10% worse than matched baseline under equal load | Keep correctness fixes if justified, but do not ship the optional optimization without its cost/quality evidence |
| Operability | Tested previous-image/config rollback, durable controller restart, bounded queue admission, readiness alerts, and complete phase decision records | Remain in staging |

The two-percentage-point statistical margin expresses tolerated uncertainty; it does not permit hiding an observed average decline. Failure rates and support audits must include counts and uncertainty. Zero observed violations does not prove zero production risk. Do not set a numerical false-abstention improvement gate until independent review has established which abstentions are actually avoidable.

## Production rollout and rollback

1. **Staging:** apply R1–R5 in fresh candidate images; pass the component, integration, paired pilot, validation, and full-coverage gates. Load-test the API and workers together, including provider degradation, PostgreSQL/Neo4j lag, interrupted streams, and a growing Temporal backlog. Capture p50/p95/p99 latency, attempt counts, terminal causes, unknown usage, and queue wait by phase and policy version.
2. **Controlled enablement:** implement stable tenant-cohort configuration before canary. The existing `shadow` mode records deterministic observations; it does not run a parallel semantic candidate. Use offline paired replay first. Any later online semantic shadow execution must be explicitly implemented, isolated from writes, and included in capacity and token accounting.
3. **Canary:** start with the documented 10% stable tenant cohort, for at least 24 hours and 100 requests, and continue longer until the release criteria can be assessed. Those minimums alone do not establish a rare-error bound. Promote to 50%, then 100%, only after a recorded gate decision and continued monitoring. Keep source-ingestion policy selection consistent for each tenant and record the policy used for every new event.
4. **Rollback:** any isolation violation, state mutation, missing accounting, or reproducible unsupported-answer regression stops promotion immediately. Restore the exact previous image digest and configuration for the affected cohort. Stop candidate backfill at a checkpoint; retain source events, metadata, revisions, and investigation traces. Do not delete memories or revert to generic `legacy` settings. Verify that the previous application can read additive candidate metadata before starting canary.
5. **Documentation:** update [README](../README.md), [architecture](ARCHITECTURE.md), [production operations](PRODUCTION.md), and [quality rollout](QUALITY_ROLLOUT.md) to describe the actual PostgreSQL authority, quality modes, recovery limits, cohort controls, and rollback procedure. Existing filesystem-authority descriptions must not guide new implementation.

Use aggregate diagnostics and source/attempt identifiers for routine production observability. Full prompt and response archives are required for this benchmark; they should not automatically become unrestricted production logging. Any production trace capture must follow existing tenant access and retention controls, exclude credentials/transport headers, and keep diagnostic text bounded in operational logs.

## Definition of completion

The reporting correction already present in the archived review is complete for that snapshot. The runtime repairs described here are complete only when their candidate code and tests exist, the full descriptive artifacts reconcile, each benchmark phase has an explicit final status, the paired quality and independent support reviews pass, and the production rollout and rollback have been exercised.

The release package must include source and image identities, exact effective configurations without credentials, regression results, the four-policy comparison, per-question HTML/CSV/JSON and model-call records, unresolved limitations, and the rollout decision. Publish improvements and regressions together. A passing unit suite, a low-cost query, or a completed pilot alone is insufficient to call Engram production-ready.
