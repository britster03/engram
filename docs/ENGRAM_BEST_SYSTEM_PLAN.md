# Engram Best-System Plan — Retrieval, Planner, Consolidation, Ingestion, Production

> **2026-09-29 production revision:** use [ORDERED_RETRIEVAL_PLAN.md](ORDERED_RETRIEVAL_PLAN.md) for current delivery priorities and [PRODUCTION_PRODUCT_AUDIT.md](PRODUCTION_PRODUCT_AUDIT.md) for verified implementation status. The historical analyses, line numbers, defect lists and build order below are design context, not proof that a feature or defect still exists. The revised plan reconciles the complete production-product requirements and supersedes conflicting recommendations here.
>
> Source: full-repo analysis 2026-09-15 — `engram_ingestion_consolidation_retrieval_design.md` (1814 lines), `AI-Memory-Ref-Repos.xlsx` (Sheet1, 15 rows), live repo `engram/` + `docs/ENGRAM_VS_SUPERMEMORY_AUDIT.md`, slice analyses Ingest/Retrieval/Consolid/Prod.
> Retrieval update: [Required ordered cascade](ORDERED_RETRIEVAL_PLAN.md) supersedes the earlier capability-routing recommendation.
>
> Non-negotiable invariant: `vector != truth; neo4j != truth; overview != truth; verified PG claim = truth`.

## 0. Verdict

Keep V2 PostgreSQL-canonical. `config.prod.yaml:28-33` already declares it (`canonical_memory.enabled:true`, `allow_filesystem_runtime:false`).
Keep `MemoryRepository.commit_conversational_event` + Temporal as the production memory write path; preserve legacy compatibility only where explicitly supported. The revised delivery order is ownership and trust, durable chat, safe user-grounded automatic memory, accurate retrieval and memory controls, then operational proof. Hybrid/RRF/reranking require measured benefit and do not precede basic conversation correctness.

Reference formulas:

```text
I = Gate → Episode → Atomic → Type → Resolve → Normalize → LocalMatch → Action → CanonicalCommit → Project → Consolidate
R(q) = L0(parametric) → L1(session) → gate → L2(past sessions) → L3(facts → vector → graph)
Stop when sufficient; verify every vector/graph candidate against canonical PostgreSQL.
Hybrid ranking and alternative embeddings are experiments inside L3_VECTOR.
```

Architecture:

```mermaid
docs/files/URLs/connectors --> documents[chunks immutable]
documents --> extract[async extract + dreaming]
extract --> pg[(PG canonical + RLS)]
pg --> outbox[txn outbox]
outbox --> proj[vector + FTS + Neo4j]
proj --> hybrid[RRF -> rerank]
hybrid --> verify[PG hydrate + verify]
verify --> gate[evidence gate]
gate --> answer[profile + answer / deterministic]
```

---

## 1. What the attachments say

### 1.1 `AI-Memory-Ref-Repos.xlsx` — sparse, signal not spec

Parsed `xl/sharedStrings.xml` + `xl/worksheets/sheet1.xml`: 1 sheet, 15 non-empty rows, 9 cols (`Repository/Core Idea/Description/Memory Structure/Storage/Retrieval/Filtering/Token Efficiency/Benchmarks`).

- Row 2 `Letta`: Core/Archival hierarchy, Vector+SQL, agentic search/eviction, paging — LoCoMo 74.0%.
- Row 3 `Mem0`: m-message contextual memory → entity graph, add/update/discard by similarity, Vector+Neo4j — LoCoMo 68.44%.
- Row 6 `Basic Memory`: Markdown KG via MCP, Filesystem+SQL, explicit-write, selective sub-graph — N/A.
- Rows 4/5/7-10 labels only: `Zep/Cognee/EverMemOS(paper WIP)/Supermemory/Nemori/Memos`, `SimpleMem` (compression/consolidation/adaptive retrieval), `A-MEM` (agent-evolved memory).
- Rows 13-15 paper links: `Memory-R1`, `Recursive LMs`, `Agent-Memory-Paper-List` survey.

Use for direction; design detail comes from §1.2 + code.

### 1.2 Design doc — adopt formulas, not wholesale copy

| Repo | Take |
|---|---|
| Graphiti/Zep | hybrid lexical/vector/graph, temporal graph, BFS, rerank |
| SimpleMem | atomic semantic memory, consolidation, query-aware retrieval |
| EverMemOS | hierarchical memory, hybrid/RRF, agentic fallback |
| Nemori | semantic episode segmentation |
| Supermemory | UPDATES/EXTENDS/DERIVES evolution semantics + profiles |
| Cognee | multi-representation index, graph/vector retrieval |
| A-MEM | local-match before formation, dynamic linking |
| Letta | working vs archival separation |
| Mem0 | lightweight update simplicity |
| Basic Memory | explicit links, selective traversal |

### 1.3 Audit doc — where Engram stands

Leads on auditability (PG versions/claims/evidence/RLS, Neo4j disposable, hydrate-then-verify). Trails on product (hybrid doc+memory, profiles, review/forget, ingestion breadth, SDKs, benchmarks). Plan below closes product gap without weakening canonical layer.

---

## 2. Historical repository snapshot

Use the linked production audit for current status. In particular, ordered retrieval and PostgreSQL session snapshots now exist; automatic user-only chat ingestion and automatic chat compaction do not.

- Canonical: `engram/storage/memory_repository.py` (4779 lines) + `canonical_schema.py` + `control_plane.py` + `postgres.py`. One-txn commit nodes/versions/claims/evidence/aliases/hierarchy/mutations/outbox. Good.
- Retrieval: `engram/retrieval/orchestrator.py` (1813 lines) + `planner.py` + `evidence.py` + `context.py`. Hydrate-then-verify enforced. Good.
- Prod: `config.prod.yaml` V2 on; `docs/ARCHITECTURE.md` still describes fs-authoritative V1; `PLAN.md` SDD map stale.
- Consolidation V2 live handlers only 2: `CONSOLIDATE_OVERVIEW + UNMERGE_CANONICAL` (`consolidation/worker.py:87-117`, `canonical.py:15-93/96-149`). Legacy 8 types in `tasks.py` dead in canonical mode.

---

## 3. Ingestion — best way

### 3.1 Current flow

`worker.py:261-271` gate (`gate_write.j2`) → `274-288` extract (`extract.j2` → `{resolved_text,triplets,l0_abstract}`) → `291-338` slug-link (`entity_linker.py:38-111`, vector k=5, cos≥0.92 + overlap≥0.5, `entity_link.j2`) → `341-399` fs write (`mem://user/episodes/…`, `mem://user/entities/…`) + `fs_outbox` → `422-527` `_index_neo4j` + `conflict.py:42-140` inside critical path → `621-647` enqueue `OVERVIEW+MANIFEST+PROPAGATE`.
Canonical mirror: `temporal/activities.py:937-1116` + `memory_repository.py:1436-2009`. `code_ingest.py:76-209` already closest to ideal (single-txn `commit_code_project`, deterministic `mutation_key event:code-edge:i`).

### 3.2 Defects

- D1 Legacy bypasses PG: fs+Neo4j writes, no typed claims/evidence/temporal/RLS txn. Violates `allow_filesystem_runtime:false`.
- D2 No predicate normalization before conflict: raw `trip['relation']` (`worker.py:476,501`) skips `relations.canonicalise` (`relations.py:41-63`) + `predicate_registry.normalize_claim_value`. `worksAt` vs `works_at` fragments `DUPLICATE_RELATION_COS 0.90`, defeats SINGLE supersede.
- D3 Slug identity, no alias order: `slugify` join (`309,414-415`), compare vs `l0_abstract` (`entity_linker.py:63`) not alias table, never `add_alias`. Canonical order (`memory_repository.py:1564-1652`: UUID → exact alias → session-local expansion `1525-1562` → `uuid5` create) has no legacy counterpart.
- D4 Untyped values: `extract.j2` untyped strings, never `normalize_extracted_triplets`, never `_coerce_value:336-371`, narrative coords leak. Dates as entities, `valid_from/until` never populated.
- D5 Unbounded local-match: `MAX_CANDIDATES=5` per mention N round trips (`316-325`), `_fetch_active_edges` uncapped, no batch-by-stable-ID.
- D6 Neo4j mutations not actions: `DUPLICATE/CONTRADICTION/CO_EXISTENCE → touch/supersede/merge` (`143-189`) with `except-pass` (`208-251`). No `ADD/REINFORCE/SUPERSEDE/CONTRADICT/NOOP`, no PG status transition.
- D7 Assistant re-ingest: `_turn_pair:244-258` feeds assistant text to `resolved_text`/`l0_abstract`; no user-vs-assistant provenance flag. `0.3-0.6` LOW_CONFIDENCE FACTs indexed at `weight 1.0` (`569-587`).
- D8 Split-brain idempotency: legacy `fs.exists` + `event_id` + `fs_outbox` vs canonical `uuid5` (`_MUTATION_NAMESPACE`, `_CLAIM_NAMESPACE`, `event:claim:index`) + `gate-v1/extract-v1/typed-v1` artifacts. Retry dupes Neo4j (`uuid4:433-469`); `_finish_indexed_event:89-112` replay via `linked_entities` canonical never writes.

### 3.3 Best pipeline

```text
Gate → Episode → Atomic → Type → Resolve → Normalize → LocalMatch → Action → CanonicalCommit → Project
```

All model I/O in Temporal Activities, all state change in one `commit_conversational_event` txn. Neo4j/vector/lexical post-commit only.

1. **Gate** (in: `turn_pair{user,assistant}+session_summary+force_store`; out: `{store,reason}` `gate-v1`; skip→`GATED_SKIP`). Add: assistant-only generic → `store=false`.
2. **Episode** (in: gated payload; out: `resolved_text+l0_abstract` `extract-v1`).
3. **Atomic** (in: `resolved_text`; out: `raw triplets[{subject,raw_predicate,object,confidence,source_span}]`, split `and/&`, drop narrative coords).
4. **Type** (in: raw; out: `typed-v1` via `normalize_extracted_triplets:233-299`: `predicate+object_type+cardinality+temporal_behavior+conflict_policy+policy_version+valid_from/until`; invalid→`InvalidCanonicalExtraction` non-retryable).
5. **Resolve** (in: typed; out: entity IDs; order UUID → exact alias (`normalize_alias`, ambiguous→FLAG) → session-local expansion (session-bounded, ambiguous→error) → `uuid5` create + `add_alias`; record spelling/short-name upgrades `1621-1651`).
6. **Normalize** (in: IDs+predicate; out: `_hash_object` dedup key).
7. **LocalMatch** (in: `(subject_id,predicate)`; out: 10-30 current claims by stable ID, one `get_current_claims(subject,predicate,at)` batch, K=30 cap).
8. **Action** (in: claim+candidates+policy; out: `ADD` (new) / `REINFORCE` (same hash, touch, no row) / `SUPERSEDE` (SINGLE+diff hash, `supersede_claim` chain) / `CONTRADICT` (FLAG+diff hash→`CONFLICTING`) / `NOOP` (retry dup). `<0.6` → `LOW_CONFIDENCE` claim, projection-demoted).
9. **CanonicalCommit** (single PG RLS txn: episode `uuid5`+version+`evidence(event:episode)`; per-triplet `uuid5(tenant+event+index)`, `CONVERSATIONAL_INGEST` replay `1448-1472`, `evidence(event:claim:i)`; `MENTIONS` edges; `MEMORY/CLAIM/HIERARCHY` dispatches; `events→COMPLETE` atomically. Require pre-typed input; direct-caller recompute `1754-1763` → loud error in prod).
10. **Project** (`ProjectionWorkflow:164-191` → Neo4j merge + vector/lexical + consolidation enqueue, retryable, disposable).

Files: retire `worker.py:process_event/_index_neo4j`, `conflict.py` Neo4j path, `entity_linker.py` Neo4j-in-txn; reimplement `unmerge.py` as PG alias/status; keep `code_ingest.py` pattern.

---

## 4. Query planner — required ordered cascade

Follow [ORDERED_RETRIEVAL_PLAN.md](ORDERED_RETRIEVAL_PLAN.md): L0 parametric →
L1 current session (cache → PostgreSQL) → sufficiency gate → L2 past sessions →
L3 facts → vector discovery → Neo4j traversal. Stop when the complete question
is supported. The model can refine search arguments; the server owns stage order.
A standalone classifier remains optional. Preserve temporal intent, tenant scope,
provenance and exact deterministic answers in the canonical adapters.

## 5. Retrieval — implementation and acceptance

The linked plan defines the semantic tool contracts, cache recovery, bounded
past-session search, PostgreSQL verification, graph templates, API depth
compatibility, failure handling, tests and rollout. Legacy repository L1–L4
identifiers are internal capabilities and must not be confused with the new
public cascade stages. Earlier hybrid/RRF/reranker and vector-representation
recommendations are experiments within L3_VECTOR, not alternate stage orders.

---

## 6. Consolidation — best way

### 6.1 Current flow

Gate `config.py:208-214` (poll 10s, conc 4, `overview_max_tokens 2048`, dead `debounce 30s/manifest 5s`, `max_backlog 10000`); backpressure 503 (`api/app.py:422-430`, `admin/routes.py:472-480`).
Queue = `postgres.py:53-62` (`consolidation_tasks` + unique `(tenant,node_id,task_type)` dedup) + `workflow_dispatches:96-111` outbox; `enqueue_task:464-484` same-txn `CONSOLIDATION` dispatch; `claim_dispatches:486-529` fenced; `ConsolidationWorkflow:141-161 → process_consolidation_activity:1237-1326` (PENDING→PROCESSING→COMPLETE, fail stays PROCESSING; `mark_task_failed:1329-1344`→FAILED). Legacy thread `consolidation/worker.py:43-211` bypassed in V2.
V2 handlers: `consolidate_overview:15-93` (resolve→`get_children`→`get_current_state`→`overview` prompt→`core.complete`→rev re-check→`save_overview:3486-3508` upsert `(tenant,scope,input_revision)`+invalidate), `enqueue_stale_overviews:152-180` (rev-mismatch, LIMIT 200, pri 6), `unmerge_entity:96-149` (Muse-Spark split → `unmerge_entity:4593-4690`, `mutation_key consolidation:{task}:unmerge`).
Triggers: ingest `_enqueue_consolidation:621-647` (OVERVIEW pri5+MANIFEST pri5+PROPAGATE pri7), `memories.py:208-231`, manual `POST /consolidation:59-78`, `memory_repository.py:3562-3571` invalidation pri6, cron `reconciliation.run_once:29-114` + `run_stale_overview_scan:122-159` vs Temporal `scan_stale_overviews:1363-1398`, `repair_dispatches:701-752`.
Current session clarification (2026-09-29): explicit `compact_session` summarizes the oldest uncompacted portion and re-enqueues only ingestible pairs. `commit_session` summarizes only ingestible turns. Normal chat marks all its pairs non-ingestible, so closing a normal chat does not create canonical session summaries or ingest its user facts. A chat compaction helper exists but is uncalled; the threshold alone does not mean automatic compaction is active. See the production audit for lifecycle/concurrency gaps.
Decay `decay.py:36-97` (`alpha·exp(-λ·days)+β·logfreq+γ·centrality`, presets personal/coding/kb, batch 10k, p95-norm `113-143`, per-node `146-187`, HISTORICAL clamp `190-199`): projection-only, global p95, no tenant, no gate input. Schedulers `decay_worker.py:16-48` + `DecayWorkflow:217-226`.

### 6.2 Defects

1. No L1/L2/L3/L4 hierarchy — overview only COLLECTION/PROJECT; no CLAIM/TOPIC/PROFILE level, no `DERIVED_FROM` (only `memory_hierarchy` + `memory_overviews`).
2. No lineage/revision regen below scope: no child-rev map/claim IDs/regen reason; any bump → pri6 LIMIT 200 churn/starvation; debounce dead (unique-index only).
3. `ATOMIZE/NORMALIZE/TEMPORALIZE/INTEGRATE` dead in V2, yet `conflict.py:10-12` promises cleanup; no post-hoc repair except full UNMERGE; TEMPORALIZE regex-first-line too weak.
4. Session double-write: compact re-enqueue + commit re-enqueue ALL + parallel SUMMARY (0.9) with duplicated `key_facts`, no `DERIVED_FROM`, no action on overlap; flag bug promotes answers to facts.
5. Decay diverges: Neo4j-only weights, global p95, ignores gate, assumes status mirror; stale after lag.
6. Reconciliation repairs dispatch not meaning: no wrong-rev overview / CONTRADICT backlog / orphan hierarchy / failed-split detection; FAILED terminal no escalation.
7. Binary backpressure: single 10000 gate, no per-type aging, no episode-close fast lane.
8. Temporal gap: evidence has source/extractor/hash but `valid_from/until` not consumed at query; SUPERSEDE/CONTRADICT no window close-out → stale ANSWERABLE.

### 6.3 Best consolidation

Levels (all PG `memory_nodes`; Neo4j/vector/lexical projections):

- `L0 raw` = EPISODE + SESSION_SUMMARY immutable, evidence-bearing, never rewritten.
- `L1 claim` = ENTITY/PREFERENCE typed claims+versions+evidence+validity (ingest writes; consolidation transitions status via actions only).
- `L2 episode-distill` = per-episode abstract+key-claim list (regen on episode close).
- `L3 topic` = COLLECTION overview over L2s.
- `L4 profile` = PROJECT/PREFERENCE traits, materialized.

Lineage (new PG): `memory_lineage(parent_id,child_id,relation=DERIVED_FROM,created_at)` UNIQUE(tenant,parent,child): L2→L0, L3→L2, L4→L1. Extend `memory_overviews.{child_revision_map JSONB,claim_ids UUID[],trigger_reason,evidence_gate}`; hydration one batch (`get_children`+`get_current_state`+evidence same tx, pattern `canonical.py:29-33` extended). Gate per derived node; `INSUFFICIENT/CONFLICTING`→`NOOP` + re-extract, not hollow publish.

Triggers/thresholds: episode-close → immediate L2 pri 3; accumulation (≥5 new L2s or ≥15 transitions since `input_revision`) pri 5; drift >30% children pri 4 fast lane; cron daily LIMIT 200 pri 6 safety net. Debounce: keep unique-index + `scheduled_at=now+30s` coalesce on PENDING re-enqueue (revive `overview_debounce_seconds`). Backpressure per-type caps, episode-close bypasses backlog gate.

Profile L4 sketch: cron weekly + on ≥10 REINFORCE same PREFERENCE predicate: batch load L1 (predicate IN …, evidence+validity), LLM proposes ADD/REINFORCE/SUPERSEDE per trait citing `claim_ids`, `CONFLICTING`→gate, write PROFILE version + `DERIVED_FROM` + close SUPERSEDEd windows same txn + dispatch. UNMERGE stays ENTITY-split repair under gate. Decay later reads PG validity+access canonically.

Files: NEW migration lineage+overview cols; `consolidation/canonical.py` (L2/L3/L4 fns+gate); `memory_repository.py` (save extension, validity close-out, lineage writers); `postgres.py` (debounced enqueue, per-type depth); `temporal/workflows.py+activities.py` (episode-close+profile); `session/manager.py` (link SUMMARY, drop dup re-enqueue).

---

## 7. Production + repo management

- ADR-001 PG-canonical V2, archive V1 fs sections to `docs/v1-archive/`. One API representation (legacy `410` already prod).
- Split `memory_repository.py` → `storage/canonical/{nodes,claims,aliases,hierarchy,mutations,dispatch,unit_of_work}`; coordinator explicit.
- Fix now: `api/routes/memories.py` `$tenant_id` without param; `deploy/postgres/init/01-databases.sh` distinct secrets, migrator owns, revoke app DDL, `MIGRATION_URL`, `FORCE RLS` + cross-tenant test; Neo4j reader/writer split or `simple: PG+pgvector+FTS` vs `scale: +Neo4j/Redis/Temporal` same semantics.
- CI (no `.github/` today): disposable PG16/Neo4j/Redis + `ruff+mypy+pytest+alembic fresh+N-1 upgrade+RLS+outbox retry+SDK contract`. Mark DB tests clean-skip without `DATABASE_URL`.
- SDKs from OpenAPI, fix `clients/python` `quotas` type, TS coverage, pin images + SBOM + `LICENSE/SECURITY/CONTRIBUTING/CHANGELOG`.
- Observability: `/health//livez//readyz//metrics`, `canonical_revision` per query, projection lag p95, outbox backlog, `temporal:7233` (`engram-prod`, conc 4/1, retention 14d), decay `0 3 * * * UTC`.

---

## 8. Historical build order + exit criteria

The following search/consolidation workstreams remain possible later improvements. The production delivery milestones in [ORDERED_RETRIEVAL_PLAN.md](ORDERED_RETRIEVAL_PLAN.md#delivery-milestones-and-dependencies) supersede their sequence and define the actual release gates.

- Phase 1 ingestion quality: episode+atomic+typed+resolve+normalize+localMatch+actions. Exit: supersede preserves history, no dup files, `allow_filesystem_runtime:false` honored (no `FilesystemStore` in txn).
- Phase 2 L2: PG FTS + vector + BFS stable IDs. Exit: `Recall@5/15` lift LoCoMo/LongMemEval.
- Phase 3 ranking: RRF + cross-encoder + MMR-broad-only. Ablate each; keep measured wins only.
- Phase 4 deep: requirement gate + targeted retry + decomposition. Exit: partial reports exact missing field.
- Phase 5 consolidation: episode→topic→profile + lineage. Exit: `GET /profile+memories` < search-all, evidence-linked.
- Always: `Recall@K/MRR/nDCG/single+multi-hop+temporal/entity/path/rejection-rate/calibration/tokens/cost/p50/p95` versioned (rev/config/models/judge). Prior audit: `263 passed` PG selection; re-run + `rebuild-kg` equality + zero cross-tenant leak per phase.

## 9. Historical search-focused shortlist

This is not the current first-release priority list; see the production revision above.

1. Memory formation: semantic episodes + atomic typed claims + local-match.
2. L2: BM25 + vector + BFS + RRF + cross-encoder.
3. Evidence completion: requirement matrix + targeted missing-fact retrieval.

Preserves correctness-first while making difficult knowledge findable and synthesizable.
