# Flows

## 1. Ingest pipeline (§5.3–§5.4)

```
  ┌─────────┐         ┌──────────────┐         ┌──────────────┐
  │ Client  │ ─POST──▶│  /ingest     │ sync    │ events table │
  └─────────┘         │  endpoint    │ INSERT  │ (RECEIVED)   │
                      └──────────────┘ ────────▶└──────────────┘
                              │ 202 Accepted
                              │
                              ▼  (background task or ingest worker)
                  ┌─────────────────────────────┐
                  │  Step 2 — write-path gate    │ Core Model
                  │  (stored=false → GATED_SKIP) │
                  └─────────────────────────────┘
                              │ stored=true → GATED_STORE
                              ▼
                  ┌─────────────────────────────┐
                  │  Step 3 — S-R-O extraction   │ Core Model
                  │  + L0 abstract               │ → extractions table
                  └─────────────────────────────┘
                              │
                              ▼
                  ┌─────────────────────────────┐
                  │  Step 4 — entity linking     │ Core Model + embeddings
                  │  (new entity | matched URI)  │
                  └─────────────────────────────┘
                              │
                              ▼
                  ┌─────────────────────────────┐
                  │  Step 5 — filesystem write   │ atomic temp+rename+fsync
                  │  (AUTHORITATIVE)             │ → fs_outbox (WRITTEN)
                  │  — preserve original turns   │ beneath retrieval summary
                  │  — validate reserved keys    │
                  └─────────────────────────────┘
                              │
                              ▼
                  ┌─────────────────────────────┐
                  │  Step 6 — conflict resolution│ rule-based → optional
                  │  + KG merge + edge writes    │   Core Model dedup
                  │  → fs_outbox (INDEXED)       │
                  │  → events.status = INDEXED   │
                  └─────────────────────────────┘
                              │
                              ▼
                  ┌─────────────────────────────┐
                  │  Step 7 — enqueue consolida- │ CONSOLIDATE_OVERVIEW
                  │  tion tasks                  │ REGENERATE_MANIFEST
                  │  events.status = COMPLETE    │ PROPAGATE_OVERVIEW
                  └─────────────────────────────┘
```

### Idempotency invariants

| Step | Idempotency anchor |
|---|---|
| 1 | `events.pair_id UNIQUE` — resubmits return existing event_id |
| 2 | `events.status` transition is a no-op if already terminal |
| 3 | `extractions.event_id PRIMARY KEY` — re-run writes the same row |
| 4 | `linked_entities.(event_id, triplet_idx) PRIMARY KEY` |
| 5 | Target file content hash — if identical, treat write as successful |
| 6 | `MERGE` on `source_uri` in Neo4j + `MERGE` on `(subject, relation, object)` for edges |
| 7 | Unique index on `(node_id, task_type)` for PENDING/PROCESSING |

### Crash recovery (§5.5)

| Stuck state | Detection | Recovery action |
|---|---|---|
| `events.status = RECEIVED` for > 5 min | Ingest worker crashed before step 2 | Requeue; step 2 is idempotent |
| `events.status = GATED_STORE` with no `extractions` row | Crashed during step 3 | Re-run step 3; the pair payload is in the ledger |
| `fs_outbox.state = WRITTEN` for > 2 min | Crashed between steps 5 and 6 | Re-run step 6. MERGE on `source_uri` is safe. |
| `fs_outbox.state = INDEX_FAILED` with `retry_count < 3` | Neo4j transient failure | Exponential backoff retry |
| `events.status = INDEXED` with no consolidation tasks for parent | Crashed between steps 6 and 7 | Re-run step 7. Unique index deduplicates. |

The Reconciliation Worker (`engram/consolidation/reconciliation.py`) runs
on a 60-second schedule and at process startup, scanning for every case
above.

## 2. Retrieval cascade (§3–§4)

```
POST /api/v1/query
      │
      ▼
  ┌────────────────────────────────────────────┐
  │  L0 gate — run_l0_gate()                   │
  │                                            │
  │  1. Regex OR-gate on first sentence        │
  │     (deixis, anaphora, possessives, …)     │
  │  2. Optional trained classifier (33M BGE)  │
  │  3. Memory-hit fallback:                   │
  │     one embedding + Neo4j vector lookup,   │
  │     cosine ≥ 0.75 overrides BYPASS         │
  │                                            │
  │  Output: BYPASS | CONTINUE                 │
  └────────────────────────────────────────────┘
      │ BYPASS: skip L1–L4, call frontier with session context only
      │ CONTINUE:
      ▼
  ┌────────────────────────────────────────────┐
  │  L1 plan (Core Model)                      │
  │                                            │
  │  Input:                                    │
  │    - user query                            │
  │    - session context                       │
  │    - breadth-first tree render (2k tokens) │
  │    - KG schema summary                     │
  │    - memory-hit abstract (if any)          │
  │                                            │
  │  Output (see §3.2.1):                      │
  │    - predicted_depth (SESSION|L1|L2|L3|L4) │
  │    - mode (AGFS | KG | HYBRID)             │
  │    - vector_queries[]                      │
  │    - entry_points[]                        │
  │    - commands[] (ordered)                  │
  │                                            │
  │  Execute vector_queries → top-K per query  │
  │  → MMR-merge into candidate set            │
  └────────────────────────────────────────────┘
      │  session_sufficient=true → short-circuit to MSC assembly
      │  predicted_depth=L1 → skip to MSC
      ▼
  ┌────────────────────────────────────────────┐
  │  L2 fused plan-judge (Core Model)          │
  │                                            │
  │  Receives L1 results. Single call produces:│
  │    - previous_level_sufficient: bool       │
  │    - terminate_cascade: bool               │
  │    - commands[]: Cypher templates to run   │
  │    - coverage: {covered, missing}          │
  │                                            │
  │  Allowed templates at L2:                  │
  │    t_neighbours_by_relation, t_path_between│
  │    t_temporal_filter, t_history_chain,     │
  │    t_cross_references, t_find_by_uri_prefix│
  │                                            │
  │  Orchestrator runs selected templates      │
  │  against Neo4j with 5s timeout.            │
  └────────────────────────────────────────────┘
      │  terminate_cascade=true → MSC assembly
      │  predicted_depth≥L3:
      ▼
  ┌────────────────────────────────────────────┐
  │  L3 fused plan-judge (Core Model)          │
  │                                            │
  │  overview_for <uri> command reads          │
  │  overview.md for top candidates.           │
  │  Budget: overview_budget_tokens (6000).    │
  └────────────────────────────────────────────┘
      │
      ▼
  ┌────────────────────────────────────────────┐
  │  L4 fused plan-judge (Core Model)          │
  │                                            │
  │  cat <uri> commands read full .md bodies.  │
  │  Multi-hop Cypher for relational reasoning.│
  │  Budget: full_doc_budget_tokens (20000).   │
  │  ALWAYS TERMINATES the cascade.            │
  └────────────────────────────────────────────┘
      │
      ▼
  ┌────────────────────────────────────────────┐
  │  MSC assembly (§4.4)                       │
  │                                            │
  │  Token budget split (§4.4.2):              │
  │    system + query        ≤ 10%             │
  │    session context       ≤ 30%             │
  │    retrieved LTM         ≤ 50%             │
  │    slack (answer)          10%             │
  │                                            │
  │  Order matters for attention:              │
  │    1. system prompt (static)               │
  │    2. session context                      │
  │    3. retrieved LTM (ordered by level)     │
  │       each with inline [ACTIVE], [LOW_CONF]│
  │       and (source: mem://...) annotations  │
  │    4. user query (repeated verbatim)       │
  └────────────────────────────────────────────┘
      │
      ▼
  ┌────────────────────────────────────────────┐
  │  Frontier LLM call                         │
  │                                            │
  │  Output: { verdict, answer | reason,       │
  │            suggested_queries,              │
  │            suggested_depth }               │
  │                                            │
  │  ANSWER → return                           │
  │  NEED_MORE → re-enter with suggested       │
  │    queries, max_reentries times            │
  │                                            │
  │  (Streaming: once ANSWER verdict is known, │
  │   a second stream_answer() call may stream │
  │   the response — §4.3.2)                   │
  └────────────────────────────────────────────┘
```

### Session lifecycle (§9.1)

```
  ACTIVE  ─── window_threshold_ratio breached? ───▶  WINDOWED
    │                                                    │
    │  close or timeout                                   │  close or timeout
    ▼                                                    ▼
  COMMITTING ─────────── (drain + SESSION_SUMMARY) ────▶ COMMITTED
```

* WINDOWED triggers an **auto-compaction** via the Core Model
  (`prompts/session_compact.j2`). The oldest half of the uncompacted turns
  gets summarised; the original turns are **re-enqueued** as INGEST events
  (source=`session_compact`) so the authoritative ingest pipeline still
  sees the raw turns (§8.3.2).
* COMMITTING writes a SESSION_SUMMARY node to the KG, drains the ingest
  pipeline, then deletes the Redis key.

### Conflict resolution flow (§6.5)

For each extracted triplet during Step 6:

```
fetch active RELATES_TO edges from subject
     │
     ▼
score every candidate edge where relation matches
and select the best object match
     ▲
     │
     ├── cosine > 0.95   →  DUPLICATE  (touch existing, no new edge)
     │
     ├── cosine < 0.50   →  different value
     │                       ├── explicit_correction=true → CONTRADICTION
     │                       │   └── old HISTORICAL + FACT SUPERSEDES edge
     │                       └── otherwise → CO_EXISTENCE
     │
     ├── 0.50–0.95 band  →  ambiguous
     │                       └── core model (prompts/dedup.j2) decides
     │                       └── contradiction requires explicit evidence
     │                       └── unknown returned edge IDs are rejected
     │                       └── fallback → CO_EXISTENCE (safer)
     │
     └── no match         →  CO_EXISTENCE  (write new ACTIVE edge)
```

The classifier evaluates all same-relation edges before deciding; an early
dissimilar value cannot hide a later exact duplicate. Different objects alone
do not imply contradiction because most relations permit plurality/history.

## 3. Consolidation worker loop

```
poll consolidation_tasks (PENDING, order by priority, scheduled_at)
     │
     ▼  atomically: status = PROCESSING
dispatch on task_type:
     REGENERATE_MANIFEST   →  rebuild .manifest from directory listing
     CONSOLIDATE_OVERVIEW  →  read children abstracts + relations
                               → Core Model → write overview.md
     PROPAGATE_OVERVIEW    →  enqueue CONSOLIDATE_OVERVIEW for each ancestor
     ATOMIZE               →  split compound triplets (e.g. "coffee and tea")
     NORMALIZE             →  map to canonical relation labels & entity names
     TEMPORALIZE           →  extract ISO date from body → metadata.temporal
     INTEGRATE             →  reset event status so step 6 replays with updates
     │
     ▼
status = COMPLETE (or FAILED with error_message)
```

## 4. Decay cron (§10.3)

```
Daily at 03:00 local (configurable via decay.schedule):
  1. Compute p95(access_count) and p95(relates_to_degree) over ACTIVE nodes
  2. For each ACTIVE node in batches of 10_000:
       retrieval_weight = α·recency + β·frequency + γ·centrality
       WRITE back to Neo4j
  3. HISTORICAL nodes: retrieval_weight = 0.0
  4. Nodes below dormant_floor (0.05) are skipped by L1 first-pass retrieval
     but accessible via explicit L2+ Cypher templates.
```
