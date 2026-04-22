# Data Model

Everything Engram persists lives in one of four places. This document is
the reference: what goes where, with what shape, and why.

## 1. Filesystem — `mem://` URIs

The filesystem at `./data/mem` (configurable via `filesystem.data_dir`) is
authoritative. Every memory node in the KG has a `source_uri` that maps
directly to a path on disk.

### URI scheme

```
mem://user/                          # user-scoped memories
mem://user/identity/profile.md
mem://user/preferences/coding_style.md
mem://user/entities/alice/
    ├── .manifest                    # one-line-per-child table of contents
    ├── overview.md                  # ~2k-token L3 summary
    └── alice_ml-engineer-at-meta.md # entity document (semantic filename)

mem://user/episodes/
    ├── .manifest
    ├── overview.md
    └── 2026-03-10_moved-to-canada.md

mem://agent/                         # agent-scoped memories (tools, patterns)
mem://shared/                        # shared resources, facts, docs
```

### Semantic filenames (§6.1.2)

Filenames encode a one-line semantic summary so a Core Model `ls` can
decide where to descend without reading file contents:

| Node type | Pattern |
|---|---|
| ENTITY | `{entity_slug}_{summary_slug}.md` |
| EVENT / EPISODE | `{YYYY-MM-DD}_{summary_slug}.md` |
| FACT | `{fact_slug}.md` (parent directory carries the context) |
| DOCUMENT | `{resource_slug}_{summary_slug}.md` |

`engram.uri.semantic_filename()` generates these deterministically.

### `.manifest` (§6.1.3)

One file per directory, newline-separated, one child per line:

```
alice_ml-engineer-at-meta.md | Alice is an ML engineer at Meta working on ranking
project_atlas/ | Distributed ML pipeline project, started Q1 2026
```

Manifests are cheap to read (no YAML parse, no content load) so the Core
Model can browse the tree without scanning full documents.

### `overview.md` (§6.1.4)

Per-directory summary, ≤ `overview_max_tokens` (default 2048). Regenerated
by the `CONSOLIDATE_OVERVIEW` task when children change, with a 30s
debounce to coalesce rapid writes.

### File body layout (§5.4.5.1)

```markdown
---
id: 4b9f-...-uuid
node_type: ENTITY                    # ENTITY | EVENT | FACT | DOCUMENT | DIRECTORY | SESSION_SUMMARY
status: ACTIVE                       # ACTIVE | HISTORICAL | LOW_CONFIDENCE
created_at: 2026-04-12T10:30:00Z
source_session_id: sess-abc-123
schema_version: 1
temporal:
  asserted_at: 2026-04-12T10:30:00Z
  valid_from: null
  valid_until: null
  phrase: "as of April 2026"
normalize:
  canonical_name: "Alice Chen"
  aliases: ["Alice", "A. Chen"]
provenance:
  extractor: core_model_v1
  confidence: 0.87
  ingest_event_id: evt-0194-...
---
L0 abstract sentence goes here on the first body line.

Full resolved text follows as normal Markdown.
```

The first body line is the **L0 abstract** — it's what gets embedded for the
vector index and what the Frontier LLM sees at L1.

### Reserved metadata keys (§6.3)

Validated before every KG index update (`engram.frontmatter.validate_metadata`).
See `_RESERVED_KEY_TYPES` in [`engram/frontmatter.py`](../engram/frontmatter.py).

## 2. Neo4j — derived index

Every `.md` file becomes a `:Node` with a fixed property set.

### Node properties (§6.2)

| Property | Type | Description |
|---|---|---|
| `id` | UUID string | Globally unique, immutable |
| `node_type` | enum | ENTITY / EVENT / FACT / DOCUMENT / DIRECTORY / SESSION_SUMMARY |
| `status` | enum | ACTIVE / HISTORICAL / LOW_CONFIDENCE |
| `source_uri` | string | Canonical `mem://` URI — **unique** |
| `parent_uri` | string | URI of the parent directory (nullable for root) |
| `l0_abstract` | string | One-sentence summary, fulltext-indexed |
| `l0_embedding` | float[384] | BGE-Small mean-pool embedding, vector-indexed |
| `retrieval_weight` | float | Decay-computed, 0.05–1.0 (defaults to 1.0) |
| `created_at` | ISO8601 | Creation time |
| `last_accessed_at` | ISO8601 | Updated on every retrieval hit |
| `access_count` | int | Retrieval hit count, defaults to 0 |
| `source_session_id` | UUID string | Session that created this node |
| `schema_version` | int | Schema version at creation |
| `overview_generated_at` | ISO8601 | DIRECTORY nodes only |
| `superseded_by` | UUID string | For HISTORICAL nodes only |
| `superseded_at` | ISO8601 | When supersession occurred |

### Edges (§6.4)

| Edge type | Purpose | Example |
|---|---|---|
| `CONTAINS` | Parent directory → child node | `(mem://user/entities/) -[CONTAINS]→ (mem://user/entities/alice/)` |
| `IS_PART_OF` | Inverse of CONTAINS | `(alice) -[IS_PART_OF]→ (user/entities/)` |
| `RELATES_TO` | Semantic relationship (extracted) | `(Alice) -[RELATES_TO {label: "works_at"}]→ (Meta)` |
| `SUPERSEDES` | New version replaces old | `(new_fact) -[SUPERSEDES]→ (old_fact)` |
| `REFERENCES` | Cross-reference between nodes | `(meeting_notes) -[REFERENCES]→ (project_atlas)` |

Edge properties on every type:

```
relation_label: string          # controlled vocab — see prompts/relations.yaml
confidence: float               # 0.0–1.0, 1.0 for structural edges
status: ACTIVE | HISTORICAL
created_at: ISO8601
superseded_at: ISO8601 | null
superseded_by: UUID | null
source_session_id: UUID | null
```

### Indexes (§6.6)

```
CREATE VECTOR INDEX  l0_idx           ON (n:Node) WITH (384-dim cosine)
CREATE FULLTEXT INDEX l0_text_idx     ON [n.l0_abstract]
CREATE INDEX          uri_prefix_idx  ON (n.source_uri)
CREATE INDEX          status_idx      ON (n.status)
CREATE INDEX          weight_idx      ON (n.retrieval_weight)
CREATE CONSTRAINT     node_source_uri_unique FOR (n:Node) REQUIRE n.source_uri IS UNIQUE
```

Created by `engram.storage.neo4j_store.Neo4jStore.ensure_indexes()` at
startup (CLI: `engram init`).

## 3. SQLite — control plane (§16.1)

One file: `./data/event_ledger.db`, WAL mode. Contains five tables:

### `events` — ingest event ledger

```
event_id      TEXT PRIMARY KEY       # evt-0194... (UUID suffix)
pair_id       TEXT UNIQUE NOT NULL   # sha256(session_id||user_idx||assistant_idx)
session_id    TEXT
source        TEXT                   # client | session | session_compact | smoke | manual
event_type    TEXT                   # INGEST | QUERY | SESSION_COMMIT
payload       TEXT                   # JSON blob
status        TEXT                   # RECEIVED | GATED_STORE | GATED_SKIP
                                     # | INDEXED | COMPLETE | FAILED
retry_count   INTEGER DEFAULT 0
error_message TEXT
created_at    TEXT DEFAULT datetime('now')
processed_at  TEXT
```

`pair_id` uniqueness is the idempotency anchor: duplicate submissions return
the same `event_id` without re-processing.

### `fs_outbox` — filesystem → KG handoff

```
event_id      TEXT PRIMARY KEY
source_uri    TEXT NOT NULL
state         TEXT               # WRITTEN | INDEXED | INDEX_FAILED
retry_count   INTEGER DEFAULT 0
written_at    TEXT NOT NULL
last_attempt  TEXT
error_message TEXT
```

When Step 5 commits (filesystem write), a `WRITTEN` row appears. Step 6 (KG
update) advances it to `INDEXED` or `INDEX_FAILED`. The reconciliation
worker retries `INDEX_FAILED` rows with exponential backoff up to 3 times.

### `extractions` — S-R-O output per event

```
event_id      TEXT PRIMARY KEY
resolved_text TEXT NOT NULL         # coreference-resolved assistant turn
triplets      TEXT NOT NULL         # JSON array of { subject, relation, object, confidence }
l0_abstract   TEXT NOT NULL
created_at    TEXT DEFAULT datetime('now')
```

### `linked_entities` — triplet → KG node mapping

```
event_id         TEXT NOT NULL
triplet_idx      INTEGER NOT NULL
subject_node_id  TEXT               # mem:// URI of the linked ENTITY node
object_node_id   TEXT
PRIMARY KEY (event_id, triplet_idx)
```

Used by `/memories/{id}/unmerge` and `rebuild-kg` to re-derive RELATES_TO
edges from the ledger after a disaster.

### `consolidation_tasks` — work queue

```
task_id       TEXT PRIMARY KEY
node_id       TEXT NOT NULL
task_type     TEXT NOT NULL         # CONSOLIDATE_OVERVIEW | REGENERATE_MANIFEST
                                    # | PROPAGATE_OVERVIEW | ATOMIZE
                                    # | NORMALIZE | TEMPORALIZE | INTEGRATE
status        TEXT DEFAULT 'PENDING'
priority      INTEGER DEFAULT 5     # 1 (highest) to 10 (lowest)
scheduled_at  TEXT NOT NULL
started_at    TEXT
completed_at  TEXT
retry_count   INTEGER DEFAULT 0
error_message TEXT
```

A **unique index** on `(node_id, task_type)` filtered to PENDING/PROCESSING
implements the §7.5 debounce: duplicate enqueues for the same pair are
silently coalesced.

### `meta` — schema version + maintenance flag

```
key         TEXT PRIMARY KEY        # schema_version | maintenance_mode
value       TEXT NOT NULL
updated_at  TEXT DEFAULT datetime('now')
```

## 4. Redis — session cache (§9.2)

One key per session: `session:{session_id}` → JSON payload:

```json
{
  "session_id": "sess-abc-123",
  "status": "ACTIVE",                 // ACTIVE | WINDOWED | COMMITTING | COMMITTED
  "turns": [
    {"role": "user", "content": "...", "timestamp": "...", "turn_idx": 0},
    {"role": "assistant", "content": "...", "timestamp": "...", "turn_idx": 1}
  ],
  "compacted_turns_idx_upper_bound": 0,
  "compacted": "(multi-line summary)",
  "key_facts": ["..."],
  "key_entities": ["..."],
  "created_at": "..."
}
```

TTL defaults to `session.timeout_minutes` and is refreshed on every session
write. On TTL expiry the Session Manager marks the session COMMITTING,
drains remaining turns into the ingest pipeline, writes a SESSION_SUMMARY
node, and deletes the Redis key.

## 5. Conflict resolution (§6.5)

When a new extracted triplet is about to be written, `engram.ingest.conflict.classify`
compares it against existing ACTIVE RELATES_TO edges from the same subject:

| Case | Detection | Resolution |
|---|---|---|
| DUPLICATE | Same subject, equivalent relation (label match OR rel cosine > 0.9), object cosine > 0.95 | Touch existing edge (increment access_count, update last_accessed_at). Discard incoming. |
| CONTRADICTION | Same subject, equivalent relation, object cosine < 0.5 | Mark old edge HISTORICAL (`status='HISTORICAL'`, `superseded_by=new_edge_id`). Write new ACTIVE edge + SUPERSEDES edge. If old object has no remaining ACTIVE edges, mark it HISTORICAL (orphan detection, §6.5.1). |
| CO_EXISTENCE | Same subject, different relation — or same relation but object cosine 0.5–0.95 (plausible alternative) | Write new ACTIVE edge; no modification to existing edges. |
| ambiguous (0.5 ≤ cos ≤ 0.95) | Rule-based classifier declines | Delegates to `prompts/dedup.j2` (Core Model); on core-model failure, default to CO_EXISTENCE (safer). |

## 6. Soft memory decay (§10)

Daily cron (`engram decay`) recomputes `retrieval_weight` for every ACTIVE node:

```
retrieval_weight = α · recency + β · frequency + γ · centrality
  recency     = exp(-λ · days_since_last_access)
  frequency   = log(1 + access_count) / log(1 + p95_access_count)
  centrality  = relates_to_degree / p95_relates_to_degree
```

Presets at `engram/decay.py`:

| Preset | α / β / γ | λ (half-life) | Rationale |
|---|---|---|---|
| `personal_conversation` | 0.40 / 0.30 / 0.30 | 0.01 (~69 days) | Personal context stays accessible |
| `coding_agent` | 0.40 / 0.50 / 0.10 | 0.05 (~14 days) | Active code context dominates |
| `knowledge_base` | 0.20 / 0.30 / 0.50 | 0.005 (~138 days) | Structural knowledge persists |

HISTORICAL nodes have `retrieval_weight = 0.0`. Nodes below
`dormant_floor` (default 0.05) are excluded from L1 first-pass vector
retrieval via post-filter over-fetch (§10.4).
