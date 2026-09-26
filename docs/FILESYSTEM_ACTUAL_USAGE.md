# Actual Usage of the Filesystem in Engram

**Analysis date:** 2026-09-09

## 1. Purpose of this document

This document explains why Engram currently uses a filesystem, exactly what it
stores, which runtime operations depend on it, and what would stop working if the
generated files were removed today.

The filesystem discussed here is the runtime memory directory configured by
`filesystem.data_dir`. It does not mean repository documents such as `README.md`
or files under `docs/`.

## 2. Short answer

The filesystem is currently used as the **canonical store for the complete memory
content and frontmatter metadata**.

The main responsibility of each component is:

| Component | Current responsibility |
|---|---|
| Filesystem | Full memory bodies, frontmatter, hierarchy, overviews, manifests, facts, and session summaries |
| PostgreSQL | Events, extractions, entity mappings, queues, retries, and workflow state |
| Neo4j | Searchable nodes, embeddings, semantic relationships, and graph traversal |
| Redis | Temporary session state and cached overviews |
| Temporal | Durable execution and retry of background workflows |

In simple terms:

```text
Filesystem        = the complete memory
Neo4j              = how memories are found and connected
PostgreSQL         = how ingestion and background processing are tracked
Temporal           = how background processing is executed reliably
```

The source code explicitly describes the filesystem as authoritative in
[`filesystem.py`](../engram/storage/filesystem.py#L1).

## 3. What the filesystem stores

Generated content is stored under a tenant-specific directory:

```text
<data_dir>/<tenant-id>/user/
├── entities/
│   └── angie-jones/
│       ├── angie-jones.md
│       ├── overview.md
│       └── .manifest
├── episodes/
│   └── 2026-09-09_angie-became-director.md
├── facts/
│   └── <event-id>/0_related-to_example.md
└── session_summaries/
    └── <session-id>.md
```

### Entity files

Entity files represent people, organisations, projects, and other values promoted
to entities:

```text
mem://user/entities/angie-jones/angie-jones.md
```

They contain the entity name, an initial description, identity metadata, aliases,
status, and provenance.

### Episode files

Episode files preserve the information extracted from one accepted conversation
event:

```text
mem://user/episodes/2026-09-09_angie-became-director.md
```

They contain the short abstract and the complete resolved text.

### Fact files

Low-confidence triplets are stored separately:

```text
mem://user/facts/<event-id>/<fact>.md
```

These files are marked `LOW_CONFIDENCE` so uncertain facts are not treated as
ordinary confirmed relationships.

### Session-summary files

Committed conversation summaries are stored as:

```text
mem://user/session_summaries/<session-id>.md
```

### Overview files

Consolidation generates an `overview.md` inside relevant memory directories. It
contains a model-generated summary used by L3 retrieval.

### Manifest files

Consolidation generates `.manifest` files containing the immediate children and
short descriptions of a directory. They are used as lightweight hierarchy indexes.

## 4. Why Markdown is used

Each memory is human-readable and contains YAML frontmatter plus a Markdown body:

```markdown
---
id: 37a29204-4670-418a-bc65-232909ce5899
node_type: ENTITY
status: ACTIVE
created_at: 2026-09-09T10:00:00Z
source_session_id: session-123
schema_version: 1
normalize:
  canonical_name: Angie Jones
  aliases:
    - Angie Jones
provenance:
  extractor: core_model_v1
  confidence: 0.9
  ingest_event_id: evt-123
---
Angie Jones (entity).

Angie Jones is a Senior Test Automation Engineer at TechNova.
```

The format originally provides:

- simple local persistence;
- easy developer inspection;
- human-readable content;
- content and metadata in one object;
- natural directory organisation;
- straightforward backup and export;
- no need to query a database to inspect a memory body.

Parsing and validation are implemented in
[`frontmatter.py`](../engram/frontmatter.py#L18).

## 5. `mem://` URI and physical file location

A memory identifier such as:

```text
mem://user/entities/angie-jones/angie-jones.md
```

is converted to a tenant-specific filesystem path:

```text
<data_dir>/<tenant-id>/user/entities/angie-jones/angie-jones.md
```

The URI currently serves two purposes:

1. logical identity used across the application;
2. physical file locator used by `FilesystemStore`.

This creates tight coupling between the public identity and the `.md` storage
implementation. URI resolution is implemented in
[`FilesystemStore.path_for`](../engram/storage/filesystem.py#L48).

## 6. Filesystem usage during ingestion

### Write the episode

Every event that passes the write gate creates an episode Markdown file containing
the extracted abstract and resolved conversation text. See
[`_write_episode`](../engram/ingest/worker.py#L321).

### Write new entities

When entity resolution does not find an existing entity, ingestion creates an
entity directory and entity Markdown file. See
[`_write_entity`](../engram/ingest/worker.py#L350).

### Reuse existing entities

When an entity already exists, its URI is reused. The existing entity file is not
updated:

```python
if ctx.fs.exists(uri):
    return uri
```

Therefore, an entity file normally remains a first-mention snapshot. Updated facts
are stored in newer episode files and Neo4j relationships rather than rewriting
the entity body.

### Write uncertain facts

Triplets with confidence from `0.3` through below `0.6` become low-confidence fact
files. See
[`_write_low_confidence_fact`](../engram/ingest/worker.py#L512).

### Validate generated content

Ingestion reads generated episode and entity files back and validates required
frontmatter fields and reserved metadata types. See
[`_validate_written_frontmatter`](../engram/ingest/worker.py#L587).

### Record the write in the database

The control-plane database stores an `fs_outbox` record and maps extracted
triplets to their resolved subject/object URIs. These records support retries and
graph reconstruction.

The file write, database update, and Neo4j update are not one atomic transaction.

## 7. Filesystem usage during consolidation

| Consolidation task | Actual filesystem use |
|---|---|
| `REGENERATE_MANIFEST` | Scans a directory and writes `.manifest` |
| `CONSOLIDATE_OVERVIEW` | Reads child abstracts and writes `overview.md` |
| `PROPAGATE_OVERVIEW` | Queues ancestor overview work that later reads/writes files |
| `ATOMIZE` | No direct file use; updates extraction rows |
| `NORMALIZE` | No direct file use; updates extraction rows using Neo4j and embeddings |
| `TEMPORALIZE` | Reads a memory file and rewrites its YAML temporal metadata |
| `INTEGRATE` | Requeues ingestion, which can rewrite generated files |
| `UNMERGE` | Reads a merged entity and writes split entity files |

Normal ingestion automatically schedules overview, manifest, and overview
propagation tasks. It does not automatically schedule every semantic task.

The handlers are defined in
[`consolidation/tasks.py`](../engram/consolidation/tasks.py#L38).

## 8. Filesystem usage during retrieval

| Retrieval level | Actual filesystem use |
|---|---|
| L0 | No direct file use; classifies and plans the request |
| L1 | Can use filesystem hierarchy and first-line descriptions for discovery |
| L2 | Primarily uses Neo4j vector and graph queries |
| L3 | Reads the generated `overview.md` |
| L4 | Reads the complete Markdown body and frontmatter |

L3 overview reading and L4 body reading are implemented in
[`retrieval/orchestrator.py`](../engram/retrieval/orchestrator.py#L390).

At L4, frontmatter is also read to determine:

- whether the memory is active or historical;
- whether it is low confidence;
- what confidence annotation should be shown.

See [`_read_full_body`](../engram/retrieval/orchestrator.py#L573) and
[`_status_for`](../engram/retrieval/orchestrator.py#L582).

## 9. Filesystem usage by the Memory API

The Memory API uses the filesystem to:

- return a complete memory body;
- return its frontmatter;
- check whether a memory exists;
- mark a memory `HISTORICAL` during retirement;
- scan `*.md` files as a fallback when Neo4j listing is unavailable.

It queries Neo4j separately to return relationships. A response can therefore
combine an older file body with newer graph relationships.

See [`memories.py`](../engram/api/routes/memories.py#L41).

## 10. Filesystem usage by session management

When a session is committed, Engram creates a durable session-summary Markdown
file and indexes a projection of it in Neo4j. This happens directly in the session
lifecycle rather than through a normal consolidation task.

See
[`_write_session_summary_node`](../engram/session/manager.py#L289).

## 11. Filesystem usage for Neo4j recovery

The Neo4j rebuild process currently:

1. deletes existing Neo4j nodes;
2. scans every generated `.md` file;
3. parses its frontmatter and body;
4. recreates Neo4j nodes and containment relationships;
5. reads extractions and linked-entity mappings from PostgreSQL;
6. recreates semantic relationships.

The complete recovery input is currently:

```text
Generated Markdown + control-plane database -> rebuilt Neo4j
```

See [`rebuild`](../engram/rebuild_kg.py#L34).

## 12. Filesystem usage in deployment

API and background worker processes must see the same generated files. Production
therefore requires a shared persistent volume.

The volume must survive:

- process restarts;
- container replacement;
- application deployments;
- API/worker scheduling on different machines.

This complicates horizontal scaling because multiple replicas require compatible
shared read/write storage.

## 13. Atomic write benefit and transaction limitation

`FilesystemStore.write_atomic` uses a temporary file, flush, `fsync`, atomic
replace, and parent-directory `fsync`. See
[`write_atomic`](../engram/storage/filesystem.py#L52).

This protects one file against partial content. It does not create a transaction
covering:

```text
episode file
+ entity files
+ PostgreSQL rows
+ Neo4j nodes and edges
+ consolidation tasks
```

A crash between these operations can leave one store ahead of another.

## 14. Existing-entity example

Suppose the first event says:

```text
Angie Jones is a Senior Test Automation Engineer at TechNova.
```

The entity file is created with that description.

A later event says:

```text
Angie Jones became Director of Quality Engineering on 2026-09-01.
```

The current result is:

| Location | Result |
|---|---|
| Angie entity file | Normally keeps the original Senior Test Automation Engineer description |
| New episode file | Stores the Director update |
| Neo4j | Stores the new role and effective-date relationships |
| `overview.md` | May synthesize both file and graph information |

Entity resolution correctly reuses Angie’s identity, but the filesystem entity
document does not become a structured current profile.

## 15. What the filesystem does not do

The filesystem does not provide:

- event queue management;
- workflow execution;
- retry scheduling;
- vector similarity search;
- graph relationship traversal;
- relational transactions across the full ingestion operation;
- structured claim-level versioning;
- reliable conflict enforcement across differently named predicates;
- database constraints for tenant-scoped memory relationships.

Those responsibilities are handled by PostgreSQL, Neo4j, Redis, Temporal,
and application logic.

## 16. Why it cannot be removed today

Removing the generated memory directory today would break or degrade:

- episode creation;
- creation of new entities;
- low-confidence fact storage;
- frontmatter validation;
- manifests;
- consolidated overviews;
- temporal frontmatter updates;
- unmerge;
- full Memory API responses;
- memory retirement;
- session-summary persistence;
- L1 hierarchy navigation;
- L3 overview retrieval;
- L4 full-content retrieval;
- filesystem fallback listing;
- complete Neo4j reconstruction.

Removing only the `.md` extension would not solve the architecture problem. The
important requirement is to move ownership of the content and lifecycle state out
of files.

## 17. Recommended future responsibility

The production target should be:

```text
PostgreSQL = canonical nodes, versions, claims, provenance, hierarchy, and overviews
Neo4j      = rebuildable search and graph projection
Redis      = optional cache and transient session state
Temporal   = background orchestration
Filesystem = optional one-way human-readable export
```

Before generated files are disabled, PostgreSQL must replace all of these current
filesystem responsibilities:

1. full memory bodies;
2. frontmatter/lifecycle metadata;
3. immutable versions and provenance;
4. typed current and historical claims;
5. memory hierarchy;
6. consolidated overviews;
7. low-confidence facts;
8. session summaries;
9. retire and unmerge state;
10. complete source data for rebuilding Neo4j.

Only after all readers, writers, lifecycle operations, and recovery procedures are
database-backed should the deployment remove its shared filesystem volume.
