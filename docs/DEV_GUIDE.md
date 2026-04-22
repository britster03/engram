# Developer Guide

## Code layout

```
engram/
├── __init__.py
├── config.py               # Pydantic config + ${VAR} env interpolation
├── cli.py                  # init / health / smoke / rebuild-kg / decay / migrate
├── deps.py                 # singleton app state (stores + providers + embedder)
├── frontmatter.py          # YAML frontmatter parser + required/reserved-key validators
├── uri.py                  # mem:// ⇄ filesystem path helpers
├── prompts.py              # Jinja2 template loader (cached)
├── relations.py            # controlled relation-label vocabulary loader
├── decay.py                # soft memory decay formulas and daily run
├── rebuild_kg.py           # reconstruct Neo4j from filesystem + extractions
├── metrics.py              # Prometheus registry + standard series
│
├── api/
│   ├── app.py              # FastAPI app factory + lifespan (workers)
│   ├── auth.py             # bearer-token AuthDep
│   ├── rate_limit.py       # token-bucket middleware
│   ├── schemas.py          # request/response DTOs
│   └── routes/
│       ├── sessions.py     # /api/v1/sessions*
│       ├── memories.py     # /api/v1/memories*
│       ├── events.py       # /api/v1/events/{id}/retry
│       └── consolidation.py# /api/v1/consolidation/{status,trigger}
│
├── storage/
│   ├── sqlite.py           # event ledger + outbox + consolidation queue
│   ├── neo4j_store.py      # writer/reader drivers + vector search
│   ├── filesystem.py       # atomic write + manifest + overview helpers
│   └── redis_cache.py      # SessionCache with redis / memory backends
│
├── models/
│   ├── core.py             # CoreModelProvider ABC + JSON extractor
│   ├── frontier.py         # FrontierLLMProvider ABC + streaming contract
│   ├── embeddings.py       # BGE-Small service (singleton)
│   └── providers/
│       └── anthropic_provider.py   # Claude adapter (core + frontier)
│
├── ingest/
│   ├── worker.py           # 7-step pipeline
│   ├── conflict.py         # DUPLICATE / CONTRADICTION / CO_EXISTENCE
│   ├── entity_linker.py    # disambiguation with cosine + token overlap
│   └── unmerge.py          # split merged entities per-source
│
├── retrieval/
│   ├── orchestrator.py     # L0→L4 cascade + re-entry + MSC assembly
│   ├── l0_gate.py          # regex + classifier + memory-hit fallback
│   ├── templates.py        # Cypher template loader + parameter validation
│   └── tree_render.py      # breadth-first directory rendering (§7.6)
│
├── session/
│   └── manager.py          # session lifecycle + compaction + re-ingest
│
├── consolidation/
│   ├── worker.py           # SQLite queue poller + dispatcher
│   ├── tasks.py            # handlers for all 7 task types
│   └── reconciliation.py   # stuck-state scanner + directory staleness
│
├── migrations/
│   ├── runner.py           # pending-migration runner + meta table
│   └── scripts/            # numbered migration scripts
│
├── training/               # optional ML scripts (deferred until traces exist)
│   ├── trace_collector.py
│   ├── gate_classifier.py
│   ├── core_sft.py
│   └── core_dpo.py
│
└── prompts/                # Jinja2 templates + relations.yaml
    ├── gate_write.j2
    ├── extract.j2
    ├── l1_plan.j2
    ├── ln_plan.j2
    ├── dedup.j2
    ├── entity_link.j2
    ├── overview.j2
    ├── session_compact.j2
    ├── unmerge.j2
    └── relations.yaml
```

## Running things

```bash
# Run everything (unit + integration using stubs — no Docker / API key needed)
.venv/bin/pytest -q

# Only the fast unit tests
.venv/bin/pytest tests/unit -q

# Integration (stub providers, FakeNeo4jStore)
.venv/bin/pytest tests/integration -q

# Lint + types
.venv/bin/ruff check .
.venv/bin/mypy engram
```

## Extending the system

### Add a Core Model task

1. Add a Jinja template under `engram/prompts/{task}.j2`.
   Follow the convention of starting with `[TASK_TAG]` in the system prompt.
2. Call it from the appropriate module:
   ```python
   from engram import prompts
   prompt = prompts.render("my_task", var1=foo)
   result = ctx.core.complete(system_prompt=prompt, user_prompt="…")
   ```
3. Register a handler in `tests/integration/stubs.py` so stub-based tests
   don't hit a real API.

### Add a Cypher template

1. Drop `templates/cypher/{name}.cypher` with parameter placeholders.
2. Add it to `_DEFAULT_PARAMS` and `_REQUIRED` in
   `engram/retrieval/templates.py`.
3. It's now available to the orchestrator via the `template` field of L2+
   plans. Also add it to the CLI-style aliases in `_alias_to_template`
   (orchestrator.py) if you want a short-form command.

All templates run under a 5s (L2) or 15s (L4) timeout, are bounded by
LIMIT, and are status-filtered by default.

### Add a consolidation task

1. Add a handler in `engram/consolidation/tasks.py` with the signature
   `(*, node_id: str, sqlite: ..., fs: ..., neo4j: ..., core: ..., embed: ..., cfg: ...)`.
2. Add a dispatch branch in `engram/consolidation/worker.py::_dispatch`.
3. Whitelist the task_type in `engram/api/routes/consolidation.py::trigger`.
4. Enqueue instances from the appropriate call site (ingest worker, session
   commit, cron, or via `POST /api/v1/consolidation/trigger`).

The unique index on `(node_id, task_type) WHERE status IN (PENDING, PROCESSING)`
automatically dedupes enqueues — no explicit debounce code needed.

### Add a new LLM provider

1. Implement `CoreModelProvider` and/or `FrontierLLMProvider` subclasses in
   a new `engram/models/providers/your_provider.py`.
2. Add a branch in `build_core_provider` / `build_frontier_provider`.
3. Update `config.yaml` `provider` enum literal types if needed.

## Testing strategy

Tests are split across `tests/unit/` and `tests/integration/`:

| Layer | What we cover | Backends |
|---|---|---|
| Unit | Pure functions, parsers, classifiers, schemas | in-memory SQLite |
| Integration | Full ingest → KG → retrieval flow | `StubCoreProvider`, `StubFrontierProvider`, `StubEmbeddingService`, `FakeNeo4jStore` |

There are **no live-API tests**: tests must run green without network,
without Docker, without API keys. Real end-to-end validation is the
`engram smoke` CLI command, which runs against the live stack.

### Stubs

`tests/integration/stubs.py` provides deterministic stand-ins for every
LLM and embedding dependency:

* `StubCoreProvider` dispatches on the `[TASK_TAG]` at the start of the
  system prompt — add a handler for each new task in `_HANDLERS`.
* `StubEmbeddingService` derives 384-dim vectors from SHA-256 — same text
  in, identical vector out.
* `FakeNeo4jStore` implements a cosine-similarity `vector_search` over an
  in-memory dict.

### Crash-recovery tests

See `tests/integration/test_crash_recovery.py` — each `events.status` /
`fs_outbox.state` stuck-state from §5.5 has a corresponding test. Flag
a regression if you touch the ingest worker or reconciliation.

## Design invariants

These are non-negotiable; protect them in review:

1. **Filesystem is authoritative.** Never rely on Neo4j being the
   source-of-truth for content — if it disagrees with disk, disk wins.
2. **Every pipeline step is idempotent.** Re-running on a partially-failed
   event must produce the same state. Enforced by `pair_id` uniqueness,
   `source_uri` MERGE in Neo4j, and content-hash check on filesystem writes.
3. **No raw Cypher from the Core Model.** Only parameterised templates from
   the whitelist. The Core Model sees `{"template": "t_children_of", "params": {...}}`,
   never a Cypher string.
4. **Reserved metadata keys are typed.** `validate_metadata()` runs before
   every KG merge; malformed frontmatter produces `FrontmatterError` →
   event status FAILED.
5. **Control-plane data lives in SQLite, not Neo4j.** Don't reach for
   Neo4j to track retry counts, queue depth, or event status.

## Common gotchas

* **`executescript` commits implicitly.** Don't call it inside a
  `sqlite.transaction()` — you'll hit "no transaction is active" on commit.
  See `engram/migrations/runner.py::ensure_meta` for the pattern.
* **BGE-Small returns unit-normalised vectors when `normalize_embeddings=True`.**
  Cosine similarity then reduces to a dot product. Our `_cosine` helper
  still computes the full form — don't "optimise" by removing the norm.
* **Neo4j `shortestPath` doesn't accept property predicates inline** —
  filter after the MATCH. See `templates/cypher/t_path_between.cypher`.
* **`anthropic` SDK requires an API key at constructor time.** The
  `build_core_provider` and `build_frontier_provider` functions raise
  early if the key is missing; don't defer this check.
