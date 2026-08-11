"""End-to-end live validation against a remote model + real Neo4j + real Redis.

Defaults to Ollama Cloud, matching the runtime config.

Prerequisites:
  1. Docker running; `docker compose up -d` has brought up Neo4j + Redis.
  2. `.env` contains a real `OLLAMA_API_KEY`.
  3. `./data/` is writable (or set `ENGRAM_DATA_DIR` to another location).

Usage:
    python scripts/validation/openai_live_test.py
    python scripts/validation/openai_live_test.py --model gemma4:31b
    python scripts/validation/openai_live_test.py --provider openai \
        --model gpt-4o-mini --api-key-env OPENAI_API_KEY
    python scripts/validation/openai_live_test.py --provider groq \
        --model llama-3.3-70b-versatile

The script is self-contained: it wipes any prior `tenant_id=_default`
KG state, ingests five turn pairs via the real Core Model, prints the
resulting KG, then runs three queries through the L0→L2 cascade. Every
latency is measured; the summary reports total runtime and a rough cost
estimate.

Cost depends on the selected remote provider and model.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))


# --- Env loading ----------------------------------------------------------

def _load_env(repo_root: Path) -> None:
    env_path = repo_root / ".env"
    if not env_path.exists():
        raise SystemExit(
            "Missing .env. Copy `.env.example` to `.env` and fill in "
            "OLLAMA_API_KEY."
        )
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def _require_real_key(name: str) -> None:
    v = os.environ.get(name, "")
    if not v or "REPLACE_WITH" in v:
        raise SystemExit(f"{name} is empty or still has the placeholder in .env")


# --- Corpus ---------------------------------------------------------------

CORPUS = [
    ("I just started a staff software engineer role on the Ollama Cloud team.",
     "That's exciting — when did you start?"),
    ("My first day was November 3rd, 2025.",
     "Welcome aboard."),
    ("I'm renting an apartment in Hayes Valley, San Francisco, through end of 2026.",
     "Noted."),
    ("My wife's birthday is June 14th.",
     "I'll remember."),
    ("I'm working on Project Helix — our post-training evaluation pipeline.",
     "Sounds interesting, tell me more later."),
]

QUERIES = [
    "Where does the user work and what team?",
    "When is the user's wife's birthday?",
    "Where does the user live and until when?",
]


# --- Main -----------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--provider", default="ollama_cloud",
                        help="core_model.provider (ollama_cloud | ollama | openai | groq | gemini)")
    parser.add_argument("--model",   default="gemma4:31b",
                        help="model identifier for both core + frontier")
    parser.add_argument("--api-key-env", default=None,
                        help="environment variable containing the provider API key")
    parser.add_argument("--max-depth", default="L2",
                        help="max cascade depth (L1, L2, L3, L4)")
    parser.add_argument("--data-dir", default="/tmp/engram_openai_test",
                        help="where to put transient SQLite + filesystem data")
    parser.add_argument("--keep-data", action="store_true",
                        help="don't wipe the data dir before starting")
    args = parser.parse_args(argv)

    _load_env(_REPO_ROOT)
    api_key_env = args.api_key_env or (
        "OLLAMA_API_KEY" if args.provider == "ollama_cloud" else "OPENAI_API_KEY"
    )
    _require_real_key(api_key_env)
    _require_real_key("NEO4J_ADMIN_PASSWORD")

    os.environ["ENGRAM_LOG_FORMAT"] = "plain"
    os.environ["ENGRAM_LOG_LEVEL"] = os.environ.get("ENGRAM_LOG_LEVEL", "WARNING")

    data_dir = Path(args.data_dir)
    if data_dir.exists() and not args.keep_data:
        shutil.rmtree(data_dir)

    # Imports happen after env is loaded so module-level config reads succeed.
    from engram.config import EngramConfig
    from engram.ingest.worker import IngestContext, process_event
    from engram.models.embeddings import EmbeddingService
    from engram.models.providers import build_providers
    from engram.retrieval.orchestrator import OrchestratorContext, run_query
    from engram.storage.filesystem import FilesystemStore
    from engram.storage.neo4j_store import Neo4jStore
    from engram.storage.sqlite import SqliteStore
    from engram.tenancy import Tenant, TenantQuotas, set_current_tenant
    from engram.uri import pair_id as pair_id_fn

    cfg = EngramConfig.model_validate({
        "api":           {"api_key": os.environ["ENGRAM_API_KEY"]},
        "core_model":    {"provider": args.provider, "model_path": args.model,
                          "api_key": os.environ[api_key_env],
                          "temperature": 0.1, "max_tokens": 1024},
        "frontier_llm":  {"provider": args.provider, "model_path": args.model,
                          "api_key": os.environ[api_key_env],
                          "temperature": 0.3, "max_tokens": 1024},
        "filesystem":    {"data_dir": str(data_dir / "mem")},
        "event_ledger":  {"path": str(data_dir / "ev.db")},
        "consolidation": {"db_path": str(data_dir / "cons.db"),
                          "max_backlog": 10000},
        "session_cache": {"backend": "redis", "redis_url": "redis://localhost:6379"},
        "knowledge_graph": {
            "backend": "neo4j", "uri": "bolt://localhost:7687",
            "writer_username": "neo4j",
            "writer_password": os.environ["NEO4J_ADMIN_PASSWORD"],
            "reader_username": "neo4j",
            "reader_password": os.environ["NEO4J_ADMIN_PASSWORD"],
        },
        "retrieval":     {"l0_skip": True, "max_depth": args.max_depth, "max_reentries": 1},
    })

    # --- Storage + providers + tenant context ----------------------------
    neo    = Neo4jStore(cfg.knowledge_graph)
    neo.ensure_indexes()
    # Clean slate for the _default tenant
    with neo.writer().session() as s:
        s.run("MATCH (n:Node) WHERE n.tenant_id = '_default' DETACH DELETE n").consume()

    sqlite         = SqliteStore(cfg.event_ledger.path)
    fs             = FilesystemStore(cfg.filesystem.data_dir)
    core, frontier = build_providers(cfg.core_model, cfg.frontier_llm)
    embed          = EmbeddingService.get(cfg.gating)

    set_current_tenant(Tenant(
        tenant_id="_default", display_name="", api_key_hashes=[],
        quotas=TenantQuotas(), status="ACTIVE",
    ))

    ingest = IngestContext(
        cfg=cfg, sqlite=sqlite, fs=fs, neo4j=neo, core=core, embed=embed,
    )
    orch = OrchestratorContext(
        cfg=cfg, fs=fs, neo4j=neo, core=core, frontier=frontier, embed=embed,
    )

    # --- Ingest ----------------------------------------------------------
    print(f"\nINGESTING {len(CORPUS)} TURN PAIRS "
          f"VIA {args.provider}/{args.model} → Neo4j + Redis")
    print("=" * 70)
    t_ingest = time.perf_counter()
    session_id = "validation-live"
    for idx, (user_msg, asst_msg) in enumerate(CORPUS):
        pid = pair_id_fn(session_id, idx * 2, idx * 2 + 1)
        eid, _ = sqlite.record_event(
            pair_id=pid, session_id=session_id, source="validation",
            event_type="INGEST",
            payload={"turn_pair": {
                "user":      {"content": user_msg, "turn_idx": idx * 2},
                "assistant": {"content": asst_msg, "turn_idx": idx * 2 + 1},
            }},
        )
        t0 = time.perf_counter()
        final = process_event(ingest, eid)
        dt = time.perf_counter() - t0
        ex = sqlite.get_extraction(eid)
        trips = len(ex["triplets"]) if ex else 0
        abstract = (ex["l0_abstract"][:60] if ex else "")
        print(f"  [{idx + 1}/{len(CORPUS)}] {final:10} ({dt:4.1f}s)  "
              f"triplets={trips}  → {abstract}")
    print(f"\n  total ingest time: {time.perf_counter() - t_ingest:.1f}s")

    # --- KG summary ------------------------------------------------------
    print("\nKG STATE AFTER INGEST")
    print("=" * 70)
    with neo.reader().session() as s:
        node_rows = list(s.run(
            "MATCH (n:Node) WHERE n.tenant_id = '_default' "
            "RETURN n.source_uri AS uri, n.node_type AS t, n.l0_abstract AS abs_ "
            "ORDER BY n.node_type, n.source_uri"
        ))
        edge_rows = list(s.run(
            "MATCH ()-[r:RELATES_TO]->() "
            "WHERE r.status = 'ACTIVE' "
            "RETURN r.relation_label AS rel, count(*) AS c ORDER BY c DESC"
        ))

    type_counts: dict[str, int] = {}
    for r in node_rows:
        k = r["t"] or "DIRECTORY"
        type_counts[k] = type_counts.get(k, 0) + 1
    print(f"  {len(node_rows)} nodes:  "
          + ", ".join(f"{v} {k}" for k, v in sorted(type_counts.items())))
    print(f"  {sum(e['c'] for e in edge_rows)} RELATES_TO edges:  "
          + ", ".join(f"{e['rel']}x{e['c']}" for e in edge_rows))

    # --- Queries ---------------------------------------------------------
    print(f"\nQUERYING VIA {args.provider}/{args.model} CASCADE")
    print("=" * 70)
    t_query = time.perf_counter()
    for q in QUERIES:
        t0 = time.perf_counter()
        result = run_query(orch, session_id=None, query=q)
        dt = time.perf_counter() - t0
        md = result.retrieval_metadata
        print(f"\n  Q: {q}")
        print(f"  A: {result.answer}")
        print(f"     [{dt:4.1f}s, cascade={md.cascade_depth_reached}, "
              f"nodes={md.nodes_retrieved}, reentries={md.reentries}]")

    total_query = time.perf_counter() - t_query
    print(f"\n  total query time: {total_query:.1f}s "
          f"({total_query / len(QUERIES):.1f}s avg)")

    print("\nDONE ✓  (see docs/VALIDATION.md for the reference run)\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
