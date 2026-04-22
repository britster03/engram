"""Engram CLI — init and smoke-test utilities.

Usage:
  python -m engram.cli init     # create Neo4j indexes + SQLite schemas
  python -m engram.cli smoke    # ingest a small corpus + run a query
  python -m engram.cli health   # print component readiness
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

from engram.deps import build_state, get_state, make_ingest_context, make_orchestrator_context
from engram.ingest.worker import process_event
from engram.retrieval.orchestrator import run_query
from engram.uri import pair_id as pair_id_fn


def cmd_init(_args: argparse.Namespace) -> int:
    state = build_state()
    print(f"Neo4j ping: {state.neo4j.ping()}")
    print(f"Redis ping: {state.session_cache.ping()}")
    print("Ensuring Neo4j indexes…")
    state.neo4j.ensure_indexes()
    print(f"SQLite: {state.cfg.event_ledger.path}")
    print(f"Filesystem: {state.fs.data_dir}")
    print("init OK")
    return 0


def cmd_health(_args: argparse.Namespace) -> int:
    state = get_state()
    print(json.dumps({
        "neo4j": state.neo4j.ping(),
        "redis": state.session_cache.ping(),
        "filesystem": state.fs.data_dir.exists(),
    }, indent=2))
    return 0


_SMOKE_CORPUS = [
    {
        "user": "I just accepted a job at Meta on the recommendations team.",
        "assistant": "Congratulations! When do you start?",
    },
    {
        "user": "I start May 4th. It's a staff ML engineer role.",
        "assistant": "That sounds great. Are you moving to the Bay Area?",
    },
    {
        "user": "Yes, I'll be in Menlo Park by April 28th.",
        "assistant": "Do you have a place lined up already?",
    },
    {
        "user": "I'm renting a place in Palo Alto for six months while I look around.",
        "assistant": "Makes sense — good luck with the transition!",
    },
    {
        "user": "My wife's birthday is July 12th, by the way.",
        "assistant": "Noted. Let me know if you want gift-idea help closer to then.",
    },
]


def cmd_rebuild_kg(_args: argparse.Namespace) -> int:
    from engram.config import get_config
    from engram.rebuild_kg import rebuild
    stats = rebuild(get_config())
    print(json.dumps(stats, indent=2))
    return 0


def cmd_decay(_args: argparse.Namespace) -> int:
    from engram.decay import run_daily
    state = build_state()
    updated = run_daily(state.neo4j, state.cfg.decay)
    print(json.dumps({"updated_nodes": updated}, indent=2))
    return 0


def cmd_migrate(_args: argparse.Namespace) -> int:
    from engram.config import get_config
    from engram.migrations.runner import run_pending
    summary = run_pending(get_config())
    print(json.dumps(summary, indent=2))
    return 0


def cmd_train(args: argparse.Namespace) -> int:
    """Orchestrate a training phase. `phase` is one of:
    gate | sft | dpo | synth (generate synthetic data).
    """
    from pathlib import Path
    if args.phase == "synth":
        from engram.training.synthetic_data import generate_all
        out = Path(args.out)
        summary = generate_all(out, seed=args.seed)
        print(json.dumps({"out": str(out), "generated": summary}, indent=2))
        return 0
    if args.phase == "gate":
        from engram.training.gate_classifier import train as train_gate
        train_gate(Path(args.data), Path(args.out), epochs=args.epochs, lr=args.lr)
        return 0
    if args.phase == "sft":
        from engram.training.core_sft import train as train_sft
        train_sft(Path(args.traces), args.base or "Qwen/Qwen3.5-0.8B",
                  Path(args.out), epochs=args.epochs, lr=args.lr)
        return 0
    if args.phase == "dpo":
        from engram.training.core_dpo import train as train_dpo
        train_dpo(Path(args.sft_model), Path(args.held_out), Path(args.out),
                  beta=args.beta, lr=args.lr, epochs=args.epochs)
        return 0
    raise SystemExit(f"unknown phase: {args.phase}")


def cmd_admin(args: argparse.Namespace) -> int:
    """Admin helpers for tenant management (CLI-local; doesn't need the API)."""
    from engram.config import get_config
    from engram.tenancy import TenantQuotas, TenantRegistry

    cfg = get_config()
    reg = TenantRegistry(cfg.event_ledger.path)

    if args.action == "create-tenant":
        quotas = TenantQuotas(
            requests_per_minute=args.rpm,
            ingest_per_minute=args.ipm,
        )
        tenant, api_key = reg.create(
            args.tenant_id, display_name=args.display_name, quotas=quotas,
        )
        print(json.dumps({
            "tenant_id": tenant.tenant_id,
            "display_name": tenant.display_name,
            "api_key": api_key,          # shown exactly once
            "note": "Store the api_key now; future lookups match by hash only.",
        }, indent=2))
        return 0
    if args.action == "list-tenants":
        rows = [{
            "tenant_id": t.tenant_id,
            "display_name": t.display_name,
            "status": t.status,
            "api_key_count": len(t.api_key_hashes),
            "quotas": {
                "requests_per_minute": t.quotas.requests_per_minute,
                "ingest_per_minute": t.quotas.ingest_per_minute,
            },
        } for t in reg.list()]
        print(json.dumps(rows, indent=2))
        return 0
    if args.action == "mint-key":
        api_key = reg.issue_key(args.tenant_id)
        print(json.dumps({"tenant_id": args.tenant_id, "api_key": api_key}, indent=2))
        return 0
    if args.action == "suspend":
        reg.update_status(args.tenant_id, "SUSPENDED")
        print(f"{args.tenant_id}: SUSPENDED")
        return 0
    if args.action == "resume":
        reg.update_status(args.tenant_id, "ACTIVE")
        print(f"{args.tenant_id}: ACTIVE")
        return 0
    raise SystemExit(f"unknown action: {args.action}")


def cmd_smoke(_args: argparse.Namespace) -> int:
    state = build_state()
    ingest_ctx = make_ingest_context(state)
    session_id = f"smoke-{int(time.time())}"
    print(f"[smoke] session: {session_id}")
    for idx, turn in enumerate(_SMOKE_CORPUS):
        user_idx = idx * 2
        asst_idx = user_idx + 1
        pid = pair_id_fn(session_id, user_idx, asst_idx)
        event_id, is_new = state.sqlite.record_event(
            pair_id=pid,
            session_id=session_id,
            source="smoke",
            event_type="INGEST",
            payload={
                "session_id": session_id,
                "turn_pair": {
                    "user": {"content": turn["user"], "turn_idx": user_idx},
                    "assistant": {"content": turn["assistant"], "turn_idx": asst_idx},
                },
            },
        )
        print(f"  pair {idx}: event={event_id} new={is_new}")
        final = process_event(ingest_ctx, event_id)
        print(f"     → {final}")

    print("\n[smoke] queries:")
    orch_ctx = make_orchestrator_context(state)
    for q in [
        "Where does the user work now?",
        "When is the user's wife's birthday?",
        "Where is the user moving?",
    ]:
        print(f"\nQ: {q}")
        result = run_query(orch_ctx, session_id=None, query=q)
        print(f"A: {result.answer}")
        print(f"   metadata: {json.dumps(result.retrieval_metadata.to_dict(), indent=2)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="engram")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", help="Create Neo4j indexes and SQLite schemas")
    sub.add_parser("health", help="Print component readiness")
    sub.add_parser("smoke", help="Run an end-to-end ingest + query test")
    sub.add_parser("rebuild-kg", help="Rebuild Neo4j from the filesystem (§2.3)")
    sub.add_parser("decay", help="Run the daily decay pass (§10.3)")
    sub.add_parser("migrate", help="Run pending schema migrations (§13.5)")

    # engram train <phase>
    train_p = sub.add_parser("train", help="Training pipeline (§14)")
    train_sub = train_p.add_subparsers(dest="phase", required=True)
    p_synth = train_sub.add_parser("synth", help="Generate synthetic training data")
    p_synth.add_argument("--out", required=True)
    p_synth.add_argument("--seed", type=int, default=0)
    p_gate = train_sub.add_parser("gate", help="Train the L0 gate classifier")
    p_gate.add_argument("--data", required=True)
    p_gate.add_argument("--out", required=True)
    p_gate.add_argument("--epochs", type=int, default=5)
    p_gate.add_argument("--lr", type=float, default=2e-5)
    p_sft = train_sub.add_parser("sft", help="LoRA fine-tune the Core Model")
    p_sft.add_argument("--traces", required=True, help="JSONL file or directory")
    p_sft.add_argument("--base", default="Qwen/Qwen3.5-0.8B")
    p_sft.add_argument("--out", required=True)
    p_sft.add_argument("--epochs", type=int, default=3)
    p_sft.add_argument("--lr", type=float, default=1e-4)
    p_dpo = train_sub.add_parser("dpo", help="DPO over winner/loser pairs")
    p_dpo.add_argument("--sft-model", required=True)
    p_dpo.add_argument("--held-out", required=True)
    p_dpo.add_argument("--out", required=True)
    p_dpo.add_argument("--beta", type=float, default=0.1)
    p_dpo.add_argument("--lr", type=float, default=5e-6)
    p_dpo.add_argument("--epochs", type=int, default=1)

    # engram admin <action>
    admin_p = sub.add_parser("admin", help="Tenant administration (local)")
    admin_sub = admin_p.add_subparsers(dest="action", required=True)
    a_create = admin_sub.add_parser("create-tenant")
    a_create.add_argument("tenant_id")
    a_create.add_argument("--display-name", default="")
    a_create.add_argument("--rpm", type=int, default=120)
    a_create.add_argument("--ipm", type=int, default=600)
    admin_sub.add_parser("list-tenants")
    a_mint = admin_sub.add_parser("mint-key")
    a_mint.add_argument("tenant_id")
    a_suspend = admin_sub.add_parser("suspend")
    a_suspend.add_argument("tenant_id")
    a_resume = admin_sub.add_parser("resume")
    a_resume.add_argument("tenant_id")

    args = parser.parse_args(argv)
    return {
        "init": cmd_init,
        "health": cmd_health,
        "smoke": cmd_smoke,
        "rebuild-kg": cmd_rebuild_kg,
        "decay": cmd_decay,
        "migrate": cmd_migrate,
        "train": cmd_train,
        "admin": cmd_admin,
    }[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
