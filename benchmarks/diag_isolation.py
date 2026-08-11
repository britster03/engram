"""Diagnostic: does a minted tenant key actually scope ingest to its tenant?

Confirms or refutes the finding that LoCoMo data landed in `_default` instead
of its per-conversation tenant. Talks to the server over HTTP (so it is
working-directory independent for the API calls) and then reads the event
ledger to see how each ingest was tagged.

It ingests TWO pairs:
  * one with a freshly-minted tenant key  -> should be tagged <that tenant>
  * one with the legacy/default key        -> should be tagged _default

Run from the folder the SERVER was started in (so the ledger path matches),
e.g. from Code\\engram:  python ..\\benchmarks\\diag_isolation.py
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

import httpx

BASE = os.environ.get("ENGRAM_BASE_URL", "http://127.0.0.1:8000")


def load_env_keys() -> dict[str, str]:
    """Pull keys from environment, falling back to a nearby .env file."""
    keys = {k: os.environ[k] for k in ("ENGRAM_ADMIN_KEY", "ENGRAM_API_KEY")
            if k in os.environ}
    if "ENGRAM_ADMIN_KEY" in keys and "ENGRAM_API_KEY" in keys:
        return keys
    for candidate in (".env", "../.env", "../../.env"):
        p = Path(candidate)
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                name, val = line.split("=", 1)
                keys.setdefault(name.strip(), val.strip().strip('"').strip("'"))
        break
    return keys


def find_ledger() -> Path | None:
    """Locate the event ledger the running server is writing to."""
    candidates = [
        Path("data/event_ledger.db"),
        Path("engram/data/event_ledger.db"),
        Path("../data/event_ledger.db"),
    ]
    existing = [c for c in candidates if c.exists()]
    if not existing:
        return None
    # The server writes to exactly one; pick the most recently modified.
    return max(existing, key=lambda c: c.stat().st_mtime)


def ingest_one(key: str, source: str) -> int:
    r = httpx.post(
        f"{BASE}/api/v1/ingest",
        headers={"Authorization": f"Bearer {key}"},
        json={
            "session_id": "diag",
            "turn_pair": {
                "user": {"content": "diag user", "turn_idx": 0},
                "assistant": {"content": "diag asst", "turn_idx": 1},
            },
            "source": source,
        },
    )
    return r.status_code


def tenant_for_source(ledger: Path, source: str) -> str | None:
    con = sqlite3.connect(f"file:{ledger}?mode=ro", uri=True)
    row = con.execute(
        "SELECT tenant_id FROM events WHERE source = ? ORDER BY rowid DESC LIMIT 1",
        (source,),
    ).fetchone()
    con.close()
    return row[0] if row else None


def main() -> int:
    keys = load_env_keys()
    admin = keys.get("ENGRAM_ADMIN_KEY")
    default_key = keys.get("ENGRAM_API_KEY")
    if not admin:
        print("ERROR: ENGRAM_ADMIN_KEY not found in env or .env")
        return 2

    ledger = find_ledger()
    if ledger is None:
        print("ERROR: could not find event_ledger.db near", Path.cwd())
        return 2
    print(f"using ledger: {ledger.resolve()}\n")

    # 1) mint a fresh tenant + key
    tid = "diag-" + time.strftime("%H%M%S")
    r = httpx.post(
        f"{BASE}/api/v1/admin/tenants",
        headers={"Authorization": f"Bearer {admin}"},
        json={"tenant_id": tid, "display_name": "diag"},
    )
    if r.status_code not in (200, 201):
        print(f"ERROR: create-tenant returned {r.status_code}: {r.text[:300]}")
        return 1
    minted = r.json()["api_key"]
    print(f"minted tenant: {tid}")
    print(f"minted key == default key? {minted == default_key}\n")

    # 2) ingest with minted key (unique source) and with the default key
    src_minted = f"diag-minted-{tid}"
    src_default = f"diag-default-{tid}"
    print("ingest with minted key ->", ingest_one(minted, src_minted))
    if default_key:
        print("ingest with default key ->", ingest_one(default_key, src_default))
    time.sleep(1.5)

    # 3) read back how each was tagged
    got_minted = tenant_for_source(ledger, src_minted)
    got_default = tenant_for_source(ledger, src_default) if default_key else "(skipped)"

    print("\n--- VERDICT ---")
    print(f"minted-key ingest : expected={tid:<14} got={got_minted}")
    print(f"default-key ingest: expected={'_default':<14} got={got_default}")
    print()
    if got_minted == tid:
        print("PASS: tenant isolation works on the server. If real runs still")
        print("      land in _default, the bug is in the harness key handling.")
    elif got_minted == "_default":
        print("FAIL: a valid minted key is being resolved to _default on the")
        print("      server side. Tenant context is not reaching ingest.")
    else:
        print(f"UNEXPECTED: got {got_minted!r} — investigate manually.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
