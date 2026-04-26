#!/usr/bin/env bash
# quickstart.sh — Engram dev environment setup
# Usage: ./scripts/quickstart.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Engram Quickstart ==="
cd "$ROOT_DIR"

# 1. Python venv
if [ ! -d ".venv" ]; then
    echo "[1] Creating Python venv..."
    python3.10 -m venv .venv
fi
echo "[1] Activating venv..."
source .venv/bin/activate

# 2. Install dependencies
echo "[2] Installing dependencies..."
pip install -e '.[dev]' -q

# 3. Secrets — prompt for any not set
env_check() {
    local var="$1"; local label="$2"; local default="${3:-}"
    if [ -z "${!var:-}" ]; then
        if [ -n "$default" ]; then
            export "$var"="$default"
            echo "  → $var set to default"
        else
            read -rp "  Enter $label: " val
            export "$var"="$val"
        fi
    fi
}

echo "[3] Setting secrets..."
env_check "ENGRAM_API_KEY" "ENGRAM_API_KEY" "engram-dev-$(openssl rand -hex 16)"
env_check "CORE_MODEL_API_KEY" "CORE_MODEL_API_KEY"
env_check "FRONTIER_LLM_API_KEY" "FRONTIER_LLM_API_KEY"
env_check "NEO4J_ADMIN_PASSWORD" "Neo4j admin password" "engram-dev"

# 4. Write .env if missing
if [ ! -f ".env" ]; then
    echo "[4] Writing .env..."
    cat > .env <<EOF
ENGRAM_API_KEY=${ENGRAM_API_KEY}
CORE_MODEL_API_KEY=${CORE_MODEL_API_KEY}
FRONTIER_LLM_API_KEY=${FRONTIER_LLM_API_KEY}
NEO4J_ADMIN_PASSWORD=${NEO4J_ADMIN_PASSWORD}
ANTHROPIC_BASE_URL=https://api.minimax.io/anthropic
ENGRAM_ADMIN_KEY=${ENGRAM_API_KEY}
EOF
else
    echo "[4] .env already exists — skipping"
fi

# 5. Backing services
echo "[5] Starting Docker services..."
docker compose up -d
echo "  Waiting for services..."
sleep 5

# 6. Init
echo "[6] Initializing schemas..."
python -m engram.cli migrate
python -m engram.cli init

echo ""
echo "=== Done ==="
echo "  API server:   uvicorn engram.api.app:app --port 8000"
echo "  Admin UI:     http://localhost:8000/admin/dashboard"
echo "  Chat UI:      http://localhost:8000/admin/chat"
echo "  Smoke test:   python -m engram.cli smoke"
