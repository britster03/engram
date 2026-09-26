#!/usr/bin/env bash
# quickstart.sh — repeatable local Engram bootstrap
# Usage: ./scripts/quickstart.sh
#
# The API binds to every network interface by default, so the Admin UI can be
# opened from another device at http://<this-machine-ip>:8000/admin/login.
# Set ENGRAM_API_HOST=127.0.0.1 before running this script to restrict access
# to this machine only.
#
# Safe restart behaviour:
#   - gracefully stops a Uvicorn instance for this app before starting a new one;
#   - stops the production Compose `engram` service when it is running;
#   - never kills an unknown process that happens to be listening on port 8000.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RUNTIME_DIR="$ROOT_DIR/data"
API_PID_FILE="$RUNTIME_DIR/engram-api.pid"
API_LOG_FILE="$RUNTIME_DIR/engram-api.log"
# Bind externally by default for LAN access. The local health check below
# deliberately remains on loopback, which works for both bind choices.
ENGRAM_API_HOST="${ENGRAM_API_HOST:-0.0.0.0}"
ENGRAM_API_PORT="${ENGRAM_API_PORT:-8000}"
# Loading the embedding model can take longer than the old 10-second probe
# window, especially on a cold start. This may be overridden when needed.
ENGRAM_API_STARTUP_TIMEOUT_SECONDS="${ENGRAM_API_STARTUP_TIMEOUT_SECONDS:-60}"

process_has_stopped() {
    local pid="$1"
    local process_state

    # `kill -0` succeeds for a zombie process. It cannot serve requests or
    # hold port 8000, so treat it as stopped and let its parent reap it.
    process_state="$(ps -p "$pid" -o stat= 2>/dev/null | tr -d '[:space:]' || true)"
    [ -z "$process_state" ] || [[ "$process_state" == Z* ]]
}

stop_api_process() {
    local pid="$1"
    local command
    command="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [ -z "$command" ]; then
        return
    fi
    case "$command" in
        *"uvicorn engram.api.app:app"*) ;;
        *)
            echo "  Refusing to stop PID $pid because it is not an Engram Uvicorn process."
            return
            ;;
    esac

    echo "  Stopping existing Engram API process (PID $pid)..."
    kill -TERM "$pid"
    for _ in $(seq 1 20); do
        if process_has_stopped "$pid"; then
            return
        fi
        sleep 0.5
    done

    # The process was verified above as this application's Uvicorn command;
    # do not let a slow graceful shutdown prevent a new local quickstart.
    echo "  API process did not stop gracefully; forcing shutdown..."
    kill -KILL "$pid" 2>/dev/null || true
    for _ in $(seq 1 10); do
        if process_has_stopped "$pid"; then
            return
        fi
        sleep 0.5
    done
    echo "ERROR: Engram API process $pid is still running after forced shutdown." >&2
    exit 1
}

stop_existing_api() {
    mkdir -p "$RUNTIME_DIR"
    if [ -f "$API_PID_FILE" ]; then
        local saved_pid
        saved_pid="$(tr -d '[:space:]' < "$API_PID_FILE")"
        if [[ "$saved_pid" =~ ^[0-9]+$ ]]; then
            stop_api_process "$saved_pid"
        fi
        rm -f "$API_PID_FILE"
    fi

    # Covers API processes started by older versions of this script, which did
    # not write a PID file. The command check inside stop_api_process prevents
    # us from touching unrelated Uvicorn applications.
    local running_pid
    while IFS= read -r running_pid; do
        [ -n "$running_pid" ] && stop_api_process "$running_pid"
    done < <(pgrep -f 'uvicorn engram\.api\.app:app' 2>/dev/null || true)
}

stop_production_compose_service() {
    # A production Compose app binds port 8000, so stop only its named service
    # before launching the local development API. This is deliberately scoped:
    # Neo4j and Redis keep running and are reused below.
    local container_running
    container_running="$(docker inspect -f '{{.State.Running}}' engram-app 2>/dev/null || true)"
    if [ "$container_running" = "true" ]; then
        echo "  Stopping existing production container engram-app..."
        docker stop --time 30 engram-app
        return
    fi

    if [ ! -f "$ROOT_DIR/.env.prod" ]; then
        return
    fi
    local running
    running="$(docker compose -f docker-compose.yml -f docker-compose.prod.yml \
        ps -q engram 2>/dev/null || true)"
    if [ -n "$running" ]; then
        echo "  Stopping existing production Compose engram service..."
        docker compose -f docker-compose.yml -f docker-compose.prod.yml stop engram
    fi
}

wait_for_service_health() {
    local service="$1"
    local timeout_seconds="${2:-120}"
    local container_id status
    local elapsed=0

    container_id="$(docker compose ps -q "$service" 2>/dev/null || true)"
    if [ -z "$container_id" ]; then
        echo "ERROR: Docker Compose did not create the $service service." >&2
        return 1
    fi

    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
        case "$status" in
            healthy)
                echo "  $service is healthy."
                return 0
                ;;
            unhealthy)
                echo "ERROR: $service became unhealthy. Check: docker compose logs $service" >&2
                return 1
                ;;
        esac
        sleep 2
        elapsed=$((elapsed + 2))
    done

    echo "ERROR: $service did not become healthy within ${timeout_seconds}s (status: ${status:-unknown})." >&2
    echo "       Check: docker compose logs $service" >&2
    return 1
}

echo "=== Engram Quickstart ==="
cd "$ROOT_DIR"

echo "[0] Stopping an existing Engram API, if present..."
stop_existing_api
stop_production_compose_service

# Load .env if it exists (allows manual editing of secrets)
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

# 1. Python venv
if [ ! -d ".venv" ]; then
    echo "[1] Creating Python venv..."
    python3 -m venv .venv
fi
echo "[1] Activating venv..."
source .venv/bin/activate

# 2. Install dependencies
echo "[2] Installing dependencies..."
pip install -e '.[dev]' -q

# 3. Provider + secrets — Muse Spark through OpenCode Go is the only runtime.
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

export ENGRAM_CONFIG_PATH="$ROOT_DIR/config.opencode-go.yaml"
PROVIDER_LABEL="Muse Spark through OpenCode Go"

echo "[3] Using provider: $PROVIDER_LABEL"
echo "[3] Setting secrets..."
env_check "ENGRAM_API_KEY" "ENGRAM_API_KEY" "engram-dev-$(openssl rand -hex 16)"
env_check "OPENCODE_GO_API_KEY" "OPENCODE_GO_API_KEY"
env_check "NEO4J_ADMIN_PASSWORD" "Neo4j admin password" "engram-dev"
env_check "ENGRAM_DB_PASSWORD" "PostgreSQL application password"
env_check "ENGRAM_DATABASE_URL" "PostgreSQL DSN (postgresql://user:password@host:port/database)"

# 4. Write .env if missing
if [ ! -f ".env" ]; then
    echo "[4] Writing .env..."
    cat > .env <<EOF
ENGRAM_API_KEY=${ENGRAM_API_KEY}
${PROVIDER_LABEL:+# Provider selected by quickstart: ${PROVIDER_LABEL}}
OPENCODE_GO_API_KEY=${OPENCODE_GO_API_KEY}
NEO4J_ADMIN_PASSWORD=${NEO4J_ADMIN_PASSWORD}
ENGRAM_DB_PASSWORD=${ENGRAM_DB_PASSWORD}
ENGRAM_DATABASE_URL=${ENGRAM_DATABASE_URL}
ENGRAM_ADMIN_KEY=${ENGRAM_API_KEY}
ENGRAM_CONFIG_PATH=${ENGRAM_CONFIG_PATH}
EOF
else
    # An existing .env may contain only provider credentials. Persist the
    # generated local API/admin key when it is absent; otherwise the server
    # starts once but cannot be restarted from the same configuration.
    if ! grep -q '^ENGRAM_API_KEY=.\+' .env; then
        if grep -q '^ENGRAM_API_KEY=' .env; then
            sed -i "s|^ENGRAM_API_KEY=.*|ENGRAM_API_KEY=${ENGRAM_API_KEY}|" .env
        else
            printf '\n# Local API key generated by quickstart\nENGRAM_API_KEY=%s\n' "$ENGRAM_API_KEY" >> .env
        fi
        echo "[4] Added missing ENGRAM_API_KEY to .env"
    fi
    if ! grep -q '^ENGRAM_ADMIN_KEY=.\+' .env; then
        if grep -q '^ENGRAM_ADMIN_KEY=' .env; then
            sed -i "s|^ENGRAM_ADMIN_KEY=.*|ENGRAM_ADMIN_KEY=${ENGRAM_API_KEY}|" .env
        else
            printf 'ENGRAM_ADMIN_KEY=%s\n' "$ENGRAM_API_KEY" >> .env
        fi
        echo "[4] Added missing ENGRAM_ADMIN_KEY to .env"
    fi
    if ! grep -q '^ENGRAM_DATABASE_URL=.\+' .env; then
        printf 'ENGRAM_DATABASE_URL=%s\n' "$ENGRAM_DATABASE_URL" >> .env
        echo "[4] Added missing ENGRAM_DATABASE_URL to .env"
    fi
    if ! grep -q '^ENGRAM_DB_PASSWORD=.\+' .env; then
        printf 'ENGRAM_DB_PASSWORD=%s\n' "$ENGRAM_DB_PASSWORD" >> .env
        echo "[4] Added missing ENGRAM_DB_PASSWORD to .env"
    fi
    if ! grep -q '^OPENCODE_GO_API_KEY=.\+' .env; then
        if grep -q '^OPENCODE_GO_API_KEY=' .env; then
            sed -i "s|^OPENCODE_GO_API_KEY=.*|OPENCODE_GO_API_KEY=${OPENCODE_GO_API_KEY}|" .env
        else
            printf 'OPENCODE_GO_API_KEY=%s\n' "$OPENCODE_GO_API_KEY" >> .env
        fi
        echo "[4] Added missing OPENCODE_GO_API_KEY to .env"
    fi
    echo "[4] .env already exists — retained existing settings"
fi

# 5. Backing services. PostgreSQL is external to the lightweight local
# Compose file and is validated by the migration command in the next step.
echo "[5] Starting Neo4j and Redis..."
docker compose up -d
echo "  Waiting for services..."
wait_for_service_health neo4j
wait_for_service_health redis

# 6. Init
echo "[6] Initializing schemas..."
python -m engram.cli migrate
python -m engram.cli init

# 7. Start API server in background and retain its PID for the next rerun.
echo "[7] Starting API server..."
nohup uvicorn engram.api.app:app --host "$ENGRAM_API_HOST" --port "$ENGRAM_API_PORT" \
    > "$API_LOG_FILE" 2>&1 &
API_PID=$!
echo "$API_PID" > "$API_PID_FILE"
API_READY=false
for _ in $(seq 1 "$ENGRAM_API_STARTUP_TIMEOUT_SECONDS"); do
    if curl -fsS "http://127.0.0.1:${ENGRAM_API_PORT}/livez" >/dev/null 2>&1; then
        echo "  API server running on http://${ENGRAM_API_HOST}:${ENGRAM_API_PORT} (PID $API_PID)"
        API_READY=true
        break
    fi
    if ! kill -0 "$API_PID" 2>/dev/null; then
        rm -f "$API_PID_FILE"
        echo "ERROR: API server failed to start. Check $API_LOG_FILE" >&2
        exit 1
    fi
    sleep 0.5
done

if [ "$API_READY" != true ]; then
    rm -f "$API_PID_FILE"
    echo "ERROR: API server did not become healthy within ${ENGRAM_API_STARTUP_TIMEOUT_SECONDS}s. Check $API_LOG_FILE" >&2
    exit 1
fi

echo ""
echo "=== Done ==="
echo "  Local Admin UI: http://localhost:${ENGRAM_API_PORT}/admin/dashboard"
if [ "$ENGRAM_API_HOST" = "0.0.0.0" ]; then
    echo "  Network Admin UI: http://<this-machine-ip>:${ENGRAM_API_PORT}/admin/login"
fi
echo "  Chat UI:      http://localhost:${ENGRAM_API_PORT}/admin/chat"
echo "  API docs:     http://localhost:${ENGRAM_API_PORT}/docs"
echo "  Smoke test:   python -m engram.cli smoke  (optional)"
echo ""
echo "  API log:            $API_LOG_FILE"
echo "  To stop the server: kill $API_PID"
