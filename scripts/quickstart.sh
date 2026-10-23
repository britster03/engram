#!/usr/bin/env bash
# Start the canonical development stack. Production uses scripts/deploy.sh.
# Existing .env credentials and production processes are left intact.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
ENV_FILE="${ENGRAM_ENV_FILE:-$ROOT_DIR/.env}"
RUNTIME_DIR="$ROOT_DIR/data/dev-runtime"
PY="$ROOT_DIR/.venv/bin/python"
mkdir -p "$RUNTIME_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Create $ENV_FILE from .env.example and fill in the credentials first." >&2
    exit 1
fi
set -a
source "$ENV_FILE"
set +a
for name in ENGRAM_API_KEY OPENCODE_GO_API_KEY NEO4J_ADMIN_PASSWORD ENGRAM_DB_PASSWORD; do
    if [[ -z "${!name:-}" ]]; then
        echo "Missing $name in $ENV_FILE" >&2
        exit 1
    fi
done

if [[ ! -x "$PY" ]]; then
    python3 -m venv "$ROOT_DIR/.venv"
fi
if [[ "${ENGRAM_SKIP_INSTALL:-0}" != 1 ]]; then
    "$PY" -m pip install -e '.[dev]'
fi

# A local development database is selected explicitly, never the production
# ENGRAM_DATABASE_URL that may be present in an existing environment file.
export ENGRAM_CONFIG_PATH="$ROOT_DIR/config.yaml"
export ENGRAM_DEV_POSTGRES_PORT="${ENGRAM_DEV_POSTGRES_PORT:-5433}"
export ENGRAM_DATABASE_URL="${ENGRAM_DEV_DATABASE_URL:-$("$PY" - <<'PY'
import os
from urllib.parse import quote
print('postgresql://engram_app:' + quote(os.environ['ENGRAM_DB_PASSWORD'], safe='')
      + '@127.0.0.1:' + os.environ['ENGRAM_DEV_POSTGRES_PORT'] + '/engram')
PY
)}"
export ENGRAM_DEV_DATABASE_URL="$ENGRAM_DATABASE_URL"
COMPOSE=(docker compose --project-name engram-dev --env-file "$ENV_FILE" -f "$ROOT_DIR/docker-compose.yml")

stop_owned_process() {
    local file="$1" pid command state
    [[ -f "$file" ]] || return 0
    read -r pid < "$file"
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || { echo "Invalid PID file: $file" >&2; return 1; }
    command="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [[ -n "$command" ]]; then
        case "$command" in
            "$PY -m engram.temporal.worker"*|"$PY -m engram.temporal.dispatcher"*|"$PY -m uvicorn engram.api.app:app"*) ;;
            *) echo "Refusing to stop unrelated PID $pid from $file" >&2; return 1 ;;
        esac
        kill -TERM "$pid"
        for _ in $(seq 1 60); do
            state="$(ps -p "$pid" -o stat= 2>/dev/null | tr -d ' ' || true)"
            [[ -z "$state" || "$state" == Z* ]] && break
            sleep 0.5
        done
        state="$(ps -p "$pid" -o stat= 2>/dev/null | tr -d ' ' || true)"
        if [[ -n "$state" && "$state" != Z* ]]; then
            echo "PID $pid has not stopped; retry after its current activity finishes." >&2
            return 1
        fi
    fi
    rm -f "$file"
}
for service in api dispatcher worker; do
    stop_owned_process "$RUNTIME_DIR/$service.pid"
done

"${COMPOSE[@]}" up -d
for service in postgres neo4j redis temporal; do
    container_id="$("${COMPOSE[@]}" ps -q "$service")"
    healthy=false
    for _ in $(seq 1 90); do
        status="$(docker inspect -f '{{.State.Health.Status}}' "$container_id")"
        [[ "$status" == healthy ]] && { healthy=true; break; }
        [[ "$status" == unhealthy ]] && break
        sleep 2
    done
    if [[ "$healthy" != true ]]; then
        echo "$service is not healthy; inspect the engram-dev Compose logs." >&2
        exit 1
    fi
done
"${COMPOSE[@]}" wait temporal-namespace
"$PY" -m engram.cli migrate
"$PY" -m engram.cli init

STARTED=()
cleanup_failed_start() {
    if [[ "${STARTUP_COMPLETE:-false}" != true ]]; then
        for service in "${STARTED[@]}"; do stop_owned_process "$RUNTIME_DIR/$service.pid"; done
    fi
}
trap cleanup_failed_start EXIT
start_process() {
    local name="$1"; shift
    nohup "$@" > "$RUNTIME_DIR/$name.log" 2>&1 &
    echo "$!" > "$RUNTIME_DIR/$name.pid"
    STARTED+=("$name")
}
start_process worker "$PY" -m engram.temporal.worker --queue all
start_process dispatcher "$PY" -m engram.temporal.dispatcher
start_process api "$PY" -m uvicorn engram.api.app:app \
    --host "${ENGRAM_API_HOST:-127.0.0.1}" --port "${ENGRAM_API_PORT:-8000}"
for _ in $(seq 1 "${ENGRAM_API_STARTUP_TIMEOUT_SECONDS:-120}"); do
    for service in "${STARTED[@]}"; do
        read -r pid < "$RUNTIME_DIR/$service.pid"
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "$service exited; inspect $RUNTIME_DIR/$service.log" >&2
            exit 1
        fi
    done
    if curl -fsS "http://127.0.0.1:${ENGRAM_API_PORT:-8000}/readyz" >/dev/null 2>&1; then
        STARTUP_COMPLETE=true
        break
    fi
    sleep 1
done
[[ "${STARTUP_COMPLETE:-false}" == true ]] || { echo "Development API did not become ready." >&2; exit 1; }
echo "Canonical development stack ready: http://127.0.0.1:${ENGRAM_API_PORT:-8000}/admin/login"
echo "Temporal UI: http://127.0.0.1:8080/namespaces/engram-dev/workflows"
echo "Process PID files and logs: $RUNTIME_DIR"
