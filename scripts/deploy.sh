#!/usr/bin/env bash
# Deploy the single-host Engram + Temporal production stack with Docker Compose.
#
# Default: API, PostgreSQL, Neo4j, Redis, Temporal, Temporal UI, workers,
# dispatcher, and Temporal-scheduled maintenance. Nginx/Prometheus/Grafana are opt-in.
#
# Examples:
#   ./scripts/deploy.sh
#   ./scripts/deploy.sh --env-file .env --no-build
#   ./scripts/deploy.sh --with-nginx
#   ./scripts/deploy.sh --full

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${ENGRAM_ENV_FILE:-.env.prod}"
BUILD_IMAGE=true
FULL_STACK=false
WITH_NGINX=false

usage() {
    cat <<'EOF'
Usage: ./scripts/deploy.sh [options]

Options:
  --env-file PATH  Environment file to use (default: .env.prod or ENGRAM_ENV_FILE)
  --no-build       Reuse the existing engram:latest image
  --with-nginx     Also start the HTTPS Nginx edge proxy (requires TLS files)
  --full           Start Nginx, Prometheus, and Grafana
  -h, --help       Show this help

The default deployment starts the complete application and Temporal stack,
but intentionally excludes Nginx, Prometheus, and Grafana. Nginx requires
deploy/tls/fullchain.pem and deploy/tls/privkey.pem.
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --env-file)
            [ "$#" -ge 2 ] || { echo "--env-file requires a path" >&2; exit 2; }
            ENV_FILE="$2"
            shift 2
            ;;
        --no-build) BUILD_IMAGE=false; shift ;;
        --with-nginx) WITH_NGINX=true; shift ;;
        --full) FULL_STACK=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

cd "$ROOT_DIR"
[ -f "$ENV_FILE" ] || { echo "ERROR: environment file not found: $ENV_FILE" >&2; exit 1; }
# `--env-file` supplies interpolation values but does not export this selector
# for Compose's service-level `env_file: ["${ENGRAM_ENV_FILE:-.env.prod}"]`.
export ENGRAM_ENV_FILE="$ENV_FILE"

COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file "$ENV_FILE")
CORE_SERVICES=(
    postgres neo4j redis
    temporal temporal-ui temporal-namespace
    engram-control-plane-migrate engram-neo4j-init
    engram engram-temporal-worker engram-dispatcher
)

if [ "$FULL_STACK" = true ]; then
    WITH_NGINX=true
    CORE_SERVICES+=(nginx prometheus grafana)
elif [ "$WITH_NGINX" = true ]; then
    CORE_SERVICES+=(nginx)
fi

if [ "$WITH_NGINX" = true ]; then
    for certificate in deploy/tls/fullchain.pem deploy/tls/privkey.pem; do
        [ -f "$certificate" ] || {
            echo "ERROR: Nginx requires $certificate. Add a trusted TLS certificate before using --with-nginx." >&2
            exit 1
        }
    done
fi

wait_for_health() {
    local service="$1"
    local timeout_seconds="$2"
    local elapsed=0 container_id state

    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        container_id="$("${COMPOSE[@]}" ps -q "$service" 2>/dev/null || true)"
        if [ -n "$container_id" ]; then
            state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
            case "$state" in
                healthy) echo "  $service: healthy"; return 0 ;;
                unhealthy)
                    echo "ERROR: $service is unhealthy. Inspect with: ${COMPOSE[*]} logs $service" >&2
                    return 1
                    ;;
            esac
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "ERROR: $service did not become healthy within ${timeout_seconds}s." >&2
    return 1
}

wait_for_running() {
    local service="$1"
    local timeout_seconds="$2"
    local elapsed=0 container_id state

    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        container_id="$("${COMPOSE[@]}" ps -q "$service" 2>/dev/null || true)"
        if [ -n "$container_id" ]; then
            state="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
            [ "$state" = "running" ] && { echo "  $service: running"; return 0; }
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "ERROR: $service did not start within ${timeout_seconds}s." >&2
    return 1
}

wait_for_completed() {
    local service="$1"
    local timeout_seconds="$2"
    local elapsed=0 container_id state exit_code

    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        container_id="$("${COMPOSE[@]}" ps -aq "$service" 2>/dev/null || true)"
        if [ -n "$container_id" ]; then
            state="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
            if [ "$state" = "exited" ]; then
                exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$container_id")"
                if [ "$exit_code" = "0" ]; then
                    echo "  $service: completed"
                    return 0
                fi
                echo "ERROR: $service exited with code $exit_code. Inspect with: ${COMPOSE[*]} logs $service" >&2
                return 1
            fi
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "ERROR: $service did not complete within ${timeout_seconds}s." >&2
    return 1
}

echo "=== Engram Docker Compose deployment ==="
echo "[1/6] Running preflight checks with $ENV_FILE..."
python3 scripts/temporal_preflight.py --env-file "$ENV_FILE"
"${COMPOSE[@]}" config --quiet

if [ "$BUILD_IMAGE" = true ]; then
    echo "[2/6] Building the Engram image..."
    "${COMPOSE[@]}" build engram
else
    echo "[2/6] Reusing the existing Engram image."
fi

echo "[3/6] Starting services..."
"${COMPOSE[@]}" up -d "${CORE_SERVICES[@]}"

echo "[4/6] Verifying one-shot schema and namespace jobs..."
for service in temporal-schema temporal-namespace engram-control-plane-migrate engram-neo4j-init; do
    wait_for_completed "$service" 180
done

echo "[5/6] Waiting for service health..."
for service in postgres neo4j redis temporal engram engram-temporal-worker engram-dispatcher; do
    wait_for_health "$service" 180
done
wait_for_running temporal-ui 60

echo "[6/6] Verifying Temporal UI and API readiness..."
ui_id="$("${COMPOSE[@]}" ps -q temporal-ui)"
docker exec "$ui_id" wget -q -O /dev/null http://127.0.0.1:8080/
api_id="$("${COMPOSE[@]}" ps -q engram)"
docker exec "$api_id" curl -fsS http://127.0.0.1:8000/readyz >/dev/null

host_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
echo
echo "Deployment complete."
echo "  API health:   healthy"
echo "  Temporal UI:  http://${host_ip:-127.0.0.1}:8080/namespaces/engram-prod/workflows"
echo "  Status:       ${COMPOSE[*]} ps"
