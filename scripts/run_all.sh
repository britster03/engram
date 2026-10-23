#!/usr/bin/env bash
# Start the complete Engram production-style stack on one host.
#
# Usage:
#   ./scripts/run_all.sh
#   ENGRAM_ENV_FILE=.env.prod ./scripts/run_all.sh
#   ./scripts/run_all.sh --no-build
#   ./scripts/run_all.sh --full
#
# The wrapper never edits the source environment file.  It creates a private
# temporary copy only to supply a default immutable image tag when one is not
# present, then delegates health checks and service startup to deploy.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    exec "$SCRIPT_DIR/deploy.sh" --help
fi

ENV_FILE="${ENGRAM_ENV_FILE:-}"
DEPLOY_ARGS=()
while (($# > 0)); do
    case "$1" in
        --env-file)
            (($# >= 2)) || {
                echo "ERROR: --env-file requires a path" >&2
                exit 2
            }
            ENV_FILE="$2"
            shift 2
            ;;
        *)
            DEPLOY_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -z "$ENV_FILE" ]]; then
    if [[ -f "$ROOT_DIR/.env" ]]; then
        ENV_FILE="$ROOT_DIR/.env"
    else
        ENV_FILE="$ROOT_DIR/.env.prod"
    fi
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: environment file not found: $ENV_FILE" >&2
    echo "Create .env or pass --env-file /path/to/.env.prod." >&2
    exit 1
fi

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/engram-run.XXXXXX")"
TEMP_ENV="$TEMP_DIR/.env"
cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

cp -- "$ENV_FILE" "$TEMP_ENV"
chmod 600 "$TEMP_ENV"
if ! grep -Eq '^ENGRAM_IMAGE_TAG=.+$' "$TEMP_ENV"; then
    {
        printf '\n# Added by scripts/run_all.sh; override ENGRAM_IMAGE_TAG to pin a release.\n'
        printf 'ENGRAM_IMAGE_TAG=%s\n' "${ENGRAM_IMAGE_TAG:-0.1.0}"
    } >> "$TEMP_ENV"
fi

cd "$ROOT_DIR"
exec "$SCRIPT_DIR/deploy.sh" --env-file "$TEMP_ENV" "${DEPLOY_ARGS[@]}"
