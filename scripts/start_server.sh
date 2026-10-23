#!/usr/bin/env bash
# Start all canonical development processes, including dispatcher and workers.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/quickstart.sh" "$@"
