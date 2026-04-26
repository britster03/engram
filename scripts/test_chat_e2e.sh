#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${ENGRAM_BASE_URL:-http://localhost:8000}"

if [ -z "${ENGRAM_API_KEY:-}" ]; then
    ENV_KEY=""
    if [ -f .env ]; then
        ENV_KEY=$(grep "^ENGRAM_API_KEY=" .env | cut -d= -f2-)
    fi
    if [ -z "$ENV_KEY" ]; then
        AUTH_KEY="test-key"
    else
        AUTH_KEY="$ENV_KEY"
    fi
else
    AUTH_KEY="$ENGRAM_API_KEY"
fi

AUTH_HEADER="Authorization: Bearer ${AUTH_KEY}"

json_field() {
    python3 -c "import sys,json; print(json.load(sys.stdin).get('$1',''))" 
}

if ! curl -s --fail "${BASE_URL}/livez" > /dev/null 2>&1; then
    echo "WARNING: Backend not reachable at ${BASE_URL}. Skipping e2e chat tests."
    exit 0
fi

echo "=== E2E Chat Completions Smoke Tests ==="
echo "Base URL: ${BASE_URL}"

SESSION_RESP=$(curl -s --fail -X POST "${BASE_URL}/api/v1/sessions" -H "${AUTH_HEADER}")
SESSION_ID=$(echo "${SESSION_RESP}" | json_field session_id)
if [ -z "${SESSION_ID}" ]; then
    echo "ERROR: Failed to create session. Response: ${SESSION_RESP}"
    exit 1
fi

CHAT_BODY='{"messages":[{"role":"user","content":"Hello, what can you do?"}],"session_id":"'"${SESSION_ID}"'","stream":false}'
CHAT_RESP=$(curl -s --fail-with-body -X POST "${BASE_URL}/api/v1/chat/completions" \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -d "${CHAT_BODY}")

ANSWER=$(echo "${CHAT_RESP}" | json_field answer)
RESP_SESSION=$(echo "${CHAT_RESP}" | json_field session_id)
if [ -z "${ANSWER}" ]; then
    echo "ERROR: Non-streaming answer is empty. Response: ${CHAT_RESP}"
    exit 1
fi
if [ "${RESP_SESSION}" != "${SESSION_ID}" ]; then
    echo "ERROR: Session ID mismatch. Expected ${SESSION_ID}, got ${RESP_SESSION}"
    exit 1
fi
echo "Non-streaming OK (answer length: ${#ANSWER})"

STREAM_BODY='{"messages":[{"role":"user","content":"Streaming test message"}],"session_id":"'"${SESSION_ID}"'","stream":true}'
STREAM_OUTPUT=$(curl -s -N --fail -X POST "${BASE_URL}/api/v1/chat/completions" \
    -H "${AUTH_HEADER}" \
    -H "Content-Type: application/json" \
    -H "Accept: text/event-stream" \
    -d "${STREAM_BODY}")

for event in metadata delta done; do
    if ! echo "${STREAM_OUTPUT}" | grep -q "event: ${event}"; then
        echo "ERROR: Missing SSE event 'event: ${event}'. Output was:"
        echo "${STREAM_OUTPUT}"
        exit 1
    fi
done
echo "Streaming OK (events: metadata, delta, done)"

SESSION_GET=$(curl -s --fail "${BASE_URL}/api/v1/sessions/${SESSION_ID}" -H "${AUTH_HEADER}")
TURN_COUNT=$(echo "${SESSION_GET}" | json_field turn_count)
if [ -z "${TURN_COUNT}" ] || [ "${TURN_COUNT}" -lt 2 ]; then
    echo "ERROR: Expected at least 2 turns. Response: ${SESSION_GET}"
    exit 1
fi
echo "Session turns OK (turn_count: ${TURN_COUNT})"

echo "=== All E2E chat smoke tests passed ==="
