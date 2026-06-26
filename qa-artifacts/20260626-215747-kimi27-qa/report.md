# Engram QA Report - Kimi 2.7 Model Path Switch

## Summary

Result: PASS.

Engram now resolves both Core Model and Frontier LLM to `ollama_cloud` with model path `kimi-k2.7-code:cloud`. Unit, integration, selected e2e, browser bulk upload, API, live Ollama Cloud, admin UI, chat, ingest, and KG checks passed. No MiniMax/Anthropic API-key traces were found outside excluded git/vendor/QA artifact paths.

## Environment

- Repo: `/media/hp/2fef3f02-b459-408a-b4c5-574a152873b1/engram`
- Commit at report start: `886b802`
- Worktree: dirty before and during this task
- Runtime provider: `ollama_cloud`
- Runtime model path: `kimi-k2.7-code:cloud`
- Runtime dependencies: Redis from `docker compose`; isolated temporary Neo4j container for QA

## Changes Verified

- `config.yaml`, `config.ollama-cloud.yaml`, and `engram/config.py` use `ollama_cloud` and `kimi-k2.7-code:cloud`.
- `.env` contains only `ENGRAM_API_KEY`, `OLLAMA_API_KEY`, `NEO4J_ADMIN_PASSWORD`, `ENGRAM_ADMIN_KEY`, and `ENGRAM_CONFIG_PATH`; values were not printed.
- `.gitignore` ignores `.env` and `.env.*`, with `!.env.example`.
- Anthropic provider/dependency and MiniMax/Anthropic env names were removed from active config/code/docs paths.
- Official Ollama model page verified `kimi-k2.7-code:cloud`: https://ollama.com/library/kimi-k2.7-code

## Runtime Notes

- Existing persisted `engram-neo4j` Docker volume rejected the configured password. I did not delete or reset that volume.
- For QA, I stopped only the persisted Neo4j container and started an isolated temporary `engram-qa-neo4j` container on ports `7474/7687`; Redis stayed from compose.
- `engram init` then passed with `Neo4j ping: True` and `Redis ping: True`.

## Automated Tests

- `tests/unit tests/integration`: `178 passed in 5.65s`
- Focused model/config/API tests: `26 passed in 2.32s`
- Selected non-browser e2e:
  - Command: `.venv/bin/pytest -q tests/e2e/test_api_auth.py tests/e2e/test_data_neo4j.py tests/e2e/test_ollama_cloud_live.py`
  - Result: `13 passed in 11.93s`
- Browser bulk-upload regression:
  - Command: `ENGRAM_BASE_URL=http://127.0.0.1:8000 .venv/bin/pytest -q tests/e2e/test_admin_bulk_upload.py`
  - Result: `1 passed in 3.06s`
- Related bulk/template regression:
  - Command: `ENGRAM_BASE_URL=http://127.0.0.1:8000 .venv/bin/pytest -q tests/unit/test_qa_server.py tests/integration/test_bulk_and_kg.py tests/e2e/test_admin_bulk_upload.py`
  - Result: `8 passed in 4.25s`

## API Evidence

- `/readyz`: ready, sqlite/neo4j/redis/filesystem true
- `/api/v1/health`: healthy, all components true
- `/api/v1/config`: Core and Frontier both `ollama_cloud`, `kimi-k2.7-code:cloud`
- `/api/v1/ingest`: accepted event `evt-ed77ebdedb09`
- SQLite ledger: `evt-ed77ebdedb09` reached `COMPLETE`
- `/api/v1/ingest/bulk`: JSONL dry-run accepted 1 row, rejected 0
- `/api/v1/kg/graph`: after ingest, 12 nodes / 12 edges
- `/admin/api/kg/graph`: KG page data, 20 nodes / 21 edges
- `/api/v1/chat/completions`: HTTP 200, live model response returned

## Browser Evidence

Screenshots:

- `screenshots/01-login.png`
- `screenshots/02-dashboard-status.png`
- `screenshots/03-dashboard-sessions.png`
- `screenshots/04-dashboard-memories.png`
- `screenshots/05-dashboard-ingest.png`
- `screenshots/06-dashboard-ingest-submitted.png`
- `screenshots/08-chat-page.png`
- `screenshots/09-chat-response.png`
- `screenshots/10-kg-page.png`

Browser checks:

- Admin login succeeded.
- Dashboard status, sessions, memories, ingest tabs loaded.
- UI ingest submitted and server logged `POST /admin/api/ingest` 202.
- UI bulk upload dry-run posted a JSONL file and server logged `POST /admin/api/ingest/bulk` 202.
- Chat page created a session and server logged `POST /admin/api/chat/completions` 200.
- Chat visible text included answer `ollama-cloud-kimi-2-7`.
- KG page loaded graph canvas and visible text showed `20 NODES, 21 EDGES`.
- Browser page errors: none.

## Issues And Resolutions

- Stale persisted Neo4j credentials: avoided destructive volume changes by using a temporary Neo4j QA container.
- Browser bulk-upload follow-up: Playwright reproduced the browser POST path and found the admin wrapper returned 200 while the underlying API uses 202. The admin wrapper now returns 202, and the Playwright regression verifies the upload request and rendered result cards.
- Wrong metrics path: `/api/v1/metrics` returned 404; correct path `/metrics` returned Prometheus output.
- One chat attempt used a ledger-only `session_id` and returned 404; rerun without a preexisting session succeeded.

## Artifacts

- API/log evidence: `qa-artifacts/20260626-215747-kimi27-qa/api/`
- Server log: `qa-artifacts/20260626-215747-kimi27-qa/logs/api-server.log`
- Bulk-upload fix server log: `qa-artifacts/20260626-215747-kimi27-qa/logs/api-server-bulk-upload-fix.log`
- Screenshots: `qa-artifacts/20260626-215747-kimi27-qa/screenshots/`
