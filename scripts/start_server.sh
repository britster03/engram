#!/bin/bash
set -a
source /home/hp/engram/.env
set +a
cd /home/hp/engram
exec .venv/bin/uvicorn engram.api.app:app --host 0.0.0.0 --port 8000
