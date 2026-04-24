#!/bin/bash
export ANTHROPIC_BASE_URL=https://api.minimax.io/anthropic
export CORE_MODEL_API_KEY=sk-cp-7NX18l9GDcm5IapD3PDYKtsrx86k1xA_Kqpz72XpCdKbQC6aF-XFxghxUgukdT-1HEwLf82MyMRbYMDj_FEr2rKhxmP-aiuZ4GFP-Od0lKeVqH9jgpEs5cE
export ENGRAM_ADMIN_KEY=m33oMciayHk6jMdAnm3bB-VOaCjYvkw-cVT-VKMRLAg
export ENGRAM_API_KEY=m33oMciayHk6jMdAnm3bB-VOaCjYvkw-cVT-VKMRLAg
export ENGRAM_CONFIG_PATH=./config.yaml
export ENGRAM_DATA_DIR=./data/mem
export FRONTIER_LLM_API_KEY=sk-cp-7NX18l9GDcm5IapD3PDYKtsrx86k1xA_Kqpz72XpCdKbQC6aF-XFxghxUgukdT-1HEwLf82MyMRbYMDj_FEr2rKhxmP-aiuZ4GFP-Od0lKeVqH9jgpEs5cE
export NEO4J_ADMIN_PASSWORD=engram-admin
cd /home/hp/engram
.venv/bin/uvicorn engram.api.app:app --port 8000 --timeout-keep-alive 60
