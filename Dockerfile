# syntax=docker/dockerfile:1.7

# ---- Dependency stage ----------------------------------------------------
FROM python:3.11-slim AS dependencies

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

# Install build deps for wheels that need them (sentence-transformers → torch CPU).
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./

# Install dependencies from a minimal package shell. Application source is
# copied only in the package stage, so ordinary code changes do not reinstall
# Torch and the full Python dependency graph.
RUN mkdir -p engram && touch engram/__init__.py

RUN python -m venv /opt/engram && \
    /opt/engram/bin/pip install --upgrade pip wheel && \
    /opt/engram/bin/pip install hatchling && \
    /opt/engram/bin/pip install tiktoken && \
    /opt/engram/bin/pip install --extra-index-url https://download.pytorch.org/whl/cpu torch && \
    /opt/engram/bin/pip install '.' && \
    /opt/engram/bin/pip install 'gunicorn>=22.0'

# The production backend network has no Internet egress. Bake the configured
# embedding model into the immutable image so worker/API startup never tries to
# fetch it at runtime.
RUN HF_HOME=/opt/engram-model-cache \
    /opt/engram/bin/python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-small-en-v1.5')"

# ---- Application wheel ---------------------------------------------------
FROM dependencies AS package

COPY engram ./engram
RUN /opt/engram/bin/pip wheel --no-deps --no-build-isolation \
    --wheel-dir /tmp/wheels .

# ---- Runtime dependency base --------------------------------------------
FROM python:3.11-slim AS runtime-base

ENV PATH="/opt/engram/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ENGRAM_LOG_FORMAT=json \
    ENGRAM_DATA_DIR=/var/lib/engram/mem \
    ENGRAM_CONFIG_PATH=/etc/engram/config.yaml \
    HF_HOME=/opt/engram-model-cache \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 10001 --create-home --home-dir /home/engram engram && \
    mkdir -p /var/lib/engram/mem /var/log/engram /etc/engram && \
    chown -R engram:engram /var/lib/engram /var/log/engram

COPY --from=dependencies /opt/engram /opt/engram
COPY --from=dependencies /opt/engram-model-cache /opt/engram-model-cache
COPY config.yaml /etc/engram/config.yaml

# ---- Final runtime -------------------------------------------------------
FROM runtime-base AS runtime

COPY --from=package /tmp/wheels /tmp/wheels
RUN /opt/engram/bin/pip install --no-deps --force-reinstall /tmp/wheels/engram-*.whl && \
    rm -rf /tmp/wheels
COPY engram/prompts /opt/engram/lib/python3.11/site-packages/engram/prompts
COPY templates /opt/engram/lib/python3.11/site-packages/templates

USER engram
WORKDIR /var/lib/engram

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:8000/livez || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["gunicorn", "engram.api.app:app", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--workers", "1", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "120", \
     "--graceful-timeout", "30", \
     "--no-control-socket", \
     "--access-logfile", "-", \
     "--access-logformat", "%(h)s %(l)s %(u)s %(t)s \"%(r)s\" %(s)s %(b)s %(L)s \"%({x-request-id}i)s\""]
