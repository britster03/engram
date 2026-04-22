# syntax=docker/dockerfile:1.7

# ---- Build stage ---------------------------------------------------------
FROM python:3.11-slim AS build

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

# Install build deps for wheels that need them (sentence-transformers → torch CPU).
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY engram ./engram

RUN python -m venv /opt/engram && \
    /opt/engram/bin/pip install --upgrade pip wheel && \
    /opt/engram/bin/pip install tiktoken && \
    /opt/engram/bin/pip install --extra-index-url https://download.pytorch.org/whl/cpu torch && \
    /opt/engram/bin/pip install '.' && \
    /opt/engram/bin/pip install 'gunicorn>=22.0'

# ---- Runtime stage -------------------------------------------------------
FROM python:3.11-slim AS runtime

ENV PATH="/opt/engram/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ENGRAM_LOG_FORMAT=json \
    ENGRAM_DATA_DIR=/var/lib/engram/mem \
    ENGRAM_CONFIG_PATH=/etc/engram/config.yaml

RUN apt-get update && apt-get install -y --no-install-recommends \
    tini curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --uid 10001 --create-home --home-dir /home/engram engram && \
    mkdir -p /var/lib/engram/mem /var/log/engram /etc/engram && \
    chown -R engram:engram /var/lib/engram /var/log/engram

COPY --from=build /opt/engram /opt/engram
COPY config.yaml /etc/engram/config.yaml
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
     "--access-logfile", "-", \
     "--access-logformat", "%(h)s %(l)s %(u)s %(t)s \"%(r)s\" %(s)s %(b)s %(L)s \"%({x-request-id}i)s\""]
