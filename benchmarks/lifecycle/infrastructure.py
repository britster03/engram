from __future__ import annotations

import json
import os
import secrets
import subprocess
import tempfile
import time
from pathlib import Path

import yaml

from .ledger import atomic, digest

ROOT = Path(__file__).resolve().parents[2]


def image_names(tag=None):
    tag = tag or os.environ.get("LOCOMO_IMAGE_TAG", "20260930")
    return {"native": "engram-lifecycle-native:" + tag,
            "benchmark": "engram-lifecycle:" + tag,
            "scoring": "engram-lifecycle-scoring:" + tag}


def dotenv(path):
    values = {}
    if not Path(path).exists():
        return values
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.removeprefix("export ").split("=", 1)
        values[name.strip()] = value.strip().strip("\"'")
    return values


def prepare(directory, source_config, project):
    if not project.startswith("engram-lifecycle-"):
        raise ValueError("Unsafe Compose project name")
    directory = Path(directory).resolve()
    runtime = directory / ".runtime"
    runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
    credential_path = runtime / "credentials.env"
    env_path = runtime / "compose.env"
    if env_path.exists():
        return dotenv(env_path)
    cfg = yaml.safe_load(Path(source_config).read_text())
    from engram.config import SessionConfig, RetrievalConfig, apply_config_environment, configuration_environment
    if not cfg.get("_benchmark_config_frozen"):
        cfg = apply_config_environment(cfg, configuration_environment(source_config))
    cfg["session"] = SessionConfig.model_validate(cfg.get("session", {})).model_dump(
        exclude={"window_threshold_ratio", "max_turns_before_window"})
    cfg["retrieval"] = RetrievalConfig.model_validate(cfg.get("retrieval", {})).model_dump()
    cfg["_benchmark_config_frozen"] = True
    inherited = {**dotenv(ROOT / ".env"), **dotenv(ROOT / ".env.prod"),
                 **configuration_environment(source_config)}
    # Only explicitly referenced MODEL credentials can leave the host.
    credentials = {}
    for section in ("core_model", "frontier_llm"):
        import re
        for field in ("api_key", "api_base"):
            raw = cfg.get(section, {}).get(field)
            for name in re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", str(raw or "")):
                if name not in inherited:
                    raise RuntimeError(f"Missing model environment variable: {name}")
                credentials[name] = inherited[name]
    password = secrets.token_hex(24)
    dsn = f"postgresql://bench:{password}@postgres:5432/engram_bench"
    cfg["event_ledger"] = {**cfg.get("event_ledger", {}), "backend": "postgres", "dsn": "${ENGRAM_DATABASE_URL}"}
    cfg["knowledge_graph"] = {**cfg.get("knowledge_graph", {}), "backend": "neo4j", "uri": "bolt://neo4j:7687", "writer_username": "neo4j", "writer_password": "", "reader_username": "neo4j", "reader_password": ""}
    cfg["session_cache"] = {"backend": "redis", "redis_url": "redis://redis:6379/0"}
    namespace = project.removeprefix("engram-")
    cfg["temporal"] = {**cfg.get("temporal", {}), "enabled": True, "address": "temporal:7233", "namespace": namespace}
    cfg["api"] = {**cfg.get("api", {}), "api_key": "${ENGRAM_API_KEY}", "admin_key": "${ENGRAM_ADMIN_KEY}"}
    credentials.update(ENGRAM_DATABASE_URL=dsn, ENGRAM_API_KEY=secrets.token_hex(32), ENGRAM_ADMIN_KEY=secrets.token_hex(32), ENGRAM_TEST_DATABASE_URL=dsn, ENGRAM_TEST_NEO4J_URI="bolt://neo4j:7687", ENGRAM_TEST_NEO4J_USERNAME="neo4j", ENGRAM_TEST_NEO4J_PASSWORD="")
    for key, value in credentials.items():
        if "\n" in value or "\r" in value:
            raise ValueError(f"Invalid multiline credential: {key}")
    credential_path.write_text("\n".join(f"{k}={v}" for k, v in credentials.items()) + "\n")
    credential_path.chmod(0o600)
    (runtime / "config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False))
    (runtime / "config.yaml").chmod(0o600)
    images = image_names()
    env = {"LOCOMO_PROJECT": project, "LOCOMO_DB_PASSWORD": password, "LOCOMO_NAMESPACE": namespace, "LOCOMO_IMAGE": images["benchmark"], "LOCOMO_SCORING_IMAGE": images["scoring"], "LOCOMO_REPO": str(ROOT), "LOCOMO_RUNTIME_DIR": str(runtime), "LOCOMO_RUN_DIR": str(directory), "LOCOMO_UID": str(os.getuid()), "LOCOMO_GID": str(os.getgid())}
    env_path.write_text("\n".join(f"{k}={v}" for k, v in env.items()) + "\n")
    env_path.chmod(0o600)
    atomic(directory / "isolation_manifest.json", {"project": project, "namespace": namespace, "source_config_hash": digest(cfg), "services": ["postgres", "neo4j", "redis", "temporal", "worker", "dispatcher"], "production_services_modified": False})
    return env


def compose(directory, *args, check=True, capture=False, monitor=None):
    env_path = Path(directory).resolve() / ".runtime/compose.env"
    env = dotenv(env_path)
    project = env.get("LOCOMO_PROJECT", "")
    if not project.startswith("engram-lifecycle-"):
        raise ValueError("Benchmark project required")
    command = ["docker", "compose", "--project-name", project, "--env-file", str(env_path), "--file", str(ROOT / "benchmarks/lifecycle/compose.yaml"), *args]
    if monitor is None:
        return subprocess.run(command, cwd=ROOT, check=check, capture_output=capture, text=True)
    # Files avoid pipe-buffer deadlocks while lengthy local scoring is monitored.
    with tempfile.TemporaryFile(mode="w+") as stdout, tempfile.TemporaryFile(mode="w+") as stderr:
        process=subprocess.Popen(command,cwd=ROOT,text=True,stdout=stdout if capture else None,stderr=stderr if capture else None)
        previous=0
        try:
            while process.poll() is None:
                if time.monotonic()-previous>=30:
                    monitor()
                    previous=time.monotonic()
                time.sleep(.5)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        stdout.seek(0);stderr.seek(0)
        result=subprocess.CompletedProcess(command,process.returncode,stdout.read() if capture else None,stderr.read() if capture else None)
        if check:
            result.check_returncode()
        return result


def assert_isolated():
    from engram.config import get_config
    from psycopg.conninfo import conninfo_to_dict
    cfg = get_config()
    project = os.environ.get("LOCOMO_PROJECT", "")
    parts = conninfo_to_dict(cfg.event_ledger.dsn)
    if not project.startswith("engram-lifecycle-") or parts.get("host") != "postgres" or parts.get("dbname") != "engram_bench" or cfg.knowledge_graph.uri != "bolt://neo4j:7687" or cfg.session_cache.redis_url != "redis://redis:6379/0" or cfg.temporal.namespace != project.removeprefix("engram-"):
        raise RuntimeError("Benchmark isolation configuration mismatch")
    return cfg
