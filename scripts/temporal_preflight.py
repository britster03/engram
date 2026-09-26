#!/usr/bin/env python3
"""Validate a single-host Temporal deployment before Docker Compose starts it."""

from __future__ import annotations

import argparse
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

REQUIRED = (
    "ENGRAM_API_KEY",
    "ENGRAM_ADMIN_KEY",
    "NEO4J_ADMIN_PASSWORD",
    "POSTGRES_PASSWORD",
    "ENGRAM_DB_PASSWORD",
    "ENGRAM_DATABASE_URL",
    "ENGRAM_IMAGE_TAG",
)
PLACEHOLDERS = ("replace_me", "change-me", "your-", "<")
MIN_SECRET_LENGTH = 16


def _load(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _valid_postgres_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
        return bool(
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.hostname
            and parsed.path not in {"", "/"}
            and parsed.username
            and parsed.password
        )
    except ValueError:
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env.prod")
    args = parser.parse_args(argv)
    env_file = Path(args.env_file)
    errors: list[str] = []
    if not env_file.is_file():
        errors.append(f"missing environment file: {env_file}")
        values: dict[str, str] = {}
    else:
        values = _load(env_file)
        mode = stat.S_IMODE(env_file.stat().st_mode)
        if mode & 0o077:
            errors.append(f"{env_file} must be owner-readable only (chmod 600)")
    for name in REQUIRED:
        value = values.get(name, "")
        if not value or any(token in value.lower() for token in PLACEHOLDERS):
            errors.append(f"missing or placeholder value for {name}")
    for name in (
        "ENGRAM_API_KEY",
        "ENGRAM_ADMIN_KEY",
        "ENGRAM_SECRET_KEY",
        "POSTGRES_PASSWORD",
        "ENGRAM_DB_PASSWORD",
    ):
        value = values.get(name, "")
        if value and len(value) < MIN_SECRET_LENGTH:
            errors.append(f"{name} must be at least {MIN_SECRET_LENGTH} characters")
    database_url = values.get("ENGRAM_DATABASE_URL", "")
    if database_url and not _valid_postgres_url(database_url):
        errors.append(
            "ENGRAM_DATABASE_URL must be a PostgreSQL URL with username, "
            "password, host, and database"
        )
    elif database_url:
        parsed_url = urlparse(database_url)
        if unquote(parsed_url.password or "") != values.get("ENGRAM_DB_PASSWORD", ""):
            errors.append(
                "ENGRAM_DATABASE_URL password must match ENGRAM_DB_PASSWORD"
            )
    if values.get("ENGRAM_IMAGE_TAG", "").lower() == "latest":
        errors.append("ENGRAM_IMAGE_TAG must be an immutable release tag, not 'latest'")
    if values.get("POSTGRES_PASSWORD") == values.get("ENGRAM_DB_PASSWORD"):
        errors.append("POSTGRES_PASSWORD must differ from ENGRAM_DB_PASSWORD")
    model_api_key = values.get("OPENCODE_GO_API_KEY", "")
    if not model_api_key or any(
        token in model_api_key.lower() for token in PLACEHOLDERS
    ):
        errors.append("missing or placeholder value for OPENCODE_GO_API_KEY")
    if shutil.disk_usage(".").free < 40 * 1024**3:
        errors.append("less than 40 GiB free disk space")
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        errors.append("Docker daemon is unavailable to the current user")
    if errors:
        print("Preflight failed:", *[f"- {error}" for error in errors], sep="\n", file=sys.stderr)
        return 1
    print("Preflight passed: secrets, permissions, disk capacity, and Docker daemon are ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
