"""Prompt template loader. Templates live under `engram/prompts/` as Jinja2 files (§8.2)."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

_PROMPTS_DIR = Path(__file__).parent / "prompts"


@lru_cache(maxsize=1)
def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(_PROMPTS_DIR),
        autoescape=select_autoescape(enabled_extensions=()),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def render(template_name: str, **vars: Any) -> str:
    """Render `prompts/{template_name}.j2` with the given variables."""
    template = _env().get_template(f"{template_name}.j2")
    return template.render(**vars)


def source_digest(template_name: str) -> str:
    """Fingerprint the actual prompt source for derived-artifact identity."""
    env = _env()
    source, _, _ = env.loader.get_source(env, f"{template_name}.j2")
    return hashlib.sha256(source.encode()).hexdigest()
