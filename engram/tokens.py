"""Token counting helpers.

For accurate budget enforcement in MSC assembly (§4.4.2) we need a real
tokenizer, not `len(text) / 3.5`. The frontier LLM's own tokenizer would be
ideal; this implementation uses `tiktoken` when available and otherwise uses
the char-ratio heuristic as a last-ditch fallback with a safety factor.

Counters are process-cached to avoid reloading the BPE vocab per call.
"""

from __future__ import annotations

import functools
import logging
import threading

log = logging.getLogger(__name__)

_DEFAULT_ENCODING = "cl100k_base"      # widely supported chat-model BPE
_FALLBACK_CHAR_RATIO = 3.5              # chars per token, conservative
_SAFETY_FACTOR = 1.10                   # fallback scales +10% to stay under budgets

_lock = threading.Lock()


@functools.lru_cache(maxsize=1)
def _get_encoder():
    """Load tiktoken's cl100k_base once. Returns None if tiktoken unavailable."""
    try:
        import tiktoken  # type: ignore
    except Exception:
        log.info("tiktoken not installed; using character-ratio fallback token counter")
        return None
    try:
        return tiktoken.get_encoding(_DEFAULT_ENCODING)
    except Exception:
        log.warning("tiktoken encoding %s unavailable; using fallback", _DEFAULT_ENCODING)
        return None


def count_tokens(text: str) -> int:
    """Return the token count for `text`. Never raises, never returns 0 for non-empty input."""
    if not text:
        return 0
    enc = _get_encoder()
    if enc is not None:
        try:
            return len(enc.encode(text, disallowed_special=()))
        except Exception:
            # Some characters (surrogate pairs, custom tokens) trip tiktoken —
            # fall through to the char-ratio path rather than raising.
            pass
    approx = max(1, int(len(text) / _FALLBACK_CHAR_RATIO * _SAFETY_FACTOR))
    return approx


def truncate_to_tokens(text: str, max_tokens: int, *, from_end: bool = True) -> str:
    """Return a suffix (default) or prefix of `text` that fits within `max_tokens`.

    For LTM context we prefer to preserve the tail (most recent / specific
    information); for session context we also want the tail. `from_end=False`
    keeps the prefix — useful for pre-existing summaries.
    """
    if max_tokens <= 0 or not text:
        return ""
    if count_tokens(text) <= max_tokens:
        return text
    enc = _get_encoder()
    if enc is not None:
        try:
            toks = enc.encode(text, disallowed_special=())
            kept = toks[-max_tokens:] if from_end else toks[:max_tokens]
            return enc.decode(kept)
        except Exception:
            pass
    # Char fallback — binary search on character count.
    lo, hi = 0, len(text)
    best = ""
    while lo <= hi:
        mid = (lo + hi) // 2
        candidate = text[-mid:] if from_end else text[:mid]
        if count_tokens(candidate) <= max_tokens:
            best = candidate
            lo = mid + 1
        else:
            hi = mid - 1
    return best
