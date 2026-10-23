"""Token counting helpers.

For accurate budget enforcement in MSC assembly (§4.4.2) we need a real
tokenizer, not `len(text) / 3.5`. The frontier LLM's own tokenizer would be
ideal; this implementation uses `tiktoken` when available and otherwise uses
the char-ratio heuristic as a last-ditch fallback with a safety factor.

Counters are process-cached to avoid reloading the BPE vocab per call.
"""

from __future__ import annotations

import functools
import hashlib
import logging
import os
import tempfile
from pathlib import Path

log = logging.getLogger(__name__)

_DEFAULT_ENCODING = "cl100k_base"  # widely supported chat-model BPE
_FALLBACK_CHAR_RATIO = 3.5  # chars per token, conservative
_SAFETY_FACTOR = 1.10  # fallback scales +10% to stay under budgets

ENCODING_NAME = _DEFAULT_ENCODING
_ENCODING_URL = "https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken"
_ENCODING_SHA256 = "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7"


class TokenizerUnavailableError(RuntimeError):
    """Exact session budget enforcement cannot proceed."""


@functools.lru_cache(maxsize=1)
def require_tokenizer():
    """Load the pinned encoding; production checks its cache before any network IO."""
    if os.environ.get("ENGRAM_TOKENIZER_OFFLINE") == "1":
        directory = os.environ.get("TIKTOKEN_CACHE_DIR") or os.environ.get("DATA_GYM_CACHE_DIR") or str(Path(tempfile.gettempdir()) / "data-gym-cache")
        path = Path(directory) / hashlib.sha1(_ENCODING_URL.encode()).hexdigest()
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != _ENCODING_SHA256:
            raise TokenizerUnavailableError("cl100k_base cache is missing or corrupt; rebuild the image with its tokenizer cache")
    try:
        import tiktoken
        return tiktoken.get_encoding(ENCODING_NAME)
    except Exception as error:
        raise TokenizerUnavailableError("cl100k_base tokenizer could not be initialized") from error


def count_tokens_exact(text: str) -> int:
    """Count with cl100k_base and never substitute a character estimate."""
    try:
        return len(require_tokenizer().encode(text, disallowed_special=()))
    except TokenizerUnavailableError:
        raise
    except Exception as error:
        raise TokenizerUnavailableError("exact token counting failed") from error


@functools.lru_cache(maxsize=1)
def _get_encoder():
    """Load tiktoken's cl100k_base once. Returns None if tiktoken unavailable."""
    try:
        return require_tokenizer()
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
