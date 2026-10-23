import json
import re
from dataclasses import asdict

import pytest

from engram import tokens
from engram.config import SessionCacheConfig, SessionConfig
from engram.models.core import CompletionResult, CoreModelError
from engram.session.compaction import input_tokens
from engram.session.manager import SessionManager, SessionState, Turn, compact_session
from engram.storage.redis_cache import SessionCache


class Core:
    def __init__(self, callback=None, output=None):
        self.calls = []
        self.callback = callback
        self.output = output if output is not None else {"compacted": "A verified rolling summary.", "key_facts": [], "key_entities": []}

    def complete(self, **kwargs):
        self.calls.append(kwargs)
        if self.callback:
            callback, self.callback = self.callback, None
            callback()
        return CompletionResult(output=self.output, raw_text=json.dumps(self.output))


def manager(**kwargs):
    cfg = SessionConfig(compaction_trigger_tokens=2048, compaction_summary_tokens=64,
                        compaction_input_tokens=4096, **kwargs)
    cache = SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=3600)
    return SessionManager(cache, session_config=cfg, session_ttl_minutes=60)


def long_session(mgr):
    state = mgr.create()
    mgr.append_turn_pair(state.session_id, " alpha" * 3000, " accepted")
    mgr.append_turn_pair(state.session_id, "recent user", "recent assistant")
    return mgr.get(state.session_id)


def test_exact_64k_boundary_and_configurable_trigger():
    mgr = SessionManager(SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=60),
                         session_ttl_minutes=1)
    state = SessionState("threshold", turns=[Turn("user", " a" * 64000, "date", 0)])
    n = mgr.context_tokens(state)
    assert n >= 64000 and mgr.needs_compaction(state)
    mgr.session_config.compaction_trigger_tokens = n
    assert mgr.needs_compaction(state)
    mgr.session_config.compaction_trigger_tokens = n + 1
    assert not mgr.needs_compaction(state)


def test_many_short_turns_and_long_characters_do_not_trigger_early():
    mgr = manager()
    state = SessionState("short", turns=[Turn("user", "hello", "date", i) for i in range(100)])
    assert not mgr.needs_compaction(state)
    state.turns = [Turn("user", " " * 15000, "date", 0)]
    assert not mgr.needs_compaction(state)


def test_render_counts_only_active_tail_and_prior_summary():
    state = SessionState("render", turns=[Turn("user", "covered", "date", 0),
        Turn("assistant", "fresh", "date", 1)], compacted_turns_idx_upper_bound=1, compacted="prior")
    assert "covered" not in state.render(max_turns=None)
    assert "prior" in state.render(max_turns=None) and "fresh" in state.render(max_turns=None)
    assert "fresh" not in state.render(max_turns=0)


def test_compaction_preserves_pairs_originals_and_output_budget():
    mgr = manager()
    state = long_session(mgr)
    originals = [asdict(t) for t in state.turns]
    core = Core()
    result = compact_session(mgr, state, core)
    assert result.compacted_turns_idx_upper_bound == 2
    assert [asdict(t) for t in result.turns] == originals
    assert "recent user" in result.render() and "alpha" not in result.render()
    assert mgr.context_tokens(result) < 1024
    assert core.calls[0]["max_tokens"] == 4096
    assert all(input_tokens(c["system_prompt"]) <= mgr.session_config.compaction_input_tokens for c in core.calls)


def test_concurrent_append_survives():
    mgr = manager()
    state = long_session(mgr)
    core = Core(callback=lambda: mgr.append_turn_pair(state.session_id, "new accepted user", "new reply"))
    result = compact_session(mgr, state, core)
    assert [t.content for t in result.turns[-2:]] == ["new accepted user", "new reply"]


def test_stale_result_cannot_replace_newer_summary():
    mgr = manager()
    state = long_session(mgr)
    newer = Core(output={"compacted": "Newer summary.", "key_facts": [], "key_entities": []})
    core = Core(callback=lambda: compact_session(mgr, mgr.get(state.session_id), newer))
    result = compact_session(mgr, state, core)
    assert result.compacted == "Newer summary."
    assert result.compacted_turns_idx_upper_bound == 2


def test_close_fences_compaction():
    mgr = manager()
    state = long_session(mgr)
    def close():
        current = mgr.get(state.session_id)
        current.status = "COMMITTING"
        mgr._persist(current)
    result = compact_session(mgr, state, Core(callback=close))
    assert result.status == "COMMITTING" and result.compacted is None


@pytest.mark.parametrize("output", [
    {},
    {"compacted": "", "key_facts": [], "key_entities": []},
    {"compacted": "valid", "key_facts": "invalid", "key_entities": []},
    {"compacted": " x" * 100, "key_facts": [], "key_entities": []},
    {"compacted": "valid", "key_facts": [" a" * 100], "key_entities": []},
])
def test_invalid_output_leaves_boundary_unchanged(output):
    mgr = manager()
    state = long_session(mgr)
    original = mgr.cache.get(state.session_id)
    with pytest.raises(CoreModelError):
        compact_session(mgr, state, Core(output=output))
    assert mgr.cache.get(state.session_id) == original


def test_unicode_oversized_turn_segments_cover_original_without_loss():
    mgr = manager()
    mgr.session_config.compaction_input_tokens = 1024
    original = "你好🙂 café <|endoftext|> " * 600
    state = mgr.create()
    mgr.append_turn_pair(state.session_id, original, "done")
    state = mgr.get(state.session_id)
    core = Core()
    result = compact_session(mgr, state, core)
    pattern = re.compile(r"\[0 \| .*? \| (\d+):(\d+)\] USER: (.*?)\n", re.S)
    segments = [match for call in core.calls for match in pattern.findall(call["system_prompt"])]
    assert len(segments) > 1
    assert "".join(text for _, _, text in segments) == original
    assert int(segments[0][0]) == 0 and int(segments[-1][1]) == len(original)
    assert result.turns[0].content == original
    assert all(input_tokens(call["system_prompt"]) <= 1024 for call in core.calls)


def test_offline_cache_missing_fails_without_network(tmp_path, monkeypatch):
    tokens.require_tokenizer.cache_clear()
    monkeypatch.setenv("ENGRAM_TOKENIZER_OFFLINE", "1")
    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", str(tmp_path))
    import requests
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("network attempted"))
    try:
        with pytest.raises(tokens.TokenizerUnavailableError, match="missing or corrupt"):
            tokens.count_tokens_exact("hello")
    finally:
        tokens.require_tokenizer.cache_clear()

def test_default_64k_compaction_retains_approximately_32k_and_whole_pairs():
    mgr = SessionManager(SessionCache(SessionCacheConfig(backend="memory"), default_ttl_seconds=3600),
                         session_ttl_minutes=60)
    state = mgr.create()
    for _ in range(33):
        mgr.append_turn_pair(state.session_id, " a" * 2000, " b" * 64)
    state = mgr.get(state.session_id)
    assert mgr.context_tokens(state) >= 64000
    result = compact_session(mgr, state, Core())
    assert 24000 <= mgr.context_tokens(result) <= 32000
    assert result.compacted_turns_idx_upper_bound % 2 == 0
    assert len(result.turns) == 66


def test_provider_failure_cannot_publish_partial_batches():
    mgr = manager()
    mgr.session_config.compaction_input_tokens = 1024
    state = long_session(mgr)
    original = mgr.cache.get(state.session_id)
    class FailingCore(Core):
        def complete(self, **kwargs):
            if self.calls:
                raise CoreModelError("provider unavailable")
            return super().complete(**kwargs)
    with pytest.raises(CoreModelError, match="provider unavailable"):
        compact_session(mgr, state, FailingCore())
    assert mgr.cache.get(state.session_id) == original


def test_legacy_summary_growth_is_normalized_and_replaced():
    mgr = manager()
    state = long_session(mgr)
    state.compacted = " legacy" * 800
    state.key_facts = ["old fact"]
    mgr._persist(state)
    core = Core()
    result = compact_session(mgr, state, core)
    assert result.compacted == core.output["compacted"]
    assert len(core.calls) > 1
    assert "legacy" not in result.compacted
    assert all(input_tokens(c["system_prompt"]) <= 4096 for c in core.calls)


def test_corrupt_offline_cache_fails_without_network(tmp_path, monkeypatch):
    import hashlib
    path = tmp_path / hashlib.sha1(tokens._ENCODING_URL.encode()).hexdigest()
    path.write_bytes(b"corrupt encoding")
    tokens.require_tokenizer.cache_clear()
    monkeypatch.setenv("ENGRAM_TOKENIZER_OFFLINE", "1")
    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", str(tmp_path))
    try:
        with pytest.raises(tokens.TokenizerUnavailableError, match="missing or corrupt"):
            tokens.require_tokenizer()
    finally:
        tokens.require_tokenizer.cache_clear()
def test_final_session_summary_uses_bounded_batches():
    from engram.session.manager import commit_session
    mgr = manager()
    mgr.session_config.compaction_input_tokens = 1024
    state = long_session(mgr)
    core = Core()
    result = commit_session(mgr, state, core)
    assert result.status == "COMMITTED"
    assert result.compacted == core.output["compacted"]
    assert len(core.calls) > 1
    assert all(input_tokens(c["system_prompt"]) <= 1024 for c in core.calls)
    assert len(result.turns) == 4
