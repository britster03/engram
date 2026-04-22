"""Real tokenizer unit tests (tiktoken + char fallback)."""

from engram.tokens import count_tokens, truncate_to_tokens


def test_count_tokens_empty_returns_zero():
    assert count_tokens("") == 0


def test_count_tokens_monotone():
    short = count_tokens("hello")
    longer = count_tokens("hello, world, this is a longer string with more tokens")
    assert longer > short
    assert short >= 1


def test_truncate_returns_tail_by_default():
    text = "sentence one. sentence two. sentence three. sentence four."
    short = truncate_to_tokens(text, 3)
    assert len(short) < len(text)
    # Tail preference: "four" should survive before "one" since we keep the end.
    assert "four" in short or short == ""


def test_truncate_returns_prefix_when_requested():
    text = "alpha beta gamma delta epsilon zeta eta theta iota"
    short = truncate_to_tokens(text, 3, from_end=False)
    assert "alpha" in short


def test_truncate_no_op_when_within_budget():
    text = "hi"
    assert truncate_to_tokens(text, 10) == text
