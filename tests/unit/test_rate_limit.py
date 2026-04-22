"""Token-bucket limiter unit tests."""

import time

from engram.api.rate_limit import TokenBucketLimiter


def test_bucket_allows_up_to_capacity():
    limiter = TokenBucketLimiter(capacity=3, refill_per_minute=60)
    assert limiter.allow("k")[0]
    assert limiter.allow("k")[0]
    assert limiter.allow("k")[0]
    allowed, retry_after = limiter.allow("k")
    assert allowed is False
    assert retry_after > 0


def test_different_keys_have_independent_buckets():
    limiter = TokenBucketLimiter(capacity=1, refill_per_minute=6)
    assert limiter.allow("a")[0]
    assert limiter.allow("b")[0]
    assert not limiter.allow("a")[0]


def test_refill_restores_tokens():
    limiter = TokenBucketLimiter(capacity=1, refill_per_minute=600)  # 10/s
    assert limiter.allow("k")[0]
    assert not limiter.allow("k")[0]
    time.sleep(0.15)  # enough for > 1 token
    assert limiter.allow("k")[0]
