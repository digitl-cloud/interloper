"""Tests for ``interloper.retry.base``."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from interloper.retry import RetryPolicy


class TestBudget:
    def test_allows_up_to_max_attempts(self):
        policy = RetryPolicy(max_attempts=3)
        assert policy.allows(1)
        assert policy.allows(3)
        assert not policy.allows(4)

    def test_max_attempts_is_at_least_one(self):
        with pytest.raises(ValidationError):
            RetryPolicy(max_attempts=0)


class TestDelay:
    def test_first_attempt_has_no_delay(self):
        assert RetryPolicy(delay=10.0).delay_before(1) == 0.0

    def test_delay_grows_geometrically(self):
        policy = RetryPolicy(delay=10.0, backoff=2.0, jitter=0.0)
        assert policy.delay_before(2) == 10.0
        assert policy.delay_before(3) == 20.0
        assert policy.delay_before(4) == 40.0

    def test_delay_is_capped(self):
        policy = RetryPolicy(delay=10.0, backoff=10.0, max_delay=50.0, jitter=0.0)
        assert policy.delay_before(4) == 50.0

    def test_jitter_stays_within_its_fraction(self):
        policy = RetryPolicy(delay=10.0, backoff=1.0, jitter=0.5)
        delays = [policy.delay_before(2) for _ in range(200)]
        assert all(5.0 <= delay <= 15.0 for delay in delays)
        assert len(set(delays)) > 1

    def test_jitter_never_yields_a_negative_delay(self):
        policy = RetryPolicy(delay=1.0, backoff=1.0, jitter=1.0)
        assert all(RetryPolicy.delay_before(policy, 2) >= 0.0 for _ in range(200))
