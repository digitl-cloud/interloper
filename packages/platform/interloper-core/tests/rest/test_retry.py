"""Tests for ``interloper.rest.retry``."""

from __future__ import annotations

import httpx2

from interloper.rest.retry import HttpRetryPolicy
from interloper.retry import RetryPolicy

POLICY = HttpRetryPolicy(max_attempts=3, delay=5, jitter=0)


def _request(method: str) -> httpx2.Request:
    return httpx2.Request(method, "https://api.test/x")


class TestDefaults:
    def test_is_a_retry_policy_at_request_scale(self):
        policy = HttpRetryPolicy()

        assert isinstance(policy, RetryPolicy)
        assert (policy.max_attempts, policy.delay, policy.max_delay) == (5, 2.0, 60.0)

    def test_follows_http_by_default(self):
        policy = HttpRetryPolicy()

        assert policy.throttle_statuses == {429}
        assert policy.error_statuses == {500, 502, 503, 504}
        assert policy.idempotent_methods == {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}

    def test_its_sets_are_given_as_plain_sets(self):
        policy = HttpRetryPolicy(idempotent_methods={"GET", "POST"}, throttle_statuses={425, 429})

        assert policy.idempotent_methods == frozenset({"GET", "POST"})
        assert policy.throttle_statuses == frozenset({425, 429})


class TestRetryable:
    """Which failures are worth sending again."""

    def test_a_throttle_is_retryable_whatever_the_method(self):
        assert POLICY.retryable(_request("POST"), httpx2.Response(429))

    def test_a_server_error_is_retryable_only_for_an_idempotent_method(self):
        assert POLICY.retryable(_request("GET"), httpx2.Response(503))
        assert not POLICY.retryable(_request("POST"), httpx2.Response(503))

    def test_a_dropped_connection_is_retryable_only_for_an_idempotent_method(self):
        assert POLICY.retryable(_request("GET"), None)
        assert not POLICY.retryable(_request("POST"), None)

    def test_success_and_client_errors_are_not_retryable(self):
        for status in (200, 400, 404):
            assert not POLICY.retryable(_request("GET"), httpx2.Response(status))

    def test_a_method_declared_idempotent_is_repeated_after_a_server_error(self):
        policy = HttpRetryPolicy(idempotent_methods={"GET", "POST"})

        assert policy.retryable(_request("POST"), httpx2.Response(503))
        assert policy.retryable(_request("POST"), None)

    def test_a_declared_throttle_status_is_repeated_whatever_the_method(self):
        policy = HttpRetryPolicy(throttle_statuses={425, 429})

        assert policy.retryable(_request("POST"), httpx2.Response(425))

    def test_a_declared_error_status_is_repeated_for_an_idempotent_method(self):
        policy = HttpRetryPolicy(error_statuses={500, 520})

        assert policy.retryable(_request("GET"), httpx2.Response(520))
        assert not policy.retryable(_request("GET"), httpx2.Response(503))


class TestWait:
    """How long to wait before sending again, or None when the request is not sent again."""

    def test_a_throttle_waits_as_long_as_the_server_says(self):
        response = httpx2.Response(429, headers={"Retry-After": "7"})

        assert POLICY.wait(_request("POST"), 2, response) == 7

    def test_the_servers_wait_is_capped_at_the_longest_delay(self):
        response = httpx2.Response(429, headers={"Retry-After": "600"})

        assert HttpRetryPolicy(max_delay=30).wait(_request("GET"), 2, response) == 30

    def test_a_throttle_without_a_hint_follows_the_policy(self):
        assert POLICY.wait(_request("POST"), 2, httpx2.Response(429)) == 5

    def test_a_server_error_follows_the_policy(self):
        assert POLICY.wait(_request("GET"), 2, httpx2.Response(503)) == 5
        assert POLICY.wait(_request("GET"), 3, httpx2.Response(503)) == 10

    def test_a_failure_not_worth_repeating_is_not_waited_for(self):
        assert POLICY.wait(_request("POST"), 2, httpx2.Response(503)) is None
        assert POLICY.wait(_request("GET"), 2, httpx2.Response(404)) is None

    def test_the_budget_ends_the_retries(self):
        assert POLICY.wait(_request("GET"), 4, httpx2.Response(503)) is None


class TestOverride:
    def test_a_platform_recognises_its_own_signal_by_overriding_retryable(self):
        class ThrottleInBody(HttpRetryPolicy):
            def retryable(self, request: httpx2.Request, response: httpx2.Response | None) -> bool:
                throttled = response is not None and response.json().get("code") == 17
                return throttled or super().retryable(request, response)

        policy = ThrottleInBody(delay=5, jitter=0)

        assert policy.wait(_request("GET"), 2, httpx2.Response(400, json={"code": 17})) == 5
        assert policy.wait(_request("GET"), 2, httpx2.Response(400, json={"code": 100})) is None
