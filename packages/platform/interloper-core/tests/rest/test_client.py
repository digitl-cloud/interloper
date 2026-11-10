"""Tests for ``interloper.rest.client``."""

from __future__ import annotations

import httpx2
import pytest

from interloper.rest.client import AsyncRESTClient, RESTClient, TransientRetry
from interloper.retry import RetryPolicy

POLICY = RetryPolicy(max_attempts=3, delay=5, jitter=0)
NO_WAIT = RetryPolicy(max_attempts=3, delay=0, jitter=0)


class TestTransientRetry:
    """Which failures are worth another attempt, and how long to wait first."""

    def test_throttling_waits_as_long_as_the_server_says_whatever_the_method(self):
        request = httpx2.Request("POST", "https://api.test/query")
        assert TransientRetry(POLICY).wait(request, 2, httpx2.Response(429, headers={"Retry-After": "7"})) == 7

    def test_throttling_without_a_hint_follows_the_policy(self):
        request = httpx2.Request("POST", "https://api.test/query")
        assert TransientRetry(POLICY).wait(request, 2, httpx2.Response(429)) == 5

    def test_a_server_error_is_retried_only_for_an_idempotent_request(self):
        retry = TransientRetry(POLICY)
        assert retry.wait(httpx2.Request("GET", "https://api.test/x"), 2, httpx2.Response(503)) == 5
        assert retry.wait(httpx2.Request("POST", "https://api.test/x"), 2, httpx2.Response(503)) is None

    def test_a_dropped_connection_is_retried_only_for_an_idempotent_request(self):
        retry = TransientRetry(POLICY)
        assert retry.wait(httpx2.Request("GET", "https://api.test/x"), 2, None) == 5
        assert retry.wait(httpx2.Request("POST", "https://api.test/x"), 2, None) is None

    def test_success_and_client_errors_are_not_retried(self):
        request = httpx2.Request("GET", "https://api.test/x")
        for status in (200, 400, 404):
            assert TransientRetry(POLICY).wait(request, 2, httpx2.Response(status)) is None

    def test_the_budget_ends_the_retries(self):
        request = httpx2.Request("GET", "https://api.test/x")
        assert TransientRetry(POLICY).wait(request, 4, httpx2.Response(503)) is None


class TestTimeouts:
    def test_a_client_built_without_a_timeout_still_gives_up_on_a_stalled_read(self):
        assert RESTClient("https://api.test").timeout.read == 300
        assert AsyncRESTClient("https://api.test").timeout.read == 300


class TestRetries:
    """Both clients repeat a request the server refused for a passing reason."""

    def test_the_sync_client_repeats_a_server_error(self):
        statuses = [503, 200]

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(statuses.pop(0))

        client = RESTClient("https://api.test", retry=NO_WAIT, transport=httpx2.MockTransport(handler))
        assert client.get("/x").status_code == 200
        assert statuses == []

    async def test_the_async_client_repeats_a_throttled_request(self):
        calls: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(request.method)
            return httpx2.Response(429, headers={"Retry-After": "0"}) if len(calls) == 1 else httpx2.Response(200)

        client = AsyncRESTClient("https://api.test", retry=NO_WAIT, transport=httpx2.MockTransport(handler))
        async with client:
            assert (await client.post("/query", json={})).status_code == 200
        assert calls == ["POST", "POST"]

    async def test_a_throttle_that_outlasts_the_budget_returns_the_last_response(self):
        calls: list[int] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(1)
            return httpx2.Response(429)

        client = AsyncRESTClient("https://api.test", retry=NO_WAIT, transport=httpx2.MockTransport(handler))
        async with client:
            assert (await client.get("/x")).status_code == 429
        assert len(calls) == 3

    def test_the_sync_client_repeats_a_dropped_read_until_the_budget_is_spent(self):
        calls: list[int] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(1)
            raise httpx2.ConnectError("connection reset", request=request)

        client = RESTClient("https://api.test", retry=NO_WAIT, transport=httpx2.MockTransport(handler))
        with pytest.raises(httpx2.ConnectError):
            client.get("/x")
        assert len(calls) == 3

    async def test_the_async_client_never_repeats_a_dropped_write(self):
        calls: list[int] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(1)
            raise httpx2.ConnectError("connection reset", request=request)

        client = AsyncRESTClient("https://api.test", retry=NO_WAIT, transport=httpx2.MockTransport(handler))
        async with client:
            with pytest.raises(httpx2.ConnectError):
                await client.post("/query", json={})
        assert calls == [1]


class TestAsyncRESTClient:
    """The async counterpart shares RESTClient's construction surface."""

    async def test_applies_base_url_headers_and_params(self):
        seen: dict[str, str | None] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen["path"] = request.url.path
            seen["token"] = request.headers.get("Access-Token")
            seen["a"] = request.url.params.get("a")
            return httpx2.Response(200, json={"ok": True})

        client = AsyncRESTClient(
            "https://api.test",
            headers={"Access-Token": "tok"},
            transport=httpx2.MockTransport(handler),
        )
        async with client:
            response = await client.get("/things", params={"a": "1"})

        assert response.json() == {"ok": True}
        assert seen == {"path": "/things", "token": "tok", "a": "1"}
