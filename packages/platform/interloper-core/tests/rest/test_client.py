"""Tests for ``interloper.rest.client``."""

from __future__ import annotations

import httpx2
import pytest

from interloper.rest.client import AsyncRESTClient, RESTClient
from interloper.rest.paginator import JSONCursorPaginator, PageNumberPaginator
from interloper.rest.retry import HttpRetryPolicy

NO_WAIT = HttpRetryPolicy(max_attempts=3, delay=0, jitter=0)


class TestTimeouts:
    def test_a_client_built_without_a_timeout_still_gives_up_on_a_stalled_read(self):
        assert RESTClient("https://api.test").timeout.read == 300
        assert AsyncRESTClient("https://api.test").timeout.read == 300


class TestWithoutPolicy:
    """A client built without a retry policy sends each request once."""

    def test_has_no_policy_by_default(self):
        assert RESTClient("https://api.test").retry is None
        assert AsyncRESTClient("https://api.test").retry is None

    @pytest.mark.parametrize("status", [429, 503])
    def test_the_sync_client_returns_a_refusal_at_once(self, status: int):
        calls: list[int] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(1)
            return httpx2.Response(status)

        client = RESTClient("https://api.test", transport=httpx2.MockTransport(handler))
        assert client.get("/x").status_code == status
        assert calls == [1]

    async def test_the_async_client_raises_a_dropped_connection_at_once(self):
        calls: list[int] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(1)
            raise httpx2.ConnectError("connection reset", request=request)

        client = AsyncRESTClient("https://api.test", transport=httpx2.MockTransport(handler))
        async with client:
            with pytest.raises(httpx2.ConnectError):
                await client.get("/x")
        assert calls == [1]


class TestRetries:
    """Both clients repeat a request the server refused for a passing reason, under the policy they were built with."""

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


class TestPaginateCookies:
    """A walk sends each page with the cookies the pages before it set, as a load balancer pinning pages needs."""

    @staticmethod
    def _cursor_walk(sent: dict[str, str | None]) -> httpx2.MockTransport:
        def handler(request: httpx2.Request) -> httpx2.Response:
            cursor = request.url.params.get("cursor", "first")
            sent[cursor] = request.headers.get("Cookie")
            following = {"first": "c1", "c1": "c2"}.get(cursor)
            headers = {"Set-Cookie": f"affinity={cursor}; Path=/"} if following else {}
            return httpx2.Response(200, json={"items": [cursor], "next": following}, headers=headers)

        return httpx2.MockTransport(handler)

    def test_the_sync_walk_sends_the_cookie_the_last_page_set(self):
        sent: dict[str, str | None] = {}
        paginator = JSONCursorPaginator(cursor_path="next", cursor_param="cursor")
        with RESTClient("https://api.test", transport=self._cursor_walk(sent)) as client:
            list(client.paginate("/x", paginator, data_selector="items"))

        assert sent == {"first": None, "c1": "affinity=first", "c2": "affinity=c1"}

    async def test_the_async_walk_sends_the_cookie_the_last_page_set(self):
        sent: dict[str, str | None] = {}
        paginator = JSONCursorPaginator(cursor_path="next", cursor_param="cursor")
        async with AsyncRESTClient("https://api.test", transport=self._cursor_walk(sent)) as client:
            [page async for page in client.paginate("/x", paginator, data_selector="items")]

        assert sent == {"first": None, "c1": "affinity=first", "c2": "affinity=c1"}

    async def test_concurrent_pages_send_the_cookie_the_first_page_set(self):
        sent: dict[str, str | None] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            page = request.url.params["page"]
            sent[page] = request.headers.get("Cookie")
            headers = {"Set-Cookie": "affinity=a; Path=/"} if page == "1" else {}
            return httpx2.Response(200, json={"total_pages": 3, "items": [page]}, headers=headers)

        paginator = PageNumberPaginator(total_path="total_pages")
        async with AsyncRESTClient("https://api.test", transport=httpx2.MockTransport(handler)) as client:
            [page async for page in client.paginate("/x", paginator, data_selector="items")]

        assert sent == {"1": None, "2": "affinity=a", "3": "affinity=a"}

    def test_a_cookie_header_the_caller_set_goes_with_every_page(self):
        sent: dict[str, str | None] = {}
        paginator = JSONCursorPaginator(cursor_path="next", cursor_param="cursor")
        with RESTClient("https://api.test", transport=self._cursor_walk(sent)) as client:
            list(client.paginate("/x", paginator, headers={"Cookie": "session=s"}, data_selector="items"))

        assert sent == {"first": "session=s", "c1": "session=s", "c2": "session=s"}

    async def test_a_cookie_header_the_client_carries_goes_with_every_page(self):
        sent: dict[str, str | None] = {}
        paginator = JSONCursorPaginator(cursor_path="next", cursor_param="cursor")
        transport = self._cursor_walk(sent)
        async with AsyncRESTClient("https://api.test", headers={"Cookie": "session=s"}, transport=transport) as client:
            [page async for page in client.paginate("/x", paginator, data_selector="items")]

        assert sent == {"first": "session=s", "c1": "session=s", "c2": "session=s"}


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


class TestDeclaredPolicy:
    async def test_a_method_the_policy_declares_idempotent_is_repeated_after_a_server_error(self):
        statuses = [503, 200]

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(statuses.pop(0))

        policy = HttpRetryPolicy(max_attempts=3, delay=0, jitter=0, idempotent_methods={"GET", "POST"})
        client = AsyncRESTClient("https://api.test", retry=policy, transport=httpx2.MockTransport(handler))
        async with client:
            assert (await client.post("/search", json={})).status_code == 200
        assert statuses == []
