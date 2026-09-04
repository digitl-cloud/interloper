"""Tests for ``interloper.rest.paginator`` and the clients' ``paginate``."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from interloper.rest.client import AsyncRESTClient, RESTClient
from interloper.rest.paginator import (
    HeaderLinkPaginator,
    JSONCursorPaginator,
    JSONLinkPaginator,
    OffsetPaginator,
    PageNumberPaginator,
    SinglePagePaginator,
    _extract,
    select,
)


def _flatten(pages: list[list[dict]]) -> list[dict]:
    return [row for page in pages for row in page]


# -- Page-number ---------------------------------------------------------------


def _page_number_handler(total_pages: int) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page", "1"))
        return httpx.Response(200, json={"total_pages": total_pages, "items": [{"p": page}]})

    return handler


class TestPageNumberSync:
    def test_walks_all_pages_with_known_total(self):
        client = RESTClient("https://t", transport=httpx.MockTransport(_page_number_handler(3)))
        pag = PageNumberPaginator(total_path="total_pages", data_selector="items")
        with client:
            pages = list(client.paginate("/x", pag, data_selector="items"))
        assert _flatten(pages) == [{"p": 1}, {"p": 2}, {"p": 3}]


class TestPageNumberAsync:
    async def test_concurrent_when_total_known(self):
        client = AsyncRESTClient("https://t", transport=httpx.MockTransport(_page_number_handler(5)))
        pag = PageNumberPaginator(total_path="total_pages", data_selector="items")
        async with client:
            pages = [p async for p in client.paginate("/x", pag, data_selector="items")]
        # Order preserved across the concurrent fan-out.
        assert [r["p"] for r in _flatten(pages)] == [1, 2, 3, 4, 5]

    async def test_sequential_fallback_when_total_unknown(self):
        def handler(request: httpx.Request) -> httpx.Response:
            page = int(request.url.params.get("page", "1"))
            items = [{"p": page}] if page <= 3 else []
            return httpx.Response(200, json={"items": items})

        client = AsyncRESTClient("https://t", transport=httpx.MockTransport(handler))
        pag = PageNumberPaginator(data_selector="items")  # no total_path → sequential
        async with client:
            pages = [p async for p in client.paginate("/x", pag, data_selector="items")]
        assert _flatten(pages) == [{"p": 1}, {"p": 2}, {"p": 3}]

    async def test_range_pages_are_actually_overlapped(self):
        # Custom async transport that records peak concurrency.
        inflight = 0
        peak = 0

        class _Tracking(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                nonlocal inflight, peak
                inflight += 1
                peak = max(peak, inflight)
                await asyncio.sleep(0.02)
                inflight -= 1
                page = int(request.url.params.get("page", "1"))
                return httpx.Response(200, json={"total_pages": 6, "items": [{"p": page}]})

        client = AsyncRESTClient("https://t", transport=_Tracking())
        pag = PageNumberPaginator(total_path="total_pages", data_selector="items")
        async with client:
            pages = [p async for p in client.paginate("/x", pag, data_selector="items", concurrency=4)]
        assert [r["p"] for r in _flatten(pages)] == [1, 2, 3, 4, 5, 6]
        assert peak > 1  # pages 2..6 fetched concurrently


# -- Offset --------------------------------------------------------------------


class TestOffsetAsync:
    async def test_concurrent_offsets_with_total(self):
        def handler(request: httpx.Request) -> httpx.Response:
            offset = int(request.url.params.get("offset", "0"))
            return httpx.Response(200, json={"total": 10, "items": [{"o": offset}]})

        client = AsyncRESTClient("https://t", transport=httpx.MockTransport(handler))
        pag = OffsetPaginator(limit=4, total_path="total", data_selector="items")
        async with client:
            pages = [p async for p in client.paginate("/x", pag, data_selector="items")]
        # offsets 0, 4, 8 (8+4 >= 10 stops the range)
        assert [r["o"] for r in _flatten(pages)] == [0, 4, 8]


# -- Cursor / link (sequential) ------------------------------------------------


class TestJSONLinkAsync:
    async def test_follows_next_url(self):
        def handler(request: httpx.Request) -> httpx.Response:
            c = int(request.url.params.get("c", "0"))
            nxt = f"https://t/x?c={c + 1}" if c < 2 else None
            return httpx.Response(200, json={"items": [{"c": c}], "paging": {"next": nxt}})

        client = AsyncRESTClient("https://t", transport=httpx.MockTransport(handler))
        pag = JSONLinkPaginator(next_url_path="paging.next")
        async with client:
            pages = [p async for p in client.paginate("/x", pag, data_selector="items")]
        assert [r["c"] for r in _flatten(pages)] == [0, 1, 2]


class TestHeaderLinkAsync:
    async def test_follows_link_header(self):
        def handler(request: httpx.Request) -> httpx.Response:
            n = int(request.url.params.get("n", "0"))
            headers = {"Link": f'<https://t/x?n={n + 1}>; rel="next"'} if n < 2 else {}
            return httpx.Response(200, json={"items": [{"n": n}]}, headers=headers)

        client = AsyncRESTClient("https://t", transport=httpx.MockTransport(handler))
        pag = HeaderLinkPaginator()
        async with client:
            pages = [p async for p in client.paginate("/x", pag, data_selector="items")]
        assert [r["n"] for r in _flatten(pages)] == [0, 1, 2]


# -- Single page + selectors ---------------------------------------------------


class TestSinglePageAndSelectors:
    async def test_single_page(self):
        client = AsyncRESTClient(
            "https://t",
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"items": [{"a": 1}]})),
        )
        async with client:
            pages = [p async for p in client.paginate("/x", SinglePagePaginator(), data_selector="items")]
        assert pages == [[{"a": 1}]]

    async def test_callable_selector_can_validate_envelope(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"code": 0, "data": {"items": [{"a": 1}]}})

        def select_items(response: httpx.Response) -> list[dict]:
            body = response.json()
            assert body["code"] == 0
            return body["data"]["items"]

        client = AsyncRESTClient("https://t", transport=httpx.MockTransport(handler))
        async with client:
            pages = [p async for p in client.paginate("/x", SinglePagePaginator(), data_selector=select_items)]
        assert pages == [[{"a": 1}]]


# -- Path extraction and selectors ---------------------------------------------


class TestExtract:
    """Dotted-path lookup into a JSON body."""

    def test_walks_nested_keys(self):
        assert _extract({"a": {"b": {"c": 1}}}, "a.b.c") == 1

    def test_a_missing_key_with_a_default_returns_it(self):
        assert _extract({"a": {}}, "a.b", None) is None

    def test_a_missing_key_without_a_default_names_the_path(self):
        with pytest.raises(KeyError, match="path 'a.b' not found"):
            _extract({"a": {}}, "a.b")

    def test_a_non_dict_along_the_path_uses_the_default(self):
        assert _extract({"a": 1}, "a.b", "fallback") == "fallback"


class TestSelect:
    """How a page's records are pulled out of a response."""

    def test_no_selector_returns_the_whole_body(self):
        response = httpx.Response(200, json={"items": [{"a": 1}]})

        assert select(response, None) == {"items": [{"a": 1}]}

    def test_a_dotted_selector_reaches_into_the_body(self):
        response = httpx.Response(200, json={"data": {"items": [{"a": 1}]}})

        assert select(response, "data.items") == [{"a": 1}]

    def test_a_callable_selector_receives_the_response(self):
        response = httpx.Response(200, json={"items": [{"a": 1}]})

        assert select(response, lambda r: r.json()["items"]) == [{"a": 1}]


# -- Offset paginator ----------------------------------------------------------


def _offset_handler(total: int | None, page_size: int = 2) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        items = [{"o": offset + i} for i in range(page_size) if offset + i < (total if total is not None else 6)]
        body: dict[str, Any] = {"items": items}
        if total is not None:
            body["total"] = total
        return httpx.Response(200, json=body)

    return handler


class TestOffsetSync:
    """Sequential offset walking, and the stop conditions."""

    def test_stops_at_the_reported_total(self):
        client = RESTClient("https://t", transport=httpx.MockTransport(_offset_handler(4)))
        paginator = OffsetPaginator(limit=2, total_path="total", data_selector="items")
        with client:
            pages = list(client.paginate("/x", paginator, data_selector="items"))

        assert _flatten(pages) == [{"o": 0}, {"o": 1}, {"o": 2}, {"o": 3}]

    def test_stops_on_an_empty_page_when_the_total_is_unknown(self):
        client = RESTClient("https://t", transport=httpx.MockTransport(_offset_handler(None)))
        paginator = OffsetPaginator(limit=2, data_selector="items")
        with client:
            pages = list(client.paginate("/x", paginator, data_selector="items"))

        assert [row["o"] for row in _flatten(pages)] == [0, 1, 2, 3, 4, 5]

    def test_a_maximum_offset_bounds_an_unbounded_endpoint(self):
        client = RESTClient("https://t", transport=httpx.MockTransport(_offset_handler(None)))
        paginator = OffsetPaginator(limit=2, maximum_offset=4, data_selector="items")
        with client:
            pages = list(client.paginate("/x", paginator, data_selector="items"))

        assert [row["o"] for row in _flatten(pages)] == [0, 1, 2, 3]

    def test_remaining_requests_are_unknown_without_a_bound(self):
        # Nothing bounds the page set, so the async client cannot fan out.
        paginator = OffsetPaginator(limit=2, data_selector="items")
        request = httpx.Request("GET", "https://t/x")
        response = httpx.Response(200, json={"items": [{"o": 0}]})

        assert paginator.remaining_requests(request, response) is None


# -- Cursor paginator ----------------------------------------------------------


class TestCursor:
    """Opaque-cursor walking."""

    def test_follows_the_cursor_until_it_is_absent(self):
        cursors = {None: "c1", "c1": "c2", "c2": None}

        def handler(request: httpx.Request) -> httpx.Response:
            cursor = request.url.params.get("cursor")
            body: dict[str, Any] = {"items": [{"c": cursor or "first"}]}
            next_cursor = cursors[cursor]
            if next_cursor is not None:
                body["next"] = next_cursor
            return httpx.Response(200, json=body)

        client = RESTClient("https://t", transport=httpx.MockTransport(handler))
        paginator = JSONCursorPaginator(cursor_path="next", cursor_param="cursor")
        with client:
            pages = list(client.paginate("/x", paginator, data_selector="items"))

        assert [row["c"] for row in _flatten(pages)] == ["first", "c1", "c2"]

    def test_a_single_page_response_ends_the_walk(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"items": [{"c": 1}]})

        client = RESTClient("https://t", transport=httpx.MockTransport(handler))
        paginator = JSONCursorPaginator(cursor_path="next", cursor_param="cursor")
        with client:
            pages = list(client.paginate("/x", paginator, data_selector="items"))

        assert _flatten(pages) == [{"c": 1}]
        assert paginator.has_next is False
