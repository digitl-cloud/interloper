"""Tests for the Instagram Graph API engine.

Graph refuses a request with an HTTP error status, wraps an edge's records in
``data``, pages an edge by the absolute ``paging.next`` link and takes list
parameters comma-separated. These tests pin each against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.instagram_insights.api import InstagramInsightsApi

GRAPH = "https://graph.facebook.com/v26.0"


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> InstagramInsightsApi:
    return InstagramInsightsApi(il.AsyncRESTClient(GRAPH, transport=httpx2.MockTransport(handler)))


def _refused(_: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(400, json={"error": {"message": "Invalid parameter", "code": 100}})


class TestGet:
    async def test_returns_the_node_as_graph_sends_it(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"id": "1784", "followers_count": 1500})

        assert await _api(handler).get("/1784", fields=["id", "followers_count"]) == {
            "id": "1784",
            "followers_count": 1500,
        }
        (request,) = requests
        assert (request.method, request.url.path) == ("GET", "/v26.0/1784")
        assert request.url.params.multi_items() == [("fields", "id,followers_count")]

    async def test_a_refusal_raises_the_http_error(self):
        with pytest.raises(httpx2.HTTPStatusError):
            await _api(_refused).get("/1784")


class TestRows:
    async def test_follows_the_next_link_and_concatenates_every_page(self):
        next_link = f"{GRAPH}/1784/media?fields=id&after=p2"
        urls: list[httpx2.URL] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(request.url)
            if request.url.params.get("after") == "p2":
                return httpx2.Response(200, json={"data": [{"id": "2"}]})
            return httpx2.Response(200, json={"data": [{"id": "1"}], "paging": {"next": next_link}})

        assert await _api(handler).rows("/1784/media", fields=["id"]) == [{"id": "1"}, {"id": "2"}]
        assert urls[0].params.multi_items() == [("fields", "id")]
        assert str(urls[1]) == next_link

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params.get("after") == "p2":
                return _refused(request)
            return httpx2.Response(
                200, json={"data": [{"id": "1"}], "paging": {"next": f"{GRAPH}/1784/media?after=p2"}}
            )

        with pytest.raises(httpx2.HTTPStatusError):
            await _api(handler).rows("/1784/media")

    async def test_a_next_link_is_followed_without_the_token_graph_copies_into_it(self):
        next_link = f"{GRAPH}/me/accounts?access_token=graph-token&limit=100&after=c2"
        requests: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(str(request.url))
            if request.url.params.get("after") == "c2":
                return httpx2.Response(200, json={"data": [{"id": "2"}]})
            return httpx2.Response(200, json={"data": [{"id": "1"}], "paging": {"next": next_link}})

        assert await _api(handler).rows("/me/accounts", limit=100) == [{"id": "1"}, {"id": "2"}]
        assert requests[1] == f"{GRAPH}/me/accounts?limit=100&after=c2"

    async def test_a_refusal_on_a_later_page_does_not_carry_the_token(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params.get("after") == "c2":
                return httpx2.Response(400, json={"error": {"message": "Invalid parameter", "code": 100}})
            next_link = f"{GRAPH}/me/accounts?access_token=graph-token&after=c2"
            return httpx2.Response(200, json={"data": [{"id": "1"}], "paging": {"next": next_link}})

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await _api(handler).rows("/me/accounts")

        assert "graph-token" not in str(raised.value)

    async def test_an_http_error_on_the_first_page_raises(self):
        with pytest.raises(httpx2.HTTPStatusError):
            await _api(_refused).rows("/1784/media")

    async def test_nothing_to_list_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [], "paging": {"cursors": {"before": "a", "after": "b"}}})

        assert await _api(handler).rows("/1784/stories") == []
        assert len(requests) == 1

    async def test_a_response_without_paging_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an edge that does not page answers the same records forever; end a runaway walk
            if len(requests) > 3:
                return _refused(request)
            return httpx2.Response(200, json={"data": [{"name": "likes"}]})

        assert await _api(handler).rows("/m1/insights", metric=["likes"]) == [{"name": "likes"}]
        assert len(requests) == 1

    async def test_a_page_without_data_raises(self):
        with pytest.raises(KeyError, match="'data' not found"):
            await _api(lambda _: httpx2.Response(200, json={"paging": {}})).rows("/me/accounts")

    async def test_scalar_and_string_parameters_are_sent_as_they_are(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": []})

        await _api(handler).rows("/me/accounts", fields="instagram_business_account{id,username,name},name", limit=100)

        assert requests[0].url.params.multi_items() == [
            ("fields", "instagram_business_account{id,username,name},name"),
            ("limit", "100"),
        ]


class TestReport:
    async def test_asks_an_insights_edge_for_one_day(self):
        requests: list[httpx2.Request] = []
        insight = {"name": "reach", "period": "day", "values": [{"value": 340}]}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [insight]})

        insights = await _api(handler).report(
            "/1784/insights",
            dt.date(2026, 9, 30),
            metric=["follower_count", "reach"],
            period="day",
        )

        assert insights == [insight]
        (request,) = requests
        assert request.url.path == "/v26.0/1784/insights"
        assert request.url.params.multi_items() == [
            ("metric", "follower_count,reach"),
            ("period", "day"),
            ("since", "2026-09-30"),
            ("until", "2026-10-01"),
        ]

    async def test_reads_one_response_never_the_next_window(self):
        # Graph pages an insights edge by time window: next asks the same query for the following days
        requests: list[httpx2.Request] = []
        day = dt.date(2026, 9, 30)

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            since = dt.date.fromisoformat(request.url.params["since"])
            following, after = since + dt.timedelta(days=1), since + dt.timedelta(days=2)
            next_window = f"{GRAPH}/1784/insights?metric=reach&since={following}&until={after}"
            paging = {"next": next_window} if since < day + dt.timedelta(days=3) else {}
            return httpx2.Response(200, json={"data": [{"name": str(since)}], "paging": paging})

        insights = await _api(handler).report("/1784/insights", day, metric="reach")

        assert insights == [{"name": "2026-09-30"}]
        assert len(requests) == 1
