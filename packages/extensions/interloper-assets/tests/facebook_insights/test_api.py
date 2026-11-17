"""Tests for the Facebook Graph API engine.

Graph answers a refused request with an error status and an ``error`` object
carrying its own ``code``, pages an edge along the absolute ``paging.next``
link, and reads list parameters (``fields``, ``metric``) as comma-separated
names. These tests pin each against a mock transport.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.facebook_insights.api import FacebookInsightsApi
from interloper_assets.facebook_insights.constants import API_VERSION, BASE_URL

GRAPH = f"{BASE_URL}/{API_VERSION}"


def _api(handler: Callable[[httpx2.Request], httpx2.Response]) -> FacebookInsightsApi:
    return FacebookInsightsApi(il.AsyncRESTClient(GRAPH, transport=httpx2.MockTransport(handler)))


def _refused(status: int, code: int, message: str) -> httpx2.Response:
    return httpx2.Response(status, json={"error": {"message": message, "type": "OAuthException", "code": code}})


class TestGet:
    async def test_returns_the_one_object_it_reads(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"access_token": "page-token", "id": "42"})

        assert await _api(handler).get("/42", fields="access_token") == {"access_token": "page-token", "id": "42"}
        (request,) = requests
        assert str(request.url) == f"{GRAPH}/42?fields=access_token"

    async def test_list_parameters_are_sent_comma_separated(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"id": "42"})

        await _api(handler).get("/42", fields=["id", "name"], limit=100)

        assert list(requests[0].url.params.multi_items()) == [("fields", "id,name"), ("limit", "100")]

    async def test_a_refusal_raises_its_error_status(self):
        api = _api(lambda _: _refused(400, 190, "Error validating access token"))

        with pytest.raises(httpx2.HTTPStatusError, match="400 Bad Request") as raised:
            await api.get("/42", fields="access_token")

        assert FacebookInsightsApi.error_code(raised.value) == 190

    async def test_an_http_error_raises(self):
        api = _api(lambda _: httpx2.Response(502, text="<html>bad gateway</html>"))

        with pytest.raises(httpx2.HTTPStatusError, match="502 Bad Gateway"):
            await api.get("/42")


class TestRows:
    async def test_follows_the_next_link_across_pages(self):
        next_link = f"{GRAPH}/42/insights?metric=page_follows&after=c2"
        requests: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            # the paginator advances the same request object, so keep its URL at send time
            requests.append(str(request.url))
            if request.url.params.get("after") == "c2":
                return httpx2.Response(200, json={"data": [{"name": "b"}], "paging": {"previous": "p"}})
            return httpx2.Response(200, json={"data": [{"name": "a"}], "paging": {"next": next_link}})

        rows = await _api(handler).rows("/42/insights", metric=["page_follows"], period="day")

        assert rows == [{"name": "a"}, {"name": "b"}]
        assert requests == [f"{GRAPH}/42/insights?metric=page_follows&period=day", next_link]

    async def test_list_parameters_are_sent_comma_separated(self):
        requests: list[httpx2.Request] = []
        insights = "insights.metric(post_clicks,post_media_view){name,values,id}"

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": []})

        await _api(handler).rows("/42/published_posts", fields=["id", "message", insights], since="2026-03-19")

        assert list(requests[0].url.params.multi_items()) == [
            ("fields", f"id,message,{insights}"),
            ("since", "2026-03-19"),
        ]

    async def test_a_refusal_on_a_later_page_raises(self):
        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.params.get("after") == "c2":
                return _refused(400, 100, "(#100) Invalid parameter")
            return httpx2.Response(
                200, json={"data": [{"id": "1"}], "paging": {"next": f"{GRAPH}/me/accounts?after=c2"}}
            )

        with pytest.raises(httpx2.HTTPStatusError, match="400 Bad Request") as raised:
            await _api(handler).rows("/me/accounts")

        assert FacebookInsightsApi.error_code(raised.value) == 100

    async def test_an_http_error_raises(self):
        api = _api(lambda _: httpx2.Response(500, text="oops"))

        with pytest.raises(httpx2.HTTPStatusError, match="500 Internal Server Error"):
            await api.rows("/me/accounts")

    async def test_an_empty_edge_reads_one_page(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [], "paging": {"cursors": {"before": "b", "after": "a"}}})

        assert await _api(handler).rows("/42/stories") == []
        assert len(requests) == 1

    async def test_a_response_without_data_raises(self):
        api = _api(lambda _: httpx2.Response(200, json={"id": "42"}))

        with pytest.raises(KeyError, match="'data' not found"):
            await api.rows("/me/accounts")

    async def test_an_edge_without_paging_is_read_once(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            # an endpoint that does not page answers the same list forever; end a runaway walk
            if len(requests) > 3:
                return httpx2.Response(500, text="runaway walk")
            return httpx2.Response(200, json={"data": [{"id": "1"}]})

        assert await _api(handler).rows("/42_1001/insights", metric="post_media_view") == [{"id": "1"}]
        assert len(requests) == 1


class TestErrorCode:
    def _error(self, response: httpx2.Response) -> httpx2.HTTPStatusError:
        request = httpx2.Request("GET", f"{GRAPH}/42/insights")
        return httpx2.HTTPStatusError("refused", request=request, response=response)

    def test_reads_graphs_own_code(self):
        assert FacebookInsightsApi.error_code(self._error(_refused(400, 100, "(#100) Invalid parameter"))) == 100

    def test_a_body_that_is_not_json_has_no_code(self):
        assert FacebookInsightsApi.error_code(self._error(httpx2.Response(400, text="<html>bad</html>"))) is None

    def test_a_body_without_an_error_object_has_no_code(self):
        assert FacebookInsightsApi.error_code(self._error(httpx2.Response(400, json={"message": "bad"}))) is None
