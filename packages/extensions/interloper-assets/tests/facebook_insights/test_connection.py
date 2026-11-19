"""Regression tests for the Facebook Insights connection.

The connection's client carries the long-lived user token as its bearer, and
the ``pages`` lookup walks ``/me/accounts`` along its absolute ``paging.next``
links. Page and post insights only answer to a Page access token, which the
connection derives from its user token via ``GET /{page_id}?fields=access_token``
to open a Graph API engine acting as the Page.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest
from interloper.errors import ConnectionCheckError

from interloper_assets.facebook_insights.connection import FacebookInsightsConnection

GRAPH = "https://graph.facebook.com/v26.0"


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> FacebookInsightsConnection:
    connection = FacebookInsightsConnection(client_id="cid", client_secret="secret", refresh_token="user-token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        GRAPH, auth=il.HTTPBearerAuth("user-token"), transport=httpx2.MockTransport(handler)
    )
    return connection


class TestClient:
    def test_targets_the_pinned_graph_version_as_the_user(self):
        client = FacebookInsightsConnection(client_id="cid", client_secret="secret", refresh_token="user-token").client
        assert str(client.base_url).rstrip("/") == GRAPH
        request = client.build_request("GET", "/me")
        assert client.auth is not None
        next(client.auth.auth_flow(request))
        assert request.headers["Authorization"] == "Bearer user-token"


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection(lambda request: httpx2.Response(200, json={"data": []}))

        assert connection.api.client is connection.client


class TestPages:
    async def test_follows_the_next_link_across_pages(self):
        next_link = f"{GRAPH}/me/accounts?fields=id%2Cname&limit=100&after=cursor-1"
        seen: list[httpx2.URL] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request.url)
            if "after" not in request.url.params:
                return httpx2.Response(200, json={"data": [{"id": "1", "name": "Acme"}], "paging": {"next": next_link}})
            return httpx2.Response(200, json={"data": [{"id": "2"}], "paging": {}})

        assert await _connection(handler).pages() == [{"id": "1", "name": "Acme"}, {"id": "2", "name": "2"}]
        assert str(seen[0]) == f"{GRAPH}/me/accounts?fields=id%2Cname&limit=100"
        assert str(seen[1]) == next_link

    async def test_check_runs_the_lookup(self):
        assert await _connection(lambda request: httpx2.Response(200, json={"data": []})).check() is True

    async def test_a_rejected_token_fails_the_check_with_graphs_reason(self):
        expired = {
            "message": "Error validating access token: Session has expired",
            "type": "OAuthException",
            "code": 190,
        }
        connection = _connection(lambda request: httpx2.Response(400, json={"error": expired}))

        with pytest.raises(ConnectionCheckError) as raised:
            await connection.check()

        assert str(raised.value) == (
            "Graph refused the request (HTTP 400, code 190): Error validating access token: Session has expired"
        )


class TestPageApi:
    async def test_derives_the_page_token_from_the_user_token(self):
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(200, json={"access_token": "page-token", "id": "42"})

        async with _connection(handler).page_api("42") as page:
            assert str(seen[0].url) == f"{GRAPH}/42?fields=access_token"
            assert seen[0].headers["Authorization"] == "Bearer user-token"
            request = page.client.build_request("GET", "/42/insights")
            assert page.client.auth is not None
            next(page.client.auth.auth_flow(request))
            assert request.headers["Authorization"] == "Bearer page-token"
            assert str(request.url) == f"{GRAPH}/42/insights"
        assert page.client.is_closed

    async def test_raises_when_the_user_cannot_act_as_the_page(self):
        connection = _connection(lambda request: httpx2.Response(200, json={"id": "42"}))
        with pytest.raises(PermissionError, match="cannot obtain a Page access token for page 42"):
            async with connection.page_api("42"):
                pass
