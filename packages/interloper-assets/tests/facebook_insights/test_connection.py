"""Regression tests for the Facebook Insights connection.

Page and post insights only answer to a Page access token, which the
connection derives from its user token via ``GET /{page_id}?fields=access_token``.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.facebook_insights.connection import FacebookInsightsConnection

GRAPH = "https://graph.facebook.com/v26.0"


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> FacebookInsightsConnection:
    connection = FacebookInsightsConnection(client_id="cid", client_secret="secret", refresh_token="user-token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        GRAPH, auth=il.HTTPBearerAuth("user-token"), transport=httpx2.MockTransport(handler)
    )
    return connection


class TestPageClient:
    async def test_derives_the_page_token_from_the_user_token(self):
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(200, json={"access_token": "page-token", "id": "42"})

        connection = _connection(handler)
        client = await connection.page_client("42")
        assert seen[0].url.path == "/v26.0/42"
        assert seen[0].url.params["fields"] == "access_token"
        request = client.build_request("GET", "/42/insights")
        assert client.auth is not None
        next(client.auth.auth_flow(request))
        assert request.headers["Authorization"] == "Bearer page-token"
        assert str(request.url) == f"{GRAPH}/42/insights"

    async def test_raises_when_the_user_cannot_act_as_the_page(self):
        connection = _connection(lambda request: httpx2.Response(200, json={"id": "42"}))
        with pytest.raises(PermissionError):
            await connection.page_client("42")
