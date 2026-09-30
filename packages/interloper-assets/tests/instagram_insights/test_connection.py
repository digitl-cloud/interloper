"""Regression tests for the Instagram Insights connection.

The ``accounts`` lookup walks the Facebook Pages the token administers along
their absolute ``paging.next`` links, and flattens each Page's connected
``instagram_business_account``; Pages without one are skipped.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.instagram_insights.connection import InstagramInsightsConnection

GRAPH = "https://graph.facebook.com/v26.0"


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> InstagramInsightsConnection:
    connection = InstagramInsightsConnection(client_id="cid", client_secret="secret", refresh_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        GRAPH, auth=il.HTTPBearerAuth("token"), transport=httpx2.MockTransport(handler)
    )
    return connection


class TestClient:
    def test_targets_the_pinned_graph_version_with_the_token(self):
        client = InstagramInsightsConnection(client_id="cid", client_secret="secret", refresh_token="token").client
        assert str(client.base_url).rstrip("/") == GRAPH
        request = client.build_request("GET", "/me")
        assert client.auth is not None
        next(client.auth.auth_flow(request))
        assert request.headers["Authorization"] == "Bearer token"


class TestAccounts:
    NEXT_LINK = f"{GRAPH}/me/accounts?limit=100&after=cursor-1"

    def _handler(self, seen: list[httpx2.URL]) -> Callable[[httpx2.Request], httpx2.Response]:
        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request.url)
            if len(seen) == 1:
                pages = [
                    {"id": "p1", "name": "Acme Page", "instagram_business_account": {"id": 11, "username": "acme"}},
                    {"id": "p2", "name": "No Instagram"},
                ]
                return httpx2.Response(200, json={"data": pages, "paging": {"next": self.NEXT_LINK}})
            pages = [
                {"id": "p3", "name": "Brand Page", "instagram_business_account": {"id": "12", "name": "Brand"}},
                {"id": "p4", "name": "Bare Page", "instagram_business_account": {"id": "13"}},
            ]
            return httpx2.Response(200, json={"data": pages})

        return handler

    async def test_flattens_the_business_account_of_every_page(self):
        seen: list[httpx2.URL] = []
        assert await _connection(self._handler(seen)).accounts() == [
            {"id": "11", "name": "acme"},
            {"id": "12", "name": "Brand"},
            {"id": "13", "name": "Bare Page"},
        ]
        assert seen[0].path == "/v26.0/me/accounts"
        assert dict(seen[0].params) == {
            "fields": "instagram_business_account{id,username,name},name",
            "access_token": "token",
            "limit": "100",
        }
        assert len(seen) == 2

    async def test_follows_the_next_link_with_its_cursor(self):
        seen: list[httpx2.URL] = []
        await _connection(self._handler(seen)).accounts()
        assert str(seen[1]) == self.NEXT_LINK

    async def test_check_runs_the_lookup(self):
        assert await _connection(lambda request: httpx2.Response(200, json={"data": []})).check() is True

    async def test_check_raises_on_a_rejected_token(self):
        connection = _connection(lambda request: httpx2.Response(401, json={"error": {"code": 190}}))
        with pytest.raises(httpx2.HTTPStatusError):
            await connection.check()
