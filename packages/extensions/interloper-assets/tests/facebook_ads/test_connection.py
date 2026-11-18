"""Tests for the Facebook Ads connection.

The connection exposes the Graph API client as ``client`` and the Marketing
API engine every asset reads through as ``api``, built on that client. Its
account lookup reads Graph over plain HTTP, the access token as its bearer.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import pytest

from interloper_assets.facebook_ads.api import FacebookGraphApi, FacebookMarketingApi
from interloper_assets.facebook_ads.connection import FacebookAdsConnection

TOKEN = "user-access-token"


def _connection() -> FacebookAdsConnection:
    return FacebookAdsConnection(id="fb", access_token=TOKEN, app_id="1", app_secret="x")


def _graph(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx2.Request], httpx2.Response]) -> None:
    real_client = httpx2.AsyncClient

    class MockClient(real_client):
        def __init__(self, **kwargs):
            super().__init__(transport=httpx2.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx2, "AsyncClient", MockClient)


class TestApi:
    def test_every_request_goes_through_the_graph_client(self):
        assert isinstance(_connection().client, FacebookGraphApi)

    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert isinstance(connection.api, FacebookMarketingApi)
        assert connection.api.client is connection.client


class TestAccounts:
    async def test_sends_the_token_as_its_bearer_never_in_the_url(self, monkeypatch):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [{"account_id": "1", "name": "Acme", "account_status": 1}]})

        _graph(monkeypatch, handler)

        assert await _connection().accounts() == [{"account_id": "1", "name": "Acme"}]
        (request,) = requests
        assert request.headers["Authorization"] == f"Bearer {TOKEN}"
        assert "access_token" not in request.url.params
        assert TOKEN not in str(request.url)

    async def test_a_refused_lookup_raises_without_the_token(self, monkeypatch):
        refusal = {"error": {"message": "Error validating access token", "type": "OAuthException", "code": 190}}
        _graph(monkeypatch, lambda _: httpx2.Response(400, json=refusal))

        with pytest.raises(httpx2.HTTPStatusError) as raised:
            await _connection().accounts()

        assert TOKEN not in str(raised.value)
