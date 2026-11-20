"""Tests for the Facebook Ads connection.

The connection exposes the Graph API client as ``client`` and the Marketing
API engine every asset reads through as ``api``, built on that client. Its
account lookup reads Graph through the SDK-free ``GraphApi``, the access token
as its bearer.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest
from interloper.errors import ConnectionCheckError

from interloper_assets.facebook_ads.api import FacebookGraphApi, FacebookMarketingApi
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.meta import api as graph

TOKEN = "user-access-token"
GRAPH = "https://graph.facebook.com/v23.0"


def _connection() -> FacebookAdsConnection:
    return FacebookAdsConnection(id="fb", access_token=TOKEN, app_id="1", app_secret="x")


def _graph(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx2.Request], httpx2.Response]) -> None:
    class GraphClient(il.AsyncRESTClient):
        def __init__(self, base_url: str, **kwargs):
            super().__init__(base_url, transport=httpx2.MockTransport(handler), **kwargs)

    monkeypatch.setattr(il, "AsyncRESTClient", GraphClient)


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

    async def test_a_refused_lookup_raises_graphs_reason_without_the_token(self, monkeypatch):
        refusal = {"error": {"message": "Error validating access token", "type": "OAuthException", "code": 190}}
        _graph(monkeypatch, lambda _: httpx2.Response(400, json=refusal))

        with pytest.raises(ConnectionCheckError) as raised:
            await _connection().accounts()

        assert str(raised.value) == "Graph refused the request (HTTP 400, code 190): Error validating access token"
        assert TOKEN not in str(raised.value)

    async def test_reads_every_page_of_accounts(self, monkeypatch):
        next_link = f"{GRAPH}/me/adaccounts?fields=account_id%2Cname%2Caccount_status&limit=500&after=c2"
        requests: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(str(request.url))
            if request.url.params.get("after") == "c2":
                return httpx2.Response(200, json={"data": [{"account_id": "2", "name": "Beta", "account_status": 1}]})
            first = [{"account_id": "1", "name": "Acme", "account_status": 1}]
            return httpx2.Response(200, json={"data": first, "paging": {"next": next_link}})

        _graph(monkeypatch, handler)

        accounts = await _connection().accounts()

        assert accounts == [{"account_id": "1", "name": "Acme"}, {"account_id": "2", "name": "Beta"}]
        assert requests == [f"{GRAPH}/me/adaccounts?fields=account_id%2Cname%2Caccount_status&limit=500", next_link]

    async def test_a_throttled_lookup_is_waited_out(self, monkeypatch):
        slept: list[float] = []

        async def sleep(seconds: float) -> None:
            slept.append(seconds)

        monkeypatch.setattr(graph, "sleep", sleep)
        throttled = {"error": {"message": "User request limit reached", "code": 17}}
        replies = [httpx2.Response(400, json=throttled), httpx2.Response(200, json={"data": []})]
        _graph(monkeypatch, lambda _: replies.pop(0))

        assert await _connection().accounts() == []
        assert slept == [60]


class TestRetry:
    async def test_the_account_lookup_repeats_requests_under_http_s_default_policy(self, monkeypatch):
        policies: list[il.HttpRetryPolicy | None] = []

        class RecordingClient(il.AsyncRESTClient):
            def __init__(self, base_url: str, **kwargs):
                policies.append(kwargs.get("retry"))
                answer = httpx2.Response(200, json={"data": [{"account_id": "1", "name": "Acme", "account_status": 1}]})
                super().__init__(base_url, transport=httpx2.MockTransport(lambda _: answer), **kwargs)

        monkeypatch.setattr(il, "AsyncRESTClient", RecordingClient)

        await _connection().accounts()

        assert policies == [il.HttpRetryPolicy()]
