"""Tests for the AdUp connection.

The connection exchanges its OAuth2 client credentials for an access token and
sends it as a bearer token on every request its engine makes.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il

from interloper_assets.adup.connection import AdupConnection
from interloper_assets.adup.constants import BASE_URL

TOKEN = {"access_token": "access-token", "token_type": "Bearer", "expires_in": 3600}


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> AdupConnection:
    connection = AdupConnection(client_id="client-id", client_secret="client-secret")
    connection.__dict__["client"] = il.AsyncRESTClient(
        BASE_URL, auth=connection.client.auth, transport=httpx2.MockTransport(handler)
    )
    return connection


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection(lambda _: httpx2.Response(200))

        assert connection.api.client is connection.client

    def test_the_api_is_built_once(self):
        connection = _connection(lambda _: httpx2.Response(200))

        assert connection.api is connection.api


class TestAuthentication:
    async def test_the_client_credentials_are_exchanged_for_a_bearer_token(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.path == "/oauth2/token":
                return httpx2.Response(200, json=TOKEN)
            return httpx2.Response(200, json={"id": "42"})

        assert await _connection(handler).api.get("/advertisers/me") == {"id": "42"}

        token_request, account_request = requests
        assert token_request.method == "POST"
        assert str(token_request.url) == "https://api.adup-tech.com/oauth2/token"
        assert dict(httpx2.QueryParams(token_request.content.decode())) == {
            "client_id": "client-id",
            "client_secret": "client-secret",
            "grant_type": "client_credentials",
        }
        assert account_request.headers["Authorization"] == "Bearer access-token"


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert AdupConnection(client_id="client-id", client_secret="client-secret").client.retry == il.HttpRetryPolicy()
