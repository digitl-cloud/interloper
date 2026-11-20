"""Tests for the Brandwatch (Falcon.io) connection.

The Measure API takes the key as the ``apikey`` query parameter on every
request, so the connection's client carries it as a default parameter and the
engine is built on that client. The check reads the channel metrics' metadata,
the cheapest call the key must be accepted for.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il

from interloper_assets.brandwatch import constants
from interloper_assets.brandwatch.connection import BrandwatchConnection


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> BrandwatchConnection:
    connection = BrandwatchConnection(api_key="key")
    connection.__dict__["client"] = il.AsyncRESTClient(
        constants.BASE_URL, params=dict(connection.client.params), transport=httpx2.MockTransport(handler)
    )
    return connection


class TestClient:
    def test_the_key_is_the_apikey_query_parameter_of_every_request(self):
        client = BrandwatchConnection(api_key="key").client

        assert str(client.base_url) == "https://api.falcon.io/measure/v2/"
        assert dict(client.params) == {"apikey": "key"}
        assert "authorization" not in client.headers
        assert client.timeout == httpx2.Timeout(60)

    def test_the_client_is_shared(self):
        connection = BrandwatchConnection(api_key="key")

        assert connection.client is connection.client


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = BrandwatchConnection(api_key="key")

        assert connection.api.client is connection.client

    def test_the_api_is_shared(self):
        connection = BrandwatchConnection(api_key="key")

        assert connection.api is connection.api


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert BrandwatchConnection(api_key="key").client.retry == il.HttpRetryPolicy()


class TestCheck:
    async def test_a_working_key_passes_by_reading_the_channel_metrics(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"data": [{"id": "channel/fans/day", "level": "CHANNEL"}]})

        assert await _connection(handler).verify(timeout=5) == il.CheckResult(ok=True, live=True)
        (request,) = requests
        assert (request.method, str(request.url)) == (
            "GET",
            "https://api.falcon.io/measure/v2/metrics/channel?apikey=key",
        )

    async def test_a_refused_key_fails_as_bad_credentials(self):
        def handler(_: httpx2.Request) -> httpx2.Response:
            fault = {"faultstring": "Invalid ApiKey", "detail": {"errorcode": "oauth.v2.InvalidApiKey"}}
            return httpx2.Response(401, json={"fault": fault})

        result = await _connection(handler).verify(timeout=5)

        assert (result.ok, result.live, result.category) == (False, True, "auth")
