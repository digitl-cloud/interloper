"""Tests for the Brandwatch (Falcon.io) connection.

The Measure API takes the key as a query parameter on every request, so the
connection's client carries it as a default parameter and the engine is built
on that client.
"""

from __future__ import annotations

import httpx2

from interloper_assets.brandwatch import constants
from interloper_assets.brandwatch.connection import BrandwatchConnection


class TestClient:
    def test_the_key_is_a_query_parameter_of_every_request(self):
        client = BrandwatchConnection(api_key="key").client

        assert str(client.base_url) == f"{constants.BASE_URL}/"
        assert dict(client.params) == {"param_key": "key"}
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
