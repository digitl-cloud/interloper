"""Tests for the Teads connection.

The connection sends its API key, as is, in the ``Authorization`` header of
every API request, and builds the engine on that client.
"""

from __future__ import annotations

from interloper_assets.teads.connection import TeadsConnection
from interloper_assets.teads.constants import BASE_URL


class TestClient:
    def test_the_client_sends_the_api_key_as_is_and_asks_for_json(self):
        client = TeadsConnection(api_key="key").client

        request = client.build_request("POST", "/api/reports/run", json={})

        assert str(request.url) == f"{BASE_URL}/api/reports/run"
        assert request.headers["Authorization"] == "key"
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["Accept"] == "application/json"


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = TeadsConnection(api_key="key")

        assert connection.api.client is connection.client
