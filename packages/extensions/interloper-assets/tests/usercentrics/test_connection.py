"""Tests for the Usercentrics connection: its client and the engine built on it."""

from __future__ import annotations

from interloper_assets.usercentrics.api import UsercentricsApi
from interloper_assets.usercentrics.connection import UsercentricsConnection
from interloper_assets.usercentrics.constants import BASE_URL


class TestClient:
    def test_the_client_sends_the_api_key_to_the_export_service(self):
        client = UsercentricsConnection(api_key="secret").client

        assert str(client.base_url).rstrip("/") == BASE_URL
        assert client.headers["X-API-Key"] == "secret"

    def test_the_client_is_shared(self):
        connection = UsercentricsConnection(api_key="secret")

        assert connection.client is connection.client


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = UsercentricsConnection(api_key="secret")

        assert isinstance(connection.api, UsercentricsApi)
        assert connection.api.client is connection.client

    def test_the_api_is_shared(self):
        connection = UsercentricsConnection(api_key="secret")

        assert connection.api is connection.api
