"""Tests for the Adservice connection.

Adservice authenticates with HTTP Basic auth, the user ``api`` and the API key
as its password. The connection exposes that client as ``client`` and the
engine built on it as ``api``.
"""

from __future__ import annotations

import base64

import httpx2

from interloper_assets.adservice.connection import AdserviceConnection


def _connection() -> AdserviceConnection:
    return AdserviceConnection(api_key="secret-key")


class TestClient:
    def test_reads_the_client_api_of_adservice(self):
        assert str(_connection().client.base_url) == "https://api.adservice.com/v2/client/"

    def test_authenticates_as_api_with_the_key_as_password(self):
        client = _connection().client
        request = client.build_request("GET", "/statistics")

        assert isinstance(client.auth, httpx2.BasicAuth)
        authenticated = next(client.auth.sync_auth_flow(request))

        expected = base64.b64encode(b"api:secret-key").decode()
        assert authenticated.headers["Authorization"] == f"Basic {expected}"

    def test_is_cached_per_connection(self):
        connection = _connection()

        assert connection.client is connection.client


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert connection.api.client is connection.client
        assert connection.api is connection.api
