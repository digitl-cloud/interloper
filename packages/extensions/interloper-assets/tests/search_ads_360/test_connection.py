"""Regression tests for the Search Ads 360 connection.

The connection authenticates its REST client as a service account through the
OAuth2 JWT-bearer grant: each token exchange carries a freshly signed assertion,
and a 401 buys a new token once. These tests drive that flow against a faked
token endpoint and API.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import parse_qs

import httpx2
import interloper as il
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth import jwt

from interloper_assets.search_ads_360.connection import SearchAds360Connection
from interloper_assets.search_ads_360.constants import BASE_URL, SCOPES

CLIENT_EMAIL = "reporter@project.iam.gserviceaccount.com"


def _key_info() -> dict[str, str]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    return {"type": "service_account", "client_email": CLIENT_EMAIL, "private_key": pem, "private_key_id": "k1"}


def _connection() -> SearchAds360Connection:
    return SearchAds360Connection(service_account_key=json.dumps(_key_info()))


def _client(connection: SearchAds360Connection, handler: Any) -> il.AsyncRESTClient:
    return il.AsyncRESTClient(BASE_URL, auth=connection.client.auth, transport=httpx2.MockTransport(handler))


class TestClient:
    def test_targets_the_reporting_api(self):
        assert str(_connection().client.base_url).rstrip("/") == BASE_URL


class TestServiceAccountAuth:
    def _serve(self, api_statuses: list[int]) -> tuple[list[httpx2.Request], Any]:
        requests: list[httpx2.Request] = []
        statuses = list(api_statuses)
        tokens = iter(["token-1", "token-2"])

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            if request.url.host == "oauth2.googleapis.com":
                return httpx2.Response(200, json={"access_token": next(tokens), "expires_in": 3600})
            return httpx2.Response(statuses.pop(0), json={})

        return requests, handler

    def test_exchanges_a_signed_assertion_for_a_bearer_token(self):
        requests, handler = self._serve([200])
        connection = _connection()
        response = asyncio.run(_client(connection, handler).get("/customers/123"))
        assert response.status_code == 200

        token_request, api_request = requests
        assert str(token_request.url) == "https://oauth2.googleapis.com/token"
        form = parse_qs(token_request.content.decode())
        assert form["grant_type"] == ["urn:ietf:params:oauth:grant-type:jwt-bearer"]
        claims = jwt.decode(form["assertion"][0], verify=False)
        assert claims["iss"] == CLIENT_EMAIL
        assert claims["scope"] == " ".join(SCOPES)
        assert claims["aud"] == "https://oauth2.googleapis.com/token"
        assert claims["exp"] - claims["iat"] == 600
        assert api_request.headers["Authorization"] == "Bearer token-1"

    def test_reuses_the_token_across_requests(self):
        requests, handler = self._serve([200, 200])
        client = _client(_connection(), handler)

        async def two_calls() -> None:
            await client.get("/customers/1")
            await client.get("/customers/2")

        asyncio.run(two_calls())
        assert [r.url.host for r in requests].count("oauth2.googleapis.com") == 1

    def test_a_401_buys_a_new_token_once(self):
        requests, handler = self._serve([401, 200])
        response = asyncio.run(_client(_connection(), handler).get("/customers/123"))
        assert response.status_code == 200
        hosts = [r.url.host for r in requests]
        assert hosts.count("oauth2.googleapis.com") == 2
        assert requests[-1].headers["Authorization"] == "Bearer token-2"
