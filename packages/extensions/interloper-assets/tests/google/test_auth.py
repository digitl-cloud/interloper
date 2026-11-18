"""Tests for the Google service account grant.

The grant authenticates a REST client as a service account through the OAuth2
JWT-bearer grant: each token exchange carries a freshly signed assertion, and a
401 buys a new token once. These tests drive that flow against a faked token
endpoint and API, on both the async and the sync client.
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import parse_qs

import httpx2
import interloper as il
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.auth import jwt

from interloper_assets.google.auth import ServiceAccountAuth

CLIENT_EMAIL = "reporter@project.iam.gserviceaccount.com"
SCOPES = ["https://www.googleapis.com/auth/one", "https://www.googleapis.com/auth/two"]


def _key_info() -> dict[str, str]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    return {"type": "service_account", "client_email": CLIENT_EMAIL, "private_key": pem, "private_key_id": "k1"}


def _serve(api_statuses: list[int]) -> tuple[list[httpx2.Request], Any]:
    requests: list[httpx2.Request] = []
    statuses = list(api_statuses)
    tokens = iter(["token-1", "token-2"])

    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        if request.url.host == "oauth2.googleapis.com":
            return httpx2.Response(200, json={"access_token": next(tokens), "expires_in": 3600})
        return httpx2.Response(statuses.pop(0), json={})

    return requests, handler


def _client(handler: Any) -> il.AsyncRESTClient:
    auth = ServiceAccountAuth(_key_info(), SCOPES)
    return il.AsyncRESTClient("https://api.example.com", auth=auth, transport=httpx2.MockTransport(handler))


class TestServiceAccountAuth:
    def test_exchanges_a_signed_assertion_for_a_bearer_token(self):
        requests, handler = _serve([200])
        response = asyncio.run(_client(handler).get("/items"))
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
        requests, handler = _serve([200, 200])
        client = _client(handler)

        async def two_calls() -> None:
            await client.get("/items/1")
            await client.get("/items/2")

        asyncio.run(two_calls())
        assert [r.url.host for r in requests].count("oauth2.googleapis.com") == 1

    def test_a_401_buys_a_new_token_once(self):
        requests, handler = _serve([401, 200])
        response = asyncio.run(_client(handler).get("/items"))
        assert response.status_code == 200
        hosts = [r.url.host for r in requests]
        assert hosts.count("oauth2.googleapis.com") == 2
        assert requests[-1].headers["Authorization"] == "Bearer token-2"

    def test_drives_the_sync_client_too(self):
        requests, handler = _serve([200])
        auth = ServiceAccountAuth(_key_info(), SCOPES)
        client = il.RESTClient("https://api.example.com", auth=auth, transport=httpx2.MockTransport(handler))

        assert client.get("/items").status_code == 200
        assert requests[-1].headers["Authorization"] == "Bearer token-1"
