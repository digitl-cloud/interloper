"""Regression tests for the PinterestAds connection.

Pinterest's token endpoint authenticates the app with HTTP Basic; the
connection's client must send that header on the refresh grant, and the
``accounts`` provider must follow the ``bookmark`` cursor through that client.
"""

from __future__ import annotations

import base64
from urllib.parse import parse_qs

import httpx2
import interloper as il

from interloper_assets.pinterest_ads import constants
from interloper_assets.pinterest_ads.connection import PinterestAdsConnection, PinterestRefreshTokenAuth


def _connection() -> PinterestAdsConnection:
    return PinterestAdsConnection(client_id="cid", client_secret="secret", refresh_token="refresh")


class TestTokenRefresh:
    def test_client_uses_the_basic_auth_flow(self):
        assert isinstance(_connection().client.auth, PinterestRefreshTokenAuth)

    async def test_refresh_grant_sends_basic_credentials(self):
        token_requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path.endswith("/oauth/token"):
                token_requests.append(request)
                return httpx2.Response(200, json={"access_token": "access"})
            assert request.headers["Authorization"] == "Bearer access"
            return httpx2.Response(200, json={"id": "1"})

        connection = _connection()
        auth = connection.client.auth
        async with il.AsyncRESTClient(constants.BASE_URL, auth=auth, transport=httpx2.MockTransport(handler)) as client:
            response = await client.get("/ad_accounts/1")
        assert response.json() == {"id": "1"}

        (token_request,) = token_requests
        expected = base64.b64encode(b"cid:secret").decode()
        assert token_request.headers["Authorization"] == f"Basic {expected}"
        form = parse_qs(token_request.content.decode())
        assert form["grant_type"] == ["refresh_token"]
        assert form["refresh_token"] == ["refresh"]


class TestAccounts:
    async def test_accounts_follow_the_bookmark(self):
        pages = {
            None: {"items": [{"id": "1", "name": "First"}], "bookmark": "next"},
            "next": {"items": [{"id": "2"}], "bookmark": None},
        }
        seen: list[dict[str, str]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            params = dict(request.url.params)
            seen.append(params)
            return httpx2.Response(200, json=pages[params.get("bookmark")])

        connection = _connection()
        connection.__dict__["client"] = il.AsyncRESTClient(constants.BASE_URL, transport=httpx2.MockTransport(handler))

        assert await connection.accounts() == [{"id": "1", "name": "First"}, {"id": "2", "name": "2"}]
        assert [params.get("bookmark") for params in seen] == [None, "next"]
        assert all(params["page_size"] == str(constants.PAGE_SIZE) for params in seen)
