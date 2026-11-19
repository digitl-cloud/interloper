"""Tests for the Amazon Ads connection.

The connection exchanges its refresh token at its region's token endpoint and
sends its client id on every call. Listing the profiles, which backs the
source's profile field and the connection check, goes through the engine as
the assets do, so it authenticates the same way they do.
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import parse_qsl

import httpx2
import interloper as il
import pytest

from interloper_assets.amazon_ads.connection import AmazonAdsAPILocation, AmazonAdsConnection

PROFILES = [
    {"profileId": 2, "countryCode": "FR", "accountInfo": {"id": "A2", "name": "Shop", "type": "seller"}},
    {"profileId": 1, "countryCode": "DE", "accountInfo": {"id": "A1", "name": "Shop", "type": "seller"}},
]


def _connection(handler: Callable[[httpx2.Request], httpx2.Response] | None = None) -> AmazonAdsConnection:
    connection = AmazonAdsConnection(client_id="cid", client_secret="secret", refresh_token="refresh", location="EU")
    if handler is not None:
        # the connection's own client, its token exchange included, answered by the handler
        connection.client._transport = httpx2.MockTransport(handler)
    return connection


def _amazon(requests: list[httpx2.Request], *, token: int = 200, profiles: int = 200) -> Callable:
    def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append(request)
        if request.url.path == "/auth/o2/token":
            return httpx2.Response(token, json={"access_token": "access"} if token == 200 else {"error": "invalid"})
        return httpx2.Response(profiles, json=PROFILES if profiles == 200 else {"code": "UNAUTHORIZED"})

    return handler


class TestClient:
    @pytest.mark.parametrize("location", list(AmazonAdsAPILocation))
    def test_calls_the_regions_api_with_its_client_id(self, location: AmazonAdsAPILocation):
        connection = AmazonAdsConnection(
            client_id="cid", client_secret="secret", refresh_token="refresh", location=location.value
        )

        client = connection.client

        assert str(client.base_url).rstrip("/") == location.api_url
        assert client.headers["Amazon-Advertising-API-ClientId"] == "cid"
        assert isinstance(client.auth, il.OAuth2RefreshTokenAuth)

    def test_the_api_is_built_on_the_client(self):
        connection = _connection()

        assert connection.api.client is connection.client


class TestProfiles:
    def test_lists_the_profiles_after_exchanging_the_refresh_token(self):
        requests: list[httpx2.Request] = []

        options = _connection(_amazon(requests)).profiles()

        assert options == [
            {"profile_id": "2", "name": "Shop (FR)", "account_id": "A2", "country_code": "FR"},
            {"profile_id": "1", "name": "Shop (DE)", "account_id": "A1", "country_code": "DE"},
        ]
        token, profiles = requests
        assert (token.method, str(token.url)) == ("POST", "https://api.amazon.co.uk/auth/o2/token")
        assert dict(parse_qsl(token.content.decode())) == {
            "grant_type": "refresh_token",
            "refresh_token": "refresh",
            "client_id": "cid",
            "client_secret": "secret",
        }
        assert (profiles.method, str(profiles.url)) == ("GET", "https://advertising-api-eu.amazon.com/v2/profiles")
        assert profiles.headers["Authorization"] == "Bearer access"
        assert profiles.headers["Amazon-Advertising-API-ClientId"] == "cid"
        assert "Amazon-Advertising-API-Scope" not in profiles.headers


class TestCheck:
    async def test_working_credentials_pass(self):
        assert await _connection(_amazon([])).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_refused_refresh_token_reports_the_providers_reason(self):
        result = await _connection(_amazon([], token=400)).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False,
            live=True,
            category="auth",
            message="The provider refused the access token request (HTTP 400, invalid): Bad Request",
        )

    async def test_a_refused_profile_listing_reports_the_credentials(self):
        result = await _connection(_amazon([], profiles=401)).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False, live=True, category="auth", message="The provider rejected the credentials."
        )
