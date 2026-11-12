"""Tests for the TikTok Ads connection.

Listing advertisers sends the connector app's ``app_id`` and ``secret``
beside the access token, and TikTok answers a rejected request with HTTP 200
and a non-zero ``code``: the connection check reports TikTok's own
``message`` for it rather than the generic failure.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest
from interloper.errors import ConnectionCheckError

from interloper_assets.tiktok_ads.connection import TiktokAdsConnection
from interloper_assets.tiktok_ads.constants import BASE_URL

APP_MISMATCH = {"code": 40001, "message": "The app_id is inconsistent with the token's app information", "data": {}}


@pytest.fixture(autouse=True)
def app_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the connector app the advertiser lookup authenticates as.

    Args:
        monkeypatch: Sets the provider-scoped environment for the test only.
    """
    monkeypatch.setenv("INTERLOPER_TIKTOK_CLIENT_ID", "app-id")
    monkeypatch.setenv("INTERLOPER_TIKTOK_CLIENT_SECRET", "app-secret")


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> TiktokAdsConnection:
    connection = TiktokAdsConnection(access_token="access-token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        BASE_URL, headers={"Access-Token": "access-token"}, transport=httpx2.MockTransport(handler)
    )
    return connection


def _advertisers(_: httpx2.Request) -> httpx2.Response:
    advertisers = [
        {"advertiser_id": "2", "advertiser_name": "beta"},
        {"advertiser_id": "3", "advertiser_name": None},
        {"advertiser_id": "1", "advertiser_name": "Alpha"},
    ]
    return httpx2.Response(200, json={"code": 0, "message": "OK", "data": {"list": advertisers}})


class TestAdvertisers:
    async def test_lists_the_advertisers_by_name_as_the_connector_app(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _advertisers(request)

        assert await _connection(handler).advertisers() == [
            {"advertiser_id": "3", "name": "3"},
            {"advertiser_id": "1", "name": "Alpha"},
            {"advertiser_id": "2", "name": "beta"},
        ]
        (request,) = requests
        assert request.url.path.endswith("/oauth2/advertiser/get/")
        assert dict(request.url.params) == {"app_id": "app-id", "secret": "app-secret"}

    async def test_a_rejected_lookup_raises_tiktoks_message(self):
        connection = _connection(lambda _: httpx2.Response(200, json=APP_MISMATCH))

        with pytest.raises(ConnectionCheckError, match="The app_id is inconsistent with the token's app information"):
            await connection.advertisers()


class TestCheck:
    async def test_a_working_token_passes(self):
        assert await _connection(_advertisers).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_token_from_another_app_reports_tiktoks_message(self):
        result = await _connection(lambda _: httpx2.Response(200, json=APP_MISMATCH)).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False,
            live=True,
            category="error",
            message="TikTok API error 40001: The app_id is inconsistent with the token's app information",
        )
