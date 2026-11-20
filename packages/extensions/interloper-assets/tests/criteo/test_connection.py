"""Tests for the Criteo connection.

The connection lists the advertisers its credentials see, through the same
``/advertisers/me`` endpoint as the advertisers asset, to back the source's
advertiser field and to prove the credentials in the connection check.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.criteo.connection import CriteoConnection
from interloper_assets.criteo.constants import BASE_URL


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> CriteoConnection:
    connection = CriteoConnection(client_id="cid", client_secret="x", refresh_token="x")
    connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
    return connection


def _advertisers(_: httpx2.Request) -> httpx2.Response:
    advertisers = [
        {"id": "2", "type": "Advertiser", "attributes": {"advertiserName": "beta"}},
        {"id": "3", "type": "Advertiser", "attributes": {"advertiserName": None}},
        {"id": "1", "type": "Advertiser", "attributes": {"advertiserName": "Alpha"}},
    ]
    return httpx2.Response(200, json={"data": advertisers, "errors": [], "warnings": []})


def _refused(_: httpx2.Request) -> httpx2.Response:
    return httpx2.Response(401, json={"errors": [{"title": "Unauthorized"}]})


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection(_advertisers)

        assert connection.api.client is connection.client


class TestAdvertisers:
    async def test_lists_the_advertisers_by_name(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _advertisers(request)

        assert await _connection(handler).advertisers() == [
            {"id": "3", "name": "3"},
            {"id": "1", "name": "Alpha"},
            {"id": "2", "name": "beta"},
        ]
        (request,) = requests
        assert (request.method, request.url.path) == ("GET", "/2026-07/advertisers/me")
        assert not request.url.params

    async def test_an_advertiser_without_attributes_is_named_by_its_id(self):
        def handler(_: httpx2.Request) -> httpx2.Response:
            advertiser = {"id": "4", "type": "Advertiser", "attributes": None}
            return httpx2.Response(200, json={"data": [advertiser], "errors": [], "warnings": []})

        assert await _connection(handler).advertisers() == [{"id": "4", "name": "4"}]

    async def test_a_refused_lookup_raises_the_http_error(self):
        with pytest.raises(httpx2.HTTPStatusError, match="401"):
            await _connection(_refused).advertisers()


class TestCheck:
    async def test_working_credentials_pass(self):
        assert await _connection(_advertisers).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_refused_credentials_fail(self):
        result = await _connection(_refused).verify(timeout=5)

        assert (result.ok, result.live) == (False, True)


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert (
            CriteoConnection(client_id="cid", client_secret="x", refresh_token="x").client.retry == il.HttpRetryPolicy()
        )
