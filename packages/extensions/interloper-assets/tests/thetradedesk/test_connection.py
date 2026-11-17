"""Tests for The Trade Desk connection.

The API key travels in the ``TTD-Auth`` header. The partner lookup backs the
source's ``partner_id`` field and the connection check, and The Trade Desk
refuses a bad key with HTTP 401, which the check reports as rejected
credentials.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx2
import interloper as il

from interloper_assets.thetradedesk.connection import TheTradeDeskConnection
from interloper_assets.thetradedesk.constants import BASE_URL


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> TheTradeDeskConnection:
    connection = TheTradeDeskConnection(api_key="api-key")
    connection.__dict__["client"] = il.AsyncRESTClient(BASE_URL, transport=httpx2.MockTransport(handler))
    return connection


def _partners(_: httpx2.Request) -> httpx2.Response:
    partners = [{"PartnerId": "p1", "PartnerName": "Partner One"}, {"PartnerId": "p2"}]
    return httpx2.Response(200, json={"Result": partners, "ResultCount": 2})


class TestClient:
    def test_the_key_travels_in_the_ttd_auth_header(self):
        client = TheTradeDeskConnection(api_key="api-key").client

        assert client.headers["TTD-Auth"] == "api-key"
        assert str(client.base_url) == f"{BASE_URL}/"
        assert client.timeout == httpx2.Timeout(60)


class TestPartners:
    async def test_lists_the_partners_the_key_can_see(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _partners(request)

        assert await _connection(handler).partners() == [
            {"partner_id": "p1", "name": "Partner One"},
            {"partner_id": "p2", "name": "p2"},
        ]
        (request,) = requests
        assert (request.method, str(request.url)) == ("POST", f"{BASE_URL}/partner/query")
        assert json.loads(request.content) == {"PageStartIndex": 0, "PageSize": 100}


class TestCheck:
    async def test_a_working_key_passes(self):
        assert await _connection(_partners).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_refused_key_reports_rejected_credentials(self):
        connection = _connection(lambda _: httpx2.Response(401, json={"Message": "Authentication failed"}))

        assert await connection.verify(timeout=5) == il.CheckResult(
            ok=False, live=True, category="auth", message="The provider rejected the credentials."
        )
