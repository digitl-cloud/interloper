"""Tests for The Trade Desk connection.

The API key travels in the ``TTD-Auth`` header, to The Trade Desk's own hosts
only, redirects included. The partner lookup backs the
source's ``partner_id`` field and the connection check, and The Trade Desk
refuses a bad key with HTTP 401, which the check reports as rejected
credentials.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from itertools import pairwise

import httpx2
import interloper as il
import pytest

from interloper_assets.thetradedesk.connection import TheTradeDeskConnection
from interloper_assets.thetradedesk.constants import BASE_URL

DELIVERY_URL = f"{BASE_URL}/myreports/delivery/report"
STORAGE_URL = "https://storage.example/report.csv?signature=s"


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

    @pytest.mark.parametrize(
        "hops",
        [
            [("https://delivery.example/report", None), (STORAGE_URL, None)],
            [("https://evilthetradedesk.com/report", None), (STORAGE_URL, None)],
            [(DELIVERY_URL, "api-key"), (STORAGE_URL, None)],
            [(DELIVERY_URL, "api-key"), ("https://files.thetradedesk.com/report.csv", "api-key")],
        ],
        ids=["foreign host", "lookalike domain", "own host to storage", "own host to own host"],
    )
    async def test_the_key_reaches_only_the_trade_desks_hosts_redirects_included(
        self, hops: list[tuple[str, str | None]], monkeypatch: pytest.MonkeyPatch
    ):
        execution = {"ReportExecutionState": "Complete", "ReportDeliveries": [{"DownloadURL": hops[0][0]}]}
        answers = {
            f"{BASE_URL}/myreports/reportschedule": httpx2.Response(200, json={"ReportScheduleId": 42}),
            f"{BASE_URL}/myreports/reportexecution/query/partners": httpx2.Response(200, json={"Result": [execution]}),
            f"{BASE_URL}/myreports/reportschedule/42": httpx2.Response(200),
        }
        for (url, _), (target, _) in pairwise(hops):
            answers[url] = httpx2.Response(302, headers={"Location": target})
        answers[hops[-1][0]] = httpx2.Response(200, text="Date,Impressions\n2026-10-01,3\n")
        requests: list[httpx2.Request] = []

        async def answer(_: httpx2.AsyncHTTPTransport, request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return answers[str(request.url)]

        # only the network is replaced: the connection's real client, its hooks and its redirect handling all run
        monkeypatch.setattr(httpx2.AsyncHTTPTransport, "handle_async_request", answer)

        rows = await TheTradeDeskConnection(api_key="api-key").api.report("p", "168005", dt.date(2026, 10, 1))

        assert rows == [{"Date": "2026-10-01", "Impressions": "3"}]
        assert [(str(request.url), request.headers.get("TTD-Auth")) for request in requests] == [
            (f"{BASE_URL}/myreports/reportschedule", "api-key"),
            (f"{BASE_URL}/myreports/reportexecution/query/partners", "api-key"),
            *hops,
            (f"{BASE_URL}/myreports/reportschedule/42", "api-key"),
        ]


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

    async def test_a_partner_with_a_null_name_is_labelled_by_its_id(self):
        connection = _connection(
            lambda _: httpx2.Response(200, json={"Result": [{"PartnerId": "p1", "PartnerName": None}]})
        )

        assert await connection.partners() == [{"partner_id": "p1", "name": "p1"}]


class TestCheck:
    async def test_a_working_key_passes(self):
        assert await _connection(_partners).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_a_refused_key_reports_rejected_credentials(self):
        connection = _connection(lambda _: httpx2.Response(401, json={"Message": "Authentication failed"}))

        assert await connection.verify(timeout=5) == il.CheckResult(
            ok=False, live=True, category="auth", message="The provider rejected the credentials."
        )
