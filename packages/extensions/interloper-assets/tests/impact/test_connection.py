"""Tests for the Impact connection.

The connection authenticates as its account with HTTP Basic Auth (the
account SID and its auth token) under the account's own base URL, and lists
the account's programs (``/Campaigns``) for the source's ``program_id``
dropdown and the connection check.
"""

from __future__ import annotations

import base64
from collections.abc import Callable

import httpx2
import interloper as il
import pytest

from interloper_assets.impact.connection import ImpactConnection
from interloper_assets.impact.constants import BASE_URL

ACCOUNT = f"{BASE_URL}/Advertisers/SID"


def _connection(handler: Callable[[httpx2.Request], httpx2.Response]) -> ImpactConnection:
    connection = ImpactConnection(account_sid="SID", auth_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        ACCOUNT,
        auth=httpx2.BasicAuth(username="SID", password="token"),
        headers={"Accept": "application/json"},
        transport=httpx2.MockTransport(handler),
    )
    return connection


def _programs(request: httpx2.Request) -> httpx2.Response:
    pages = {"1": [{"Id": "1", "Name": "Alpha"}], "2": [{"Id": "2"}]}
    return httpx2.Response(200, json={"@numpages": "2", "Campaigns": pages[request.url.params["Page"]]})


class TestClient:
    def test_the_client_authenticates_as_the_account_under_its_url(self):
        client = ImpactConnection(account_sid="SID", auth_token="token").client

        assert isinstance(client.auth, httpx2.BasicAuth)
        request = next(client.auth.auth_flow(client.build_request("GET", "/Campaigns")))

        assert str(request.url) == f"{ACCOUNT}/Campaigns"
        assert request.headers["Accept"] == "application/json"
        assert request.headers["Authorization"] == f"Basic {base64.b64encode(b'SID:token').decode()}"


class TestApi:
    def test_the_api_is_built_on_the_client(self):
        connection = _connection(_programs)

        assert connection.api.client is connection.client


class TestPrograms:
    async def test_lists_every_page_of_the_accounts_programs(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return _programs(request)

        assert await _connection(handler).programs() == [{"Id": "1", "Name": "Alpha"}, {"Id": "2", "Name": "2"}]
        assert [(str(request.url.copy_with(query=None)), dict(request.url.params)) for request in requests] == [
            (f"{ACCOUNT}/Campaigns", {"Page": "1"}),
            (f"{ACCOUNT}/Campaigns", {"Page": "2"}),
        ]

    @pytest.mark.parametrize("program", [{"Id": "7"}, {"Id": "7", "Name": None}, {"Id": "7", "Name": ""}])
    async def test_a_program_without_a_name_is_labelled_by_its_id(self, program: dict[str, str | None]):
        connection = _connection(lambda _: httpx2.Response(200, json={"@numpages": "1", "Campaigns": [program]}))

        assert await connection.programs() == [{"Id": "7", "Name": "7"}]


class TestCheck:
    async def test_working_credentials_pass(self):
        assert await _connection(_programs).verify(timeout=5) == il.CheckResult(ok=True, live=True)

    async def test_rejected_credentials_fail(self):
        result = await _connection(lambda _: httpx2.Response(401)).verify(timeout=5)

        assert result == il.CheckResult(
            ok=False, live=True, category="auth", message="The provider rejected the credentials."
        )


class TestRetry:
    def test_the_client_repeats_requests_under_http_s_default_policy(self):
        assert ImpactConnection(account_sid="SID", auth_token="token").client.retry == il.HttpRetryPolicy()
