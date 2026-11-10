"""Regression tests for the BingAds connection."""

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any
from xml.etree import ElementTree as ET

import httpx2
import pytest

from interloper_assets.bing_ads.connection import BingAdsConnection


class TestReportingServiceManagerWorkingDirectory:
    """Each manager must get its own working directory.

    The SDK defaults to a shared ``/tmp/BingAdsSDKPython`` created with a racy
    exists-then-makedirs, so concurrent report assets in one pod crash with
    ``FileExistsError`` (prod runs 7c88e1be / 1205e329).
    """

    def test_each_manager_gets_a_fresh_working_directory(self, monkeypatch):
        import bingads.v13.reporting.reporting_service_manager as rsm_module

        captured: list[dict[str, Any]] = []

        class FakeManager:
            def __init__(self, authorization_data: Any, **kwargs: Any):
                captured.append(kwargs)

        monkeypatch.setattr(rsm_module, "ReportingServiceManager", FakeManager)
        monkeypatch.setattr(
            BingAdsConnection, "authorization_data", lambda self, account_id, customer_id=None: SimpleNamespace()
        )

        connection = BingAdsConnection(
            client_id="cid",
            client_secret="secret",
            refresh_token="token",
            developer_token="dev",
        )
        connection.reporting_service_manager("123")
        connection.reporting_service_manager("456")

        dirs = [kwargs["working_directory"] for kwargs in captured]
        assert len(dirs) == 2
        assert dirs[0] != dirs[1]
        for path in dirs:
            assert os.path.isdir(path)


GET_ACCOUNT_RESPONSE = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><GetAccountResponse xmlns="https://bingads.microsoft.com/Customer/v13">
<Account xmlns:a="https://bingads.microsoft.com/Customer/v13/Entities"
 xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="a:AdvertiserAccount">
<a:CurrencyCode>EUR</a:CurrencyCode><a:Id>111</a:Id><a:Name>Acct</a:Name>
<a:ParentCustomerId>999</a:ParentCustomerId><a:PauseReason i:nil="true"/>
<a:TimeZone>AmsterdamBerlinBernRomeStockholmVienna</a:TimeZone>
<a:LinkedAgencies><a:CustomerInfo><a:Id>5</a:Id><a:Name>Agency</a:Name></a:CustomerInfo></a:LinkedAgencies>
</Account></GetAccountResponse></s:Body></s:Envelope>"""


def _connection() -> BingAdsConnection:
    return BingAdsConnection(client_id="cid", client_secret="secret", refresh_token="token", developer_token="dev")


@pytest.fixture
def soap(monkeypatch):
    calls: list[tuple[str, str]] = []

    def install(response: str | dict[str, str]) -> list[tuple[str, str]]:
        async def access_token(self, client: Any) -> str:
            return "access"

        async def soap_call(self, client: Any, access_token: str, action: str, body: str) -> ET.Element:
            calls.append((action, body))
            return ET.fromstring(response if isinstance(response, str) else response[action])

        monkeypatch.setattr(BingAdsConnection, "_access_token", access_token)
        monkeypatch.setattr(BingAdsConnection, "_soap_call", soap_call)
        return calls

    return install


class TestAccount:
    async def test_the_account_reads_every_field_by_its_bing_name(self, soap):
        calls = soap(GET_ACCOUNT_RESPONSE)
        account = await _connection().account("111")
        assert calls[0][0] == "GetAccount" and "<AccountId>111</AccountId>" in calls[0][1]
        assert account == {
            "CurrencyCode": "EUR",
            "Id": "111",
            "Name": "Acct",
            "ParentCustomerId": "999",
            "PauseReason": None,
            "TimeZone": "AmsterdamBerlinBernRomeStockholmVienna",
            "LinkedAgencies": [{"Id": "5", "Name": "Agency"}],
        }

    async def test_the_customer_is_the_accounts_parent(self, soap):
        soap(GET_ACCOUNT_RESPONSE)
        assert await _connection().customer_id("111") == "999"

    async def test_an_account_without_a_parent_customer_fails(self, soap):
        soap(GET_ACCOUNT_RESPONSE.replace("<a:ParentCustomerId>999</a:ParentCustomerId>", ""))
        with pytest.raises(RuntimeError, match="names no parent customer"):
            await _connection().customer_id("111")

    async def test_a_response_without_an_account_fails(self, soap):
        soap("<Envelope><Body><GetAccountResponse/></Body></Envelope>")
        with pytest.raises(RuntimeError, match="no account for 111"):
            await _connection().account("111")

    async def test_a_non_numeric_account_id_is_refused_before_any_call(self, soap):
        calls = soap(GET_ACCOUNT_RESPONSE)
        with pytest.raises(ValueError):
            await _connection().account("1</AccountId>")
        assert calls == []


SEARCH_ACCOUNTS_RESPONSE = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><SearchAccountsResponse xmlns="https://bingads.microsoft.com/Customer/v13">
<Accounts xmlns:a="https://bingads.microsoft.com/Customer/v13/Entities">
<a:AdvertiserAccount><a:Id>111</a:Id><a:Name>Acct</a:Name><a:Number>X1</a:Number>
<a:ParentCustomerId>999</a:ParentCustomerId></a:AdvertiserAccount>
</Accounts></SearchAccountsResponse></s:Body></s:Envelope>"""
GET_USER_RESPONSE = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><GetUserResponse xmlns="https://bingads.microsoft.com/Customer/v13">
<User xmlns:a="https://bingads.microsoft.com/Customer/v13/Entities"><a:Id>42</a:Id></User>
</GetUserResponse></s:Body></s:Envelope>"""


class TestAccessToken:
    async def test_the_refresh_token_is_exchanged_for_an_access_token(self):
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            return httpx2.Response(200, json={"access_token": "fresh"})

        async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as client:
            assert await _connection()._access_token(client) == "fresh"
        form = dict(item.split("=", 1) for item in requests[0].content.decode().split("&"))
        assert (form["grant_type"], form["refresh_token"], form["client_id"]) == ("refresh_token", "token", "cid")


class TestAccountsProvider:
    async def test_the_accounts_the_user_reaches_become_options_with_their_customer(self, soap):
        calls = soap({"GetUser": GET_USER_RESPONSE, "SearchAccounts": SEARCH_ACCOUNTS_RESPONSE})
        options = await _connection().accounts()
        assert [action for action, _ in calls] == ["GetUser", "SearchAccounts"]
        assert "<a:Value>42</a:Value>" in calls[1][1]
        assert options == [{"account_id": "111", "customer_id": "999", "name": "Acct (X1)"}]


class TestBulkServiceManager:
    def test_the_manager_carries_the_customer_and_its_own_working_directory(self, monkeypatch):
        import bingads.v13.bulk as bulk_module

        captured: list[tuple[Any, dict[str, Any]]] = []

        class FakeManager:
            def __init__(self, authorization_data: Any, **kwargs: Any):
                captured.append((authorization_data, kwargs))

        monkeypatch.setattr(bulk_module, "BulkServiceManager", FakeManager)
        connection = _connection()
        connection.__dict__["_authentication"] = SimpleNamespace()
        connection.bulk_service_manager("111", "999")
        connection.bulk_service_manager("111", "999")

        (first, first_kwargs), (_, second_kwargs) = captured
        assert (first.account_id, first.customer_id) == ("111", "999")
        assert first_kwargs["working_directory"] != second_kwargs["working_directory"]
