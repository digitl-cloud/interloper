"""Regression tests for the BingAds connection."""

from __future__ import annotations

import sys
from typing import Any

import httpx2
import pytest

from interloper_assets.bing_ads.connection import BingAdsConnection

CUSTOMER = "https://bingads.microsoft.com/Customer/v13"
ENTITIES = "https://bingads.microsoft.com/Customer/v13/Entities"
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


def _connection() -> BingAdsConnection:
    return BingAdsConnection(client_id="cid", client_secret="secret", refresh_token="token", developer_token="dev")


@pytest.fixture
def customer_management(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, str], list[httpx2.Request]]:
    answers = {"GetUser": GET_USER_RESPONSE, "SearchAccounts": SEARCH_ACCOUNTS_RESPONSE}
    sent: list[httpx2.Request] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(request)
        if request.url.host == "login.microsoftonline.com":
            return httpx2.Response(200, json={"access_token": "access"})
        return httpx2.Response(200, text=answers[request.headers["SOAPAction"]])

    real_client = httpx2.AsyncClient

    def client(**kwargs: Any) -> httpx2.AsyncClient:
        return real_client(transport=httpx2.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx2, "AsyncClient", client)
    return answers, sent


def _body(request: httpx2.Request) -> str:
    content = request.content.decode()
    return content[content.index("<s:Body>") + len("<s:Body>") : content.index("</s:Body>")]


class TestApi:
    def test_the_api_is_built_on_the_connections_credentials(self):
        api = _connection().api

        assert (api.client_id, api.client_secret, api.refresh_token, api.developer_token) == (
            "cid",
            "secret",
            "token",
            "dev",
        )

    def test_the_api_is_shared_by_every_caller(self):
        connection = _connection()

        assert connection.api is connection.api


class TestAccountsProvider:
    async def test_the_user_is_resolved_then_their_accounts_searched(self, customer_management):
        _, sent = customer_management

        options = await _connection().accounts()

        assert options == [{"account_id": "111", "customer_id": "999", "name": "Acct (X1)"}]
        soap = [request for request in sent if request.url.host != "login.microsoftonline.com"]
        assert [request.headers["SOAPAction"] for request in soap] == ["GetUser", "SearchAccounts"]
        get_user, search_accounts = (_body(request) for request in soap)
        assert get_user == f'<GetUserRequest xmlns="{CUSTOMER}"><UserId i:nil="true"/></GetUserRequest>'
        assert search_accounts == (
            f'<SearchAccountsRequest xmlns="{CUSTOMER}">'
            f'<Predicates xmlns:a="{ENTITIES}">'
            "<a:Predicate><a:Field>UserId</a:Field><a:Operator>Equals</a:Operator><a:Value>42</a:Value></a:Predicate>"
            "</Predicates>"
            '<Ordering i:nil="true"/>'
            f'<PageInfo xmlns:a="{ENTITIES}"><a:Index>0</a:Index><a:Size>1000</a:Size></PageInfo>'
            "</SearchAccountsRequest>"
        )

    async def test_each_lookup_carries_an_access_token_of_its_own(self, customer_management):
        _, sent = customer_management

        await _connection().accounts()

        assert [request.url.host for request in sent] == [
            "login.microsoftonline.com",
            "clientcenter.api.bingads.microsoft.com",
            "login.microsoftonline.com",
            "clientcenter.api.bingads.microsoft.com",
        ]

    async def test_no_user_lists_no_accounts(self, customer_management):
        answers, sent = customer_management
        answers["GetUser"] = "<Envelope><Body><GetUserResponse/></Body></Envelope>"

        assert await _connection().accounts() == []
        assert [request.headers.get("SOAPAction") for request in sent if "SOAPAction" in request.headers] == ["GetUser"]

    async def test_the_check_runs_the_lookup(self, customer_management):
        _, sent = customer_management

        assert await _connection().check() is True
        assert any(request.headers.get("SOAPAction") == "SearchAccounts" for request in sent)


class TestApiProcess:
    async def test_the_lookup_and_the_check_never_load_the_sdk(self, customer_management, monkeypatch):
        # the API process installs the connection classes but not the bingads SDK, nor suds with it
        for name in [name for name in sys.modules if name.split(".")[0] in ("bingads", "suds")]:
            monkeypatch.delitem(sys.modules, name)
        monkeypatch.setitem(sys.modules, "bingads", None)
        monkeypatch.setitem(sys.modules, "suds", None)
        connection = _connection()

        assert await connection.accounts() == [{"account_id": "111", "customer_id": "999", "name": "Acct (X1)"}]
        assert await connection.check() is True
