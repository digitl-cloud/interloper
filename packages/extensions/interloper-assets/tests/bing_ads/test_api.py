"""Tests for the Microsoft Advertising API engine.

Customer Management is spoken as raw SOAP, each call signed with an access
token exchanged for it; Reporting and Bulk go through the bingads SDK's
service managers, which the SDK builds per account. These tests fake HTTP and
the SDK's OAuth and managers, and pin what the engine sends and reads.
"""

from __future__ import annotations

import datetime as dt
import os
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx2
import pandas as pd
import pytest
from suds import WebFault

from interloper_assets.bing_ads.api import BingAdsApi

CUSTOMER = "https://bingads.microsoft.com/Customer/v13"
ENTITIES = "https://bingads.microsoft.com/Customer/v13/Entities"
SEARCH_ACCOUNTS_RESPONSE = f"""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><SearchAccountsResponse xmlns="{CUSTOMER}"><Accounts xmlns:a="{ENTITIES}"
 xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
<a:AdvertiserAccount><a:Id>111</a:Id><a:Name>One</a:Name><a:PauseReason i:nil="true"/>
<a:LinkedAgencies><a:CustomerInfo><a:Id>5</a:Id><a:Name>Agency</a:Name></a:CustomerInfo></a:LinkedAgencies>
</a:AdvertiserAccount>
<a:AdvertiserAccount><a:Id>112</a:Id><a:Name>Two</a:Name></a:AdvertiserAccount>
</Accounts></SearchAccountsResponse></s:Body></s:Envelope>"""
GET_ACCOUNT_RESPONSE = f"""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><GetAccountResponse xmlns="{CUSTOMER}"><Account xmlns:a="{ENTITIES}">
<a:Id>111</a:Id><a:ParentCustomerId>999</a:ParentCustomerId>
</Account></GetAccountResponse></s:Body></s:Envelope>"""


def _api() -> BingAdsApi:
    return BingAdsApi(client_id="cid", client_secret="secret", refresh_token="refresh", developer_token="dev")


def _web_fault(detail: SimpleNamespace) -> WebFault:
    return WebFault(SimpleNamespace(faultstring="Invalid client data.", detail=detail), document=None)


def _operation_error(code: str, message: str) -> SimpleNamespace:
    return SimpleNamespace(
        ApiFaultDetail=SimpleNamespace(
            BatchErrors="",
            OperationErrors=SimpleNamespace(OperationError=SimpleNamespace(ErrorCode=code, Message=message)),
        )
    )


@dataclass
class Manager:
    """A Reporting or Bulk service manager, writing the file the fake holds."""

    sdk: FakeSdk
    authorization_data: Any
    working_directory: str

    def download_file(self, download_parameters: Any) -> str | None:
        self.sdk.downloads.append(download_parameters)
        if self.sdk.fault is not None:
            raise self.sdk.fault
        if self.sdk.file is None:
            return None
        path = Path(download_parameters.result_file_directory) / download_parameters.result_file_name
        path.write_text(self.sdk.file, encoding="utf-8-sig")
        return str(path)


class FakeSdk:
    """The bingads SDK's OAuth and service managers, and HTTP for Customer Management."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Install the fakes for one test.

        Args:
            monkeypatch: Undoes every replacement when the test ends.
        """
        import bingads
        import bingads.v13.bulk as bulk_module
        import bingads.v13.reporting as reporting_module

        self.file: str | None = None
        self.fault: Exception | None = None
        self.managers: list[Manager] = []
        self.downloads: list[Any] = []
        self.soap: dict[str, httpx2.Response] = {}
        self.token = httpx2.Response(200, json={"access_token": "access"})
        self.sent: list[tuple[str, str, dict[str, str], str]] = []

        real_client = httpx2.AsyncClient

        def client(**kwargs: Any) -> httpx2.AsyncClient:
            return real_client(transport=httpx2.MockTransport(self._answer), **kwargs)

        def manager(authorization_data: Any, working_directory: str) -> Manager:
            self.managers.append(Manager(self, authorization_data, working_directory))
            return self.managers[-1]

        def grant(client_id: str, client_secret: str, redirection_uri: Any) -> Any:
            return SimpleNamespace(client_id=client_id, request_oauth_tokens_by_refresh_token=lambda token: token)

        monkeypatch.setattr(httpx2, "AsyncClient", client)
        monkeypatch.setattr(bingads, "OAuthWebAuthCodeGrant", grant)
        monkeypatch.setattr(bingads, "OAuthAuthorization", lambda client_id, oauth_tokens: (client_id, oauth_tokens))
        monkeypatch.setattr(reporting_module, "ReportingServiceManager", manager)
        monkeypatch.setattr(bulk_module, "BulkServiceManager", manager)

    def _answer(self, request: httpx2.Request) -> httpx2.Response:
        action = request.headers.get("SOAPAction", "")
        self.sent.append((str(request.url), action, dict(request.headers), request.content.decode()))
        if request.url.host == "login.microsoftonline.com":
            return self.token
        return self.soap[action]


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> FakeSdk:
    return FakeSdk(monkeypatch)


def _body(content: str) -> str:
    return content[content.index("<s:Body>") + len("<s:Body>") : content.index("</s:Body>")]


def _soap_message(report_request: Any) -> str:
    from bingads import ServiceClient

    method = ServiceClient("ReportingService", 13).soap_client.service.SubmitGenerateReport.method
    return str(method.binding.input.get_message(method, (report_request,), {}))


def _fresh_directories(*paths: str) -> bool:
    return all(os.path.isdir(path) and os.path.basename(path).startswith("bingads-") for path in paths)


async def _ad_report(api: BingAdsApi, **params: Any) -> pd.DataFrame:
    return await api.report(
        "111",
        dt.date(2026, 10, 29),
        "AdPerformanceReportRequest",
        scope="AccountThroughAdGroupReportScope",
        columns=[],
        **params,
    )


class TestCall:
    async def test_an_operation_is_signed_with_an_access_token_exchanged_for_it(self, sdk):
        sdk.soap["SearchAccounts"] = httpx2.Response(200, text=SEARCH_ACCOUNTS_RESPONSE)

        await _api().call("SearchAccounts", "AdvertiserAccount")

        (token_url, _, _, form), (url, action, headers, envelope) = sdk.sent
        assert token_url == "https://login.microsoftonline.com/common/oauth2/v2.0/token"
        assert dict(urllib.parse.parse_qsl(form)) == {
            "grant_type": "refresh_token",
            "refresh_token": "refresh",
            "client_id": "cid",
            "client_secret": "secret",
            "scope": "https://ads.microsoft.com/msads.manage offline_access",
        }
        assert (
            url
            == "https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc"
        )
        assert (action, headers["content-type"], headers["soapaction"]) == (
            "SearchAccounts",
            "text/xml; charset=utf-8",
            "SearchAccounts",
        )
        assert envelope == (
            '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:i="http://www.w3.org/2001/XMLSchema-instance">'
            f'<s:Header xmlns="{CUSTOMER}">'
            '<Action mustUnderstand="1">SearchAccounts</Action>'
            "<DeveloperToken>dev</DeveloperToken>"
            "<AuthenticationToken>access</AuthenticationToken>"
            "</s:Header>"
            f'<s:Body><SearchAccountsRequest xmlns="{CUSTOMER}"></SearchAccountsRequest></s:Body>'
            "</s:Envelope>"
        )

    async def test_parameters_are_encoded_as_customer_management_reads_them(self, sdk):
        sdk.soap["SearchAccounts"] = httpx2.Response(200, text=SEARCH_ACCOUNTS_RESPONSE)

        await _api().call(
            "SearchAccounts",
            "AdvertiserAccount",
            Predicates={"Predicate": [{"Field": "UserId", "Value": 42}, {"Field": "Name", "Value": "A & <B>"}]},
            Ordering=None,
            PageInfo={"Index": 0, "Size": 1000},
        )

        assert _body(sdk.sent[-1][3]) == (
            f'<SearchAccountsRequest xmlns="{CUSTOMER}">'
            f'<Predicates xmlns:a="{ENTITIES}">'
            "<a:Predicate><a:Field>UserId</a:Field><a:Value>42</a:Value></a:Predicate>"
            "<a:Predicate><a:Field>Name</a:Field><a:Value>A &amp; &lt;B&gt;</a:Value></a:Predicate>"
            "</Predicates>"
            '<Ordering i:nil="true"/>'
            f'<PageInfo xmlns:a="{ENTITIES}"><a:Index>0</a:Index><a:Size>1000</a:Size></PageInfo>'
            "</SearchAccountsRequest>"
        )

    async def test_booleans_and_instants_are_encoded_as_soap_reads_them(self, sdk):
        sdk.soap["GetThing"] = httpx2.Response(200, text="<Envelope/>")

        await _api().call(
            "GetThing",
            "Thing",
            Active=True,
            Deleted=False,
            Day=dt.date(2026, 11, 5),
            Since=dt.datetime(2026, 11, 5, 10, 30),
        )

        assert _body(sdk.sent[-1][3]) == (
            f'<GetThingRequest xmlns="{CUSTOMER}">'
            "<Active>true</Active><Deleted>false</Deleted>"
            "<Day>2026-11-05</Day><Since>2026-11-05T10:30:00</Since>"
            "</GetThingRequest>"
        )

    async def test_every_entity_of_the_answer_is_read_by_its_field_names(self, sdk):
        sdk.soap["SearchAccounts"] = httpx2.Response(200, text=SEARCH_ACCOUNTS_RESPONSE)

        assert await _api().call("SearchAccounts", "AdvertiserAccount") == [
            {
                "Id": "111",
                "Name": "One",
                "PauseReason": None,
                "LinkedAgencies": [{"Id": "5", "Name": "Agency"}],
            },
            {"Id": "112", "Name": "Two"},
        ]

    async def test_an_answer_without_the_entity_reads_as_no_entities(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(200, text="<Envelope><Body><GetAccountResponse/></Body></Envelope>")

        assert await _api().call("GetAccount", "Account", AccountId=111) == []

    async def test_a_refused_operation_raises_the_http_error(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(500, text="<s:Fault/>")

        with pytest.raises(httpx2.HTTPStatusError):
            await _api().call("GetAccount", "Account", AccountId=111)

    async def test_a_refused_token_exchange_sends_no_operation(self, sdk):
        sdk.token = httpx2.Response(400, json={"error": "invalid_grant"})

        with pytest.raises(httpx2.HTTPStatusError):
            await _api().call("GetAccount", "Account", AccountId=111)
        assert len(sdk.sent) == 1


REPORT_FILE = """\
CampaignId,TimePeriod,Impressions,Ctr,AllConversionRate
7,2026-10-29,"12,345",2.00%,
"""


class TestReport:
    async def test_the_request_covers_the_one_day_and_the_account(self, sdk):
        sdk.file = REPORT_FILE

        await _api().report(
            "111",
            dt.date(2026, 10, 29),
            "CampaignPerformanceReportRequest",
            scope="AccountThroughCampaignReportScope",
            scope_fields={"Campaigns": None},
            columns=["CampaignId", "TimePeriod", "Impressions"],
            Aggregation="Weekly",
            ReportName="Campaigns",
        )

        (manager,) = sdk.managers
        (download,) = sdk.downloads
        request = download.report_request
        assert (manager.authorization_data.account_id, manager.authorization_data.customer_id) == ("111", None)
        assert (manager.authorization_data.developer_token, manager.authorization_data.authentication) == (
            "dev",
            ("cid", "refresh"),
        )
        assert type(request).__name__ == "CampaignPerformanceReportRequest"
        assert type(request.Scope).__name__ == "AccountThroughCampaignReportScope"
        assert (request.Scope.AccountIds, request.Scope.Campaigns) == ({"long": ["111"]}, None)
        assert request.Columns.CampaignPerformanceReportColumn == [["CampaignId", "TimePeriod", "Impressions"]]
        assert (request.Aggregation, request.ReportName) == ("Weekly", "Campaigns")
        assert (request.Format, request.ExcludeColumnHeaders, request.ExcludeReportHeader) == ("Csv", False, True)
        assert request.ExcludeReportFooter is True
        for day in (request.Time.CustomDateRangeStart, request.Time.CustomDateRangeEnd):
            assert (day.Year, day.Month, day.Day) == (2026, 10, 29)
        assert request.Time.PredefinedTime is None
        assert request.Time.ReportTimeZone == "AmsterdamBerlinBernRomeStockholmVienna"
        assert (download.result_file_name, download.overwrite_result_file, download.timeout_in_milliseconds) == (
            "report.csv",
            True,
            3600000,
        )

    async def test_a_scope_of_the_account_alone_builds_a_valid_message(self, sdk):
        sdk.file = REPORT_FILE

        await _api().report(
            "111",
            dt.date(2026, 10, 29),
            "AccountPerformanceReportRequest",
            scope="AccountReportScope",
            columns=["AccountId", "TimePeriod"],
        )

        (download,) = sdk.downloads
        assert [name for name, _ in download.report_request.Scope] == ["AccountIds"]
        message = _soap_message(download.report_request)
        assert ":AccountIds>" in message and "Campaigns" not in message

    async def test_the_scope_keeps_the_factorys_defaults_unless_the_asset_sets_them(self, sdk):
        sdk.file = REPORT_FILE

        await _ad_report(_api())

        scope = sdk.downloads[0].report_request.Scope
        assert type(scope.Campaigns).__name__ == "ArrayOfCampaignReportScope"

    @pytest.mark.parametrize("field", ["Format", "Scope", "Time"])
    async def test_a_field_the_engine_sets_cannot_be_overridden(self, sdk, field: str):
        sdk.file = REPORT_FILE

        with pytest.raises(TypeError, match=field):
            await _ad_report(_api(), **{field: "other"})
        assert sdk.downloads == []

    async def test_the_scope_names_its_account_once(self, sdk):
        with pytest.raises(TypeError, match="AccountIds"):
            await _ad_report(_api(), scope_fields={"AccountIds": {"long": ["222"]}})
        assert sdk.downloads == []

    async def test_numbers_and_the_named_rates_are_read_as_numbers(self, sdk):
        sdk.file = REPORT_FILE

        frame = await _ad_report(_api(), percentages=["Ctr", "AllConversionRate", "TopImpressionRatePercent"])

        assert frame.astype(object).where(frame.notna(), None).to_dict("records") == [
            {
                "CampaignId": 7,
                "TimePeriod": "2026-10-29",
                "Impressions": 12345,
                "Ctr": "2.00",
                "AllConversionRate": None,
            }
        ]
        assert (str(frame["Ctr"].dtype), str(frame["AllConversionRate"].dtype)) == ("string", "string")

    async def test_a_rate_the_asset_does_not_name_is_read_as_written(self, sdk):
        sdk.file = REPORT_FILE

        frame = await _ad_report(_api())

        assert frame.loc[0, "Ctr"] == "2.00%"

    @pytest.mark.parametrize("file", [None, ""])
    async def test_a_report_without_rows_is_an_empty_frame(self, sdk, file: str | None):
        sdk.file = file

        frame = await _ad_report(_api())

        assert frame.empty and list(frame.columns) == []

    async def test_a_soap_fault_raises_its_operation_errors(self, sdk):
        sdk.fault = _web_fault(_operation_error("AccountNotAuthorized", "insufficient privileges"))

        error = r"^Bing Ads API error -- AccountNotAuthorized: insufficient privileges$"
        with pytest.raises(RuntimeError, match=error):
            await _ad_report(_api())

    async def test_every_batch_error_of_a_fault_is_named(self, sdk):
        sdk.fault = _web_fault(
            SimpleNamespace(
                ApiFaultDetail=SimpleNamespace(
                    OperationErrors="",
                    BatchErrors=SimpleNamespace(
                        BatchError=[
                            SimpleNamespace(Code="1", ErrorCode="A", Message="first"),
                            SimpleNamespace(Code="2", ErrorCode=None, Message="second"),
                        ]
                    ),
                )
            )
        )

        with pytest.raises(RuntimeError, match=r"^Bing Ads API error -- A: first; 2: second$"):
            await _ad_report(_api())

    async def test_a_fault_without_operation_errors_is_raised_as_is(self, sdk):
        sdk.fault = _web_fault(SimpleNamespace())

        with pytest.raises(WebFault):
            await _ad_report(_api())

    async def test_another_error_is_raised_as_is(self, sdk):
        sdk.fault = TimeoutError("report timed out")

        with pytest.raises(TimeoutError, match="report timed out"):
            await _ad_report(_api())

    async def test_each_report_gets_its_own_working_directory(self, sdk):
        sdk.file = REPORT_FILE
        api = _api()

        for _ in range(2):
            await _ad_report(api)

        first, second = (manager.working_directory for manager in sdk.managers)
        assert first != second
        assert _fresh_directories(first, second)


BULK_FILE = """\
Type,Status,Id,Parent Id,Campaign,Modified Time,Start Date,End Date,Name,Bid,Active
Format Version,,,,,,,,6.0,,
Account,,111,999,,11/04/2026 08:00:00,,,,,
Campaign,Active,555,111,Camp A,11/04/2026 09:15:02,10/01/2026 00:00:00,12/31/2026 00:00:00,,,TRUE
Campaign,Paused,556,111,Camp B,11/03/2026 10:00:00,,,,,false
"""


class TestBulk:
    async def test_the_download_is_scoped_to_the_account_and_its_parent_customer(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(200, text=GET_ACCOUNT_RESPONSE)
        sdk.file = BULK_FILE

        await _api().bulk("111", data_scope=["EntityData", "QualityScoreData"], download_entities=["Campaigns"])

        (_, (_, action, _, envelope)) = sdk.sent
        assert action == "GetAccount"
        assert (
            _body(envelope) == f'<GetAccountRequest xmlns="{CUSTOMER}"><AccountId>111</AccountId></GetAccountRequest>'
        )
        (manager,) = sdk.managers
        (download,) = sdk.downloads
        assert (manager.authorization_data.account_id, manager.authorization_data.customer_id) == ("111", "999")
        assert (download.result_file_name, download.overwrite_result_file, download.timeout_in_milliseconds) == (
            "entities.csv",
            True,
            3600000,
        )
        assert (download.data_scope, download.download_entities, download.file_type) == (
            ["EntityData", "QualityScoreData"],
            ["Campaigns"],
            "Csv",
        )

    async def test_the_records_are_read_without_the_file_rows_and_typed(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(200, text=GET_ACCOUNT_RESPONSE)
        sdk.file = BULK_FILE

        frame = await _api().bulk("111", data_scope=["EntityData"], download_entities=["Campaigns"])

        assert frame.astype(object).where(frame.notna(), None).to_dict("records") == [
            {
                "Type": "Campaign",
                "Status": "Active",
                "Id": "555",
                "Parent Id": "111",
                "Campaign": "Camp A",
                "Modified Time": pd.Timestamp("2026-11-04 09:15:02", tz="UTC"),
                "Start Date": dt.date(2026, 10, 1),
                "End Date": dt.date(2026, 12, 31),
                "Active": True,
            },
            {
                "Type": "Campaign",
                "Status": "Paused",
                "Id": "556",
                "Parent Id": "111",
                "Campaign": "Camp B",
                "Modified Time": pd.Timestamp("2026-11-03 10:00:00", tz="UTC"),
                "Start Date": None,
                "End Date": None,
                "Active": False,
            },
        ]
        assert list(frame.index) == [0, 1]

    async def test_each_download_gets_its_own_working_directory(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(200, text=GET_ACCOUNT_RESPONSE)
        sdk.file = BULK_FILE
        api = _api()

        for _ in range(2):
            await api.bulk("111", data_scope=["EntityData"], download_entities=["Ads"])

        first, second = (manager.working_directory for manager in sdk.managers)
        assert first != second
        assert _fresh_directories(first, second)

    async def test_a_soap_fault_raises_its_operation_errors(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(200, text=GET_ACCOUNT_RESPONSE)
        sdk.fault = _web_fault(_operation_error("CustomerIdHasToBeSpecified", "no customer"))

        with pytest.raises(RuntimeError, match=r"^Bing Ads API error -- CustomerIdHasToBeSpecified: no customer$"):
            await _api().bulk("111", data_scope=["EntityData"], download_entities=["Ads"])

    async def test_an_account_bing_does_not_return_downloads_nothing(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(200, text="<Envelope><Body><GetAccountResponse/></Body></Envelope>")

        with pytest.raises(RuntimeError, match=r"^Bing Ads returned no account for 111$"):
            await _api().bulk("111", data_scope=["EntityData"], download_entities=["Ads"])
        assert sdk.managers == []

    async def test_an_account_without_a_parent_customer_downloads_nothing(self, sdk):
        sdk.soap["GetAccount"] = httpx2.Response(
            200, text=GET_ACCOUNT_RESPONSE.replace("<a:ParentCustomerId>999</a:ParentCustomerId>", "")
        )

        with pytest.raises(RuntimeError, match=r"^Bing Ads account 111 names no parent customer$"):
            await _api().bulk("111", data_scope=["EntityData"], download_entities=["Ads"])
        assert sdk.managers == []

    async def test_a_non_numeric_account_is_refused_before_any_request(self, sdk):
        with pytest.raises(ValueError):
            await _api().bulk("1</AccountId>", data_scope=["EntityData"], download_entities=["Ads"])
        assert sdk.sent == []


class TestAuthentication:
    async def test_the_sdk_exchanges_the_refresh_token_once_per_engine(self, sdk, monkeypatch):
        import bingads

        grants: list[str] = []

        def grant(client_id: str, client_secret: str, redirection_uri: Any) -> Any:
            def exchange(token: str) -> str:
                grants.append(token)
                return "tokens"

            return SimpleNamespace(client_id=client_id, request_oauth_tokens_by_refresh_token=exchange)

        monkeypatch.setattr(bingads, "OAuthWebAuthCodeGrant", grant)
        sdk.file = REPORT_FILE
        api = _api()

        for _ in range(2):
            await api.report(
                "111",
                dt.date(2026, 10, 29),
                "AdPerformanceReportRequest",
                scope="AccountThroughAdGroupReportScope",
                columns=[],
            )

        assert grants == ["refresh"]
