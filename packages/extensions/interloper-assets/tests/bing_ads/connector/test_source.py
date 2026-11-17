"""Regression tests for the BingAds source configuration.

The Bing Ads report comes back with PascalCase CSV headers
(``AccountName``, ``TimePeriod``, ``Ctr``, …) while the ``Ads``
schema is snake_case. A source-level ``DataFrameNormalizer`` bridges the
two — these tests pin that the normalizer reaches every asset instance,
survives the host→child spec round-trip, and that a raw API-shaped row
normalizes and validates against the schema (the same chain that broke
in prod for AmazonAds; see ``amazon_ads/test_source.py``).
"""

from __future__ import annotations

import datetime as dt
import inspect
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx2
import interloper as il
import pandas as pd
import pytest
from bingads import ServiceClient
from interloper.asset import Asset
from interloper.asset.context import ExecutionContext
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer
from suds import WebFault
from suds.sudsobject import Object as SudsObject

from interloper_assets.bing_ads import constants
from interloper_assets.bing_ads.connection import BingAdsConnection
from interloper_assets.bing_ads.connector import schemas
from interloper_assets.bing_ads.connector.schemas import AdsStats
from interloper_assets.bing_ads.connector.source import BingAds

ENTITY_ASSETS = ("accounts", "campaigns", "ad_groups", "ads")
DAY = dt.date(2026, 11, 5)
AD_GROUPS_FILE = """\
Type,Status,Id,Parent Id,Campaign,Ad Group,Modified Time,Start Date,End Date,Cpc Bid,Use Optimized Targeting,Name,Bid
Format Version,,,,,,,,,,,6.0,
Account,,111,999,,,11/04/2026 08:00:00,,,,,,
Ad Group,Active,9007199254740993,555,Camp,Group A,11/04/2026 09:15:02.123,10/01/2026,12/31/2026,0.45,True,,
Ad Group,Paused,777,555,Camp,Group B,11/03/2026 10:00:00,10/01/2026,,0.3,FALSE,,
"""
CAMPAIGNS_FILE = """\
Type,Status,Id,Parent Id,Campaign,Modified Time,Start Date,End Date
Format Version,,,,,,,
Account,,111,999,,11/04/2026 08:00:00,,
Campaign,Active,555,111,Camp A,11/04/2026 09:15:02,10/01/2026 00:00:00,12/31/2026 00:00:00
Campaign,Paused,556,111,Camp B,11/03/2026 10:00:00,,
"""
ADS_FILE = """\
Type,Status,Id,Parent Id,Headline,Title Part 1
Format Version,,,,,
Account,,111,999,,
Responsive Search Ad,Active,1,9,"[{""text"":""Shop""}]",
Expanded Text Ad,Paused,2,9,,Title
Some Future Ad,Active,3,9,,
"""


def _source() -> Any:
    return BingAds(
        id="src-1",
        account_id="123",
        connection=BingAdsConnection(client_id="x", client_secret="x", refresh_token="x", developer_token="x"),
    )


def _connected_source() -> Any:
    connection = BingAdsConnection(
        client_id="cid", client_secret="secret", refresh_token="refresh", developer_token="dev"
    )
    return BingAds(id="src-1", account_id="111", connection=connection)


def _asset(source: Any, key: str) -> Any:
    return next(a for a in source.assets if type(a).key == key)


async def _run(source: Any, key: str) -> Any:
    asset = _asset(source, key)
    data = asset.data(context=ExecutionContext(asset, il.TimePartition(value=DAY).window))
    return await data if inspect.isawaitable(data) else data


def _reconcile(source: Any, key: str, data: Any) -> list[dict[str, Any]]:
    asset = _asset(source, key)
    reconciled = TableData.from_data(asset.normalizer.normalize(data)).reconcile(asset.schema)
    return reconciled.astype(object).where(reconciled.notna(), None).to_dict("records")


TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
CUSTOMER_MANAGEMENT_URL = (
    "https://clientcenter.api.bingads.microsoft.com/Api/CustomerManagement/v13/CustomerManagementService.svc"
)
ENVELOPE = (
    '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
    'xmlns:i="http://www.w3.org/2001/XMLSchema-instance">'
    '<s:Header xmlns="https://bingads.microsoft.com/Customer/v13">'
    '<Action mustUnderstand="1">{action}</Action>'
    "<DeveloperToken>dev</DeveloperToken>"
    "<AuthenticationToken>access</AuthenticationToken>"
    "</s:Header>"
    "<s:Body>{body}</s:Body>"
    "</s:Envelope>"
)
GET_ACCOUNT_RESPONSE = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
<s:Body><GetAccountResponse xmlns="https://bingads.microsoft.com/Customer/v13">
<Account xmlns:a="https://bingads.microsoft.com/Customer/v13/Entities"
 xmlns:i="http://www.w3.org/2001/XMLSchema-instance" i:type="a:AdvertiserAccount">
<a:BusinessAddress><a:City>Hamburg</a:City></a:BusinessAddress>
<a:CurrencyCode>EUR</a:CurrencyCode><a:Id>111</a:Id><a:Name>Acct</a:Name>
<a:ParentCustomerId>999</a:ParentCustomerId><a:PauseReason i:nil="true"/>
<a:TimeZone>AmsterdamBerlinBernRomeStockholmVienna</a:TimeZone>
<a:LinkedAgencies><a:CustomerInfo><a:Id>5</a:Id><a:Name>Agency</a:Name></a:CustomerInfo></a:LinkedAgencies>
</Account></GetAccountResponse></s:Body></s:Envelope>"""
REPORT_FILE = """\
AccountId,TimePeriod,AdId,Impressions,Clicks,Ctr,Spend,ConversionRate
111,2026-11-05,1,"1,234",10,1.50%,12.5,0.00%
"""
REPORT_DTYPES = {
    "AccountId": "int64",
    "TimePeriod": "str",
    "AdId": "int64",
    "Impressions": "int64",
    "Clicks": "int64",
    "Ctr": "string",
    "Spend": "float64",
    "ConversionRate": "string",
}


@dataclass
class Sent:
    """One HTTP request as it left the source."""

    method: str
    url: str
    content_type: str | None
    action: str | None
    content: str


@dataclass
class Manager:
    """A Reporting or Bulk service manager, writing the file the fake holds for its service."""

    bing: FakeBing
    service: str
    authorization_data: Any
    working_directory: str

    def download_file(self, download_parameters: Any) -> str | None:
        self.bing.downloads.append(download_parameters)
        if self.bing.fault is not None:
            raise self.bing.fault
        content = self.bing.report_file if self.service == "Reporting" else self.bing.bulk_file
        if content is None:
            return None
        path = Path(download_parameters.result_file_directory) / download_parameters.result_file_name
        path.write_text(content, encoding="utf-8-sig")
        return str(path)


class FakeBing:
    """Bing behind the seams the source reaches it through: HTTP, the SDK's OAuth and its service managers."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Install the fakes for one test.

        Args:
            monkeypatch: Undoes every replacement when the test ends.
        """
        import bingads
        import bingads.v13.bulk as bulk_module
        import bingads.v13.reporting as reporting_module

        self.soap = {"GetAccount": GET_ACCOUNT_RESPONSE}
        self.report_file: str | None = REPORT_FILE
        self.bulk_file = AD_GROUPS_FILE
        self.fault: Exception | None = None
        self.sent: list[Sent] = []
        self.client_options: list[dict[str, Any]] = []
        self.grants: list[tuple[str, ...]] = []
        self.managers: list[Manager] = []
        self.downloads: list[Any] = []

        real_client = httpx2.AsyncClient

        def client(**kwargs: Any) -> httpx2.AsyncClient:
            self.client_options.append(kwargs)
            return real_client(transport=httpx2.MockTransport(self._answer), **kwargs)

        def manager(service: str) -> Any:
            def build(authorization_data: Any, working_directory: str) -> Manager:
                self.managers.append(Manager(self, service, authorization_data, working_directory))
                return self.managers[-1]

            return build

        monkeypatch.setattr(httpx2, "AsyncClient", client)
        monkeypatch.setattr(bingads, "OAuthWebAuthCodeGrant", self._grant)
        monkeypatch.setattr(bingads, "OAuthAuthorization", lambda client_id, oauth_tokens: (client_id, oauth_tokens))
        monkeypatch.setattr(reporting_module, "ReportingServiceManager", manager("Reporting"))
        monkeypatch.setattr(bulk_module, "BulkServiceManager", manager("Bulk"))

    def _grant(self, client_id: str, client_secret: str, redirection_uri: Any) -> Any:
        def request_oauth_tokens_by_refresh_token(refresh_token: str) -> str:
            self.grants.append((client_id, client_secret, str(redirection_uri), refresh_token))
            return "sdk-tokens"

        return SimpleNamespace(
            client_id=client_id, request_oauth_tokens_by_refresh_token=request_oauth_tokens_by_refresh_token
        )

    def _answer(self, request: httpx2.Request) -> httpx2.Response:
        action = request.headers.get("SOAPAction")
        self.sent.append(
            Sent(
                request.method, str(request.url), request.headers.get("Content-Type"), action, request.content.decode()
            )
        )
        if str(request.url) == TOKEN_URL:
            return httpx2.Response(200, json={"access_token": "access"})
        return httpx2.Response(200, text=self.soap[request.headers["SOAPAction"]])


@pytest.fixture
def bing(monkeypatch: pytest.MonkeyPatch) -> FakeBing:
    return FakeBing(monkeypatch)


def _plain(value: Any) -> Any:
    """Read a suds object as nested dicts, so a whole request compares at once.

    Args:
        value: A suds object, a list, or a plain value.

    Returns:
        The value, each suds object a dict of its fields under a ``type`` naming its SOAP type.
    """
    if isinstance(value, SudsObject):
        return {"type": type(value).__name__, **{name: _plain(item) for name, item in value}}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _token_request(sent: Sent) -> dict[str, str]:
    assert (sent.method, sent.url) == ("POST", TOKEN_URL)
    return dict(urllib.parse.parse_qsl(sent.content))


def _soap(sent: Sent, action: str, body: str) -> None:
    assert (sent.method, sent.url, sent.content_type, sent.action) == (
        "POST",
        CUSTOMER_MANAGEMENT_URL,
        "text/xml; charset=utf-8",
        action,
    )
    assert sent.content == ENVELOPE.format(action=action, body=body)


GET_ACCOUNT_BODY = '<GetAccountRequest xmlns="https://bingads.microsoft.com/Customer/v13"><AccountId>111</AccountId></GetAccountRequest>'
TOKEN_FORM = {
    "grant_type": "refresh_token",
    "refresh_token": "refresh",
    "client_id": "cid",
    "client_secret": "secret",
    "scope": "https://ads.microsoft.com/msads.manage offline_access",
}


class TestRequests:
    """What each asset sends Bing and what it returns, faked where the source reaches Bing."""

    async def test_the_ad_report_asks_for_the_days_ad_performance_in_csv(self, bing):
        await _run(_connected_source(), "ads_stats")

        (manager,) = bing.managers
        (download,) = bing.downloads
        assert manager.service == "Reporting"
        assert (manager.authorization_data.account_id, manager.authorization_data.customer_id) == ("111", None)
        assert manager.authorization_data.developer_token == "dev"
        assert manager.authorization_data.authentication == ("cid", "sdk-tokens")
        assert bing.grants == [("cid", "secret", "None", "refresh")]
        assert Path(manager.working_directory).name.startswith("bingads-")
        assert {name: value for name, value in vars(download).items() if name != "_result_file_directory"} == {
            "_report_request": download.report_request,
            "_result_file_name": "report.csv",
            "_decompress_result_file": True,
            "_overwrite_result_file": True,
            "_timeout_in_milliseconds": 3600000,
        }
        day = {"type": "Date", "Day": 5, "Month": 11, "Year": 2026}
        assert _plain(download.report_request) == {
            "type": "AdPerformanceReportRequest",
            "ExcludeColumnHeaders": False,
            "ExcludeReportFooter": True,
            "ExcludeReportHeader": True,
            "Format": "Csv",
            "FormatVersion": None,
            "ReportName": "Ad Performance Report",
            "ReturnOnlyCompleteData": False,
            "Aggregation": "Daily",
            "Columns": {
                "type": "ArrayOfAdPerformanceReportColumn",
                "AdPerformanceReportColumn": [constants.AD_PERFORMANCE_FIELDS],
            },
            "Filter": _plain(ServiceClient("ReportingService", 13).factory.create("AdPerformanceReportFilter")),
            "Scope": {
                "type": "AccountThroughAdGroupReportScope",
                "AccountIds": {"long": ["111"]},
                "AdGroups": {"type": "ArrayOfAdGroupReportScope", "AdGroupReportScope": []},
                "Campaigns": None,
            },
            "Time": {
                "type": "ReportTime",
                "CustomDateRangeEnd": day,
                "CustomDateRangeStart": day,
                "PredefinedTime": None,
                "ReportTimeZone": "AmsterdamBerlinBernRomeStockholmVienna",
            },
        }
        assert bing.sent == []

    async def test_the_ad_report_reads_numbers_and_rates_as_numbers(self, bing):
        frame = await _run(_connected_source(), "ads_stats")

        assert frame.to_dict("records") == [
            {
                "AccountId": 111,
                "TimePeriod": "2026-11-05",
                "AdId": 1,
                "Impressions": 1234,
                "Clicks": 10,
                "Ctr": "1.50",
                "Spend": 12.5,
                "ConversionRate": "0.00",
            }
        ]
        assert frame.dtypes.astype(str).to_dict() == REPORT_DTYPES

    @pytest.mark.parametrize("report_file", [None, ""])
    async def test_a_report_without_rows_is_an_empty_frame(self, bing, report_file: str | None):
        bing.report_file = report_file

        frame = await _run(_connected_source(), "ads_stats")

        assert frame.empty and list(frame.columns) == []

    async def test_a_soap_fault_during_the_report_names_its_operation_errors(self, bing):
        bing.fault = _web_fault(
            SimpleNamespace(
                AdApiFaultDetail=SimpleNamespace(
                    Errors=SimpleNamespace(AdApiError=SimpleNamespace(Code="105", ErrorCode="InvalidCredentials"))
                )
            )
        )

        with pytest.raises(RuntimeError, match=r"^Bing Ads API error -- InvalidCredentials: $"):
            await _run(_connected_source(), "ads_stats")

    async def test_the_account_is_read_from_customer_management(self, bing):
        rows = await _run(_connected_source(), "accounts")

        token, get_account = bing.sent
        assert _token_request(token) == TOKEN_FORM
        _soap(get_account, "GetAccount", GET_ACCOUNT_BODY)
        assert bing.client_options == [{"timeout": 30}]
        assert rows == [
            {
                "CurrencyCode": "EUR",
                "Id": "111",
                "Name": "Acct",
                "ParentCustomerId": "999",
                "PauseReason": None,
                "TimeZone": "AmsterdamBerlinBernRomeStockholmVienna",
                "LinkedAgencies": [{"Id": "5", "Name": "Agency"}],
                "date": DAY,
            }
        ]

    async def test_an_answer_without_the_account_fails(self, bing):
        bing.soap["GetAccount"] = "<Envelope><Body><GetAccountResponse/></Body></Envelope>"

        with pytest.raises(RuntimeError, match=r"^Bing Ads returned no account for 111$"):
            await _run(_connected_source(), "accounts")

    async def test_a_refused_account_lookup_raises_the_http_error(self, bing, monkeypatch):
        answer = FakeBing._answer

        def refuse(self: FakeBing, request: httpx2.Request) -> httpx2.Response:
            response = answer(self, request)
            return response if str(request.url) == TOKEN_URL else httpx2.Response(500, text="<s:Fault/>")

        monkeypatch.setattr(FakeBing, "_answer", refuse)

        with pytest.raises(httpx2.HTTPStatusError):
            await _run(_connected_source(), "accounts")
        assert [sent.action for sent in bing.sent] == [None, "GetAccount"]

    @pytest.mark.parametrize(
        ("key", "entities"), [("campaigns", "Campaigns"), ("ad_groups", "AdGroups"), ("ads", "Ads")]
    )
    async def test_an_entity_downloads_the_bulk_file_under_the_accounts_customer(self, bing, key: str, entities: str):
        await _run(_connected_source(), key)

        token, get_account = bing.sent
        assert _token_request(token) == TOKEN_FORM
        _soap(get_account, "GetAccount", GET_ACCOUNT_BODY)
        (manager,) = bing.managers
        (download,) = bing.downloads
        assert manager.service == "Bulk"
        assert (manager.authorization_data.account_id, manager.authorization_data.customer_id) == ("111", "999")
        assert manager.authorization_data.developer_token == "dev"
        assert manager.authorization_data.authentication == ("cid", "sdk-tokens")
        assert Path(manager.working_directory).name.startswith("bingads-")
        assert {name: value for name, value in vars(download).items() if name != "_result_file_directory"} == {
            "_result_file_name": "entities.csv",
            "_decompress_result_file": True,
            "_overwrite_result_file": True,
            "_submit_download_parameter": download._submit_download_parameter,
            "_timeout_in_milliseconds": 3600000,
        }
        assert vars(download._submit_download_parameter) == {
            "_data_scope": ["EntityData"],
            "_download_entities": [entities],
            "_file_type": "Csv",
            "_campaign_ids": None,
            "_last_sync_time_in_utc": None,
        }

    async def test_an_entity_returns_the_bulk_records_typed_and_dated(self, bing):
        frame = await _run(_connected_source(), "ad_groups")

        assert frame.astype(object).where(frame.notna(), None).to_dict("records") == [
            {
                "Type": "Ad Group",
                "Status": "Active",
                "Id": "9007199254740993",
                "Parent Id": "555",
                "Campaign": "Camp",
                "Ad Group": "Group A",
                "Modified Time": pd.Timestamp("2026-11-04 09:15:02.123", tz="UTC"),
                "Start Date": dt.date(2026, 10, 1),
                "End Date": dt.date(2026, 12, 31),
                "Cpc Bid": "0.45",
                "Use Optimized Targeting": True,
                "date": DAY,
            },
            {
                "Type": "Ad Group",
                "Status": "Paused",
                "Id": "777",
                "Parent Id": "555",
                "Campaign": "Camp",
                "Ad Group": "Group B",
                "Modified Time": pd.Timestamp("2026-11-03 10:00:00", tz="UTC"),
                "Start Date": dt.date(2026, 10, 1),
                "End Date": None,
                "Cpc Bid": "0.3",
                "Use Optimized Targeting": False,
                "date": DAY,
            },
        ]
        assert list(frame.index) == [0, 1]

    async def test_an_account_without_a_parent_customer_downloads_nothing(self, bing):
        bing.soap["GetAccount"] = GET_ACCOUNT_RESPONSE.replace("<a:ParentCustomerId>999</a:ParentCustomerId>", "")

        with pytest.raises(RuntimeError, match=r"^Bing Ads account 111 names no parent customer$"):
            await _run(_connected_source(), "campaigns")
        assert bing.managers == []

    async def test_a_bulk_answer_without_the_account_downloads_nothing(self, bing):
        bing.soap["GetAccount"] = "<Envelope><Body><GetAccountResponse/></Body></Envelope>"

        with pytest.raises(RuntimeError, match=r"^Bing Ads returned no account for 111$"):
            await _run(_connected_source(), "ads")
        assert bing.managers == []

    async def test_a_non_numeric_account_is_refused_before_any_request(self, bing):
        source = BingAds(id="src-1", account_id="1</AccountId>", connection=_connected_source().connection)

        for key in ("accounts", "campaigns"):
            with pytest.raises(ValueError):
                await _run(source, key)
        assert bing.sent == []


class TestEntityAssets:
    def test_entities_are_snapshots_tagged_entity(self):
        for key in ENTITY_ASSETS:
            asset = _asset(_source(), key)
            assert isinstance(asset.partitioning, il.SnapshotPartitionConfig), key
            assert asset.partitioning.column == "date", key
            assert asset.tags == ["Entity"], key

    async def test_the_columns_no_downloaded_record_fills_are_dropped(self, bing):
        frame = await _run(_connected_source(), "ad_groups")
        assert list(frame["Type"]) == ["Ad Group", "Ad Group"]
        assert "Bid" not in frame.columns and "Name" not in frame.columns

    async def test_a_soap_fault_during_the_download_names_its_operation_errors(self, bing):
        bing.fault = _web_fault(
            SimpleNamespace(
                ApiFaultDetail=SimpleNamespace(
                    BatchErrors="",
                    OperationErrors=SimpleNamespace(
                        OperationError=SimpleNamespace(ErrorCode="CustomerIdHasToBeSpecified", Message="no customer")
                    ),
                )
            )
        )
        with pytest.raises(RuntimeError, match="CustomerIdHasToBeSpecified: no customer"):
            await _run(_connected_source(), "campaigns")

    async def test_another_download_error_is_raised_as_is(self, bing):
        bing.fault = TimeoutError("bulk download timed out")
        with pytest.raises(TimeoutError, match="timed out"):
            await _run(_connected_source(), "ads")

    async def test_bulk_values_reconcile_to_their_types(self, bing):
        source = _connected_source()
        first, second = _reconcile(source, "ad_groups", await _run(source, "ad_groups"))
        assert (first["id"], first["parent_id"], first["ad_group"], first["date"]) == (
            "9007199254740993",
            "555",
            "Group A",
            DAY,
        )
        assert (first["start_date"], first["end_date"], second["end_date"]) == (
            dt.date(2026, 10, 1),
            dt.date(2026, 12, 31),
            None,
        )
        assert first["modified_time"] == pd.Timestamp("2026-11-04 09:15:02.123", tz="UTC")
        assert (first["cpc_bid"], first["use_optimized_targeting"], second["use_optimized_targeting"]) == (
            0.45,
            True,
            False,
        )

    async def test_a_campaign_schedule_with_a_time_keeps_its_day(self, bing):
        bing.bulk_file = CAMPAIGNS_FILE
        source = _connected_source()
        first, second = _reconcile(source, "campaigns", await _run(source, "campaigns"))
        assert (first["id"], first["start_date"], first["end_date"]) == (
            "555",
            dt.date(2026, 10, 1),
            dt.date(2026, 12, 31),
        )
        assert (second["start_date"], second["end_date"]) == (None, None)

    async def test_every_ad_type_is_kept(self, bing):
        bing.bulk_file = ADS_FILE
        source = _connected_source()
        rows = _reconcile(source, "ads", await _run(source, "ads"))
        assert [(row["type"], row["id"], row["parent_id"]) for row in rows] == [
            ("Responsive Search Ad", "1", "9"),
            ("Expanded Text Ad", "2", "9"),
            ("Some Future Ad", "3", "9"),
        ]
        assert rows[0]["headline"] == '[{"text":"Shop"}]'

    async def test_the_account_leaves_out_its_address_and_tax_registration(self, bing):
        bing.soap["GetAccount"] = GET_ACCOUNT_RESPONSE.replace(
            "<a:CurrencyCode>",
            "<a:LastModifiedTime>2026-10-01T08:30:00.123</a:LastModifiedTime>"
            "<a:TaxCertificate><a:Status>Valid</a:Status></a:TaxCertificate><a:CurrencyCode>",
        )
        source = _connected_source()
        rows = await _run(source, "accounts")
        assert not {"BusinessAddress", "TaxCertificate"} & set(rows[0])
        [row] = _reconcile(source, "accounts", rows)
        assert (row["id"], row["date"], row["parent_customer_id"], row["time_zone"]) == (
            "111",
            DAY,
            "999",
            "AmsterdamBerlinBernRomeStockholmVienna",
        )
        assert row["linked_agencies"] == '[{"Id": "5", "Name": "Agency"}]'
        assert row["last_modified_time"] == pd.Timestamp("2026-10-01 08:30:00.123")

    def test_the_excluded_account_fields_are_not_declared(self):
        declared = set(schemas.Accounts.model_fields)
        assert not declared & {"business_address", "tax_certificate", "tax_information"}


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        src = _source()
        assert isinstance(src.normalizer, DataFrameNormalizer)

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "ads_stats")

        # Exactly what the k8s runner ships to the child pod.
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "ads_stats")
        assert isinstance(child_asset, Asset)

        assert isinstance(child_asset.normalizer, DataFrameNormalizer)

    def test_pascalcase_report_row_conforms_after_roundtrip(self):
        """A raw API-shaped row must normalize and validate against the schema."""
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "ads_stats")
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == "ads_stats")
        assert isinstance(child_asset, Asset)

        # One row with every requested report column, as Bing returns them.
        # All schema fields are required-nullable, so None is valid everywhere.
        row: dict[str, object] = dict.fromkeys(constants.AD_PERFORMANCE_FIELDS)
        row["TimePeriod"] = "2026-06-10"
        df = pd.DataFrame([row])

        normalizer = child_asset.normalizer
        assert normalizer is not None
        normalized = normalizer.normalize(df)
        TableData.from_data(normalized).reconcile(AdsStats)  # must not raise


def _web_fault(detail: SimpleNamespace) -> WebFault:
    return WebFault(SimpleNamespace(faultstring="Invalid client data.", detail=detail), document=None)
