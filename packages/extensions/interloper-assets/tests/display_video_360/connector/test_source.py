"""Regression tests for the DisplayVideo360 source.

Partner objects nest configuration two levels deep (``dataAccessConfig.sdfConfig.*``,
``adServerConfig.measurementConfig.dv360ToCm*``). The source-level normalizer must
flatten them and split digit groups (``dv360ToCm`` → ``dv_360_to_cm``) onto the
schema columns — and survive the host→child DAG-spec round-trip.
"""

from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace
from typing import Any

import pytest
from interloper.asset import Asset
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.display_video_360 import constants
from interloper_assets.display_video_360.connection import DisplayVideo360Connection
from interloper_assets.display_video_360.connector import schemas
from interloper_assets.display_video_360.connector.source import DisplayVideo360, DisplayVideo360Normalizer

_ASSET_KEY = "partners"

_PARTNER = {
    "name": "partners/123",
    "partnerId": "123",
    "updateTime": "2026-01-01T00:00:00Z",
    "displayName": "Digitl",
    "entityStatus": "ENTITY_STATUS_ACTIVE",
    "generalConfig": {"timeZone": "Europe/Berlin", "currencyCode": "EUR"},
    "adServerConfig": {
        "measurementConfig": {"dv360ToCmCostReportingEnabled": True, "dv360ToCmDataSharingEnabled": False}
    },
    "dataAccessConfig": {"sdfConfig": {"version": "SDF_VERSION_8", "adminEmail": "admin@digitl.com"}},
    "exchangeConfig": {
        "enabledExchanges": [{"exchange": "EXCHANGE_GOOGLE_AD_MANAGER", "googleAdManagerAgencyId": "1"}]
    },
    "billingConfig": {"billingProfileId": "77"},
}


def _source(**overrides: str) -> Any:
    fields: dict[str, Any] = {"partner_id": "123", **overrides}
    return DisplayVideo360(id="src-1", **fields)


class TestAudienceScope:
    """Audience calls scope by partner XOR advertiser; advertiser wins when set."""

    def test_partner_scope_by_default(self):
        assert _source()._audience_scope == {"partnerId": "123"}

    def test_advertiser_scope_when_set(self):
        assert _source(advertiser_id="9")._audience_scope == {"advertiserId": "9"}

    def test_custom_audiences_requires_audience_id(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == "custom_audiences")
        with pytest.raises(ValueError, match="audience_id"):
            asset.data(context=object())


class TestReports:
    """Bid Manager report filters and header normalization."""

    def test_filters_partner_only(self):
        assert _source()._report_filters == [{"type": "FILTER_PARTNER", "value": "123"}]

    def test_filters_narrowed_by_advertiser(self):
        assert _source(advertiser_id="9")._report_filters == [
            {"type": "FILTER_PARTNER", "value": "123"},
            {"type": "FILTER_ADVERTISER", "value": "9"},
        ]

    def test_percent_headers_do_not_collide(self):
        normalizer = DisplayVideo360Normalizer(snake_case_digits=True, flatten_max_level=2)
        assert normalizer.column_name("Active View: % Viewable Impressions") == ("active_view_pct_viewable_impressions")
        assert normalizer.column_name("Active View: Viewable Impressions") == "active_view_viewable_impressions"
        assert normalizer.column_name("CM360 Placement ID") == "cm_360_placement_id"
        assert normalizer.column_name("YouTube Revenue (eCPV) (Adv Currency)") == (
            "you_tube_revenue_e_cpv_adv_currency"
        )

    def test_report_rows_conform_to_schema(self, google, connection, download):
        google.serve({"queryId": "q"}, {"key": {"reportId": "r"}}, _DONE_REPORT)
        rows = _run(connection, "line_items_stats")
        normalized = _source().normalizer.normalize(rows)
        table = TableData.from_data(normalized)
        df = table.reconcile(schemas.LineItemsStats)
        TableData.from_data(df).reconcile(schemas.LineItemsStats, strict=True)  # must not raise
        record = df.to_dict("records")[0]
        assert record["active_view_pct_viewable_impressions"] == "85.5"
        assert record["impressions"] == 100


class TestNormalizerMapping:
    """A realistic partner payload flattens exactly onto the Partners schema."""

    def test_partner_payload_maps_onto_schema(self):
        src = _source()
        normalizer = src.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        normalized = normalizer.normalize([_PARTNER])

        # `date` is stamped by the asset (partition column), not present in the raw payload.
        fields = set(schemas.Partners.model_fields)
        assert set(normalized.columns) == fields - {"date"}

        table = TableData.from_data(normalized)
        df = table.reconcile(schemas.Partners)
        TableData.from_data(df).reconcile(schemas.Partners, strict=True)  # must not raise
        record = df.to_dict("records")[0]
        assert record["ad_server_config_measurement_config_dv_360_to_cm_cost_reporting_enabled"] is True
        assert record["data_access_config_sdf_config_admin_email"] == "admin@digitl.com"

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        assert len(src.assets) == 5
        for asset in src.assets:
            assert isinstance(asset.normalizer, DisplayVideo360Normalizer), type(asset).key


_DAY = dt.date(2026, 7, 10)
_DISPLAY_VIDEO = "https://displayvideo.googleapis.com/v4"
_BID_MANAGER = "https://doubleclickbidmanager.googleapis.com/v2"
_DONE_REPORT = {"metadata": {"status": {"state": "DONE"}, "googleCloudStoragePath": "https://storage/report.csv"}}


def _run(connection: DisplayVideo360Connection, key: str, **fields: str) -> list[dict[str, Any]]:
    settings: dict[str, Any] = {"partner_id": "123", **fields}
    source = DisplayVideo360(id="src-1", connection=connection, **settings)
    asset = next(a for a in source.assets if type(a).key == key)
    return asset.data(context=SimpleNamespace(partition_date=_DAY))


def _report_requests(title: str, dimensions: list[str], metrics: list[str], filters: list[dict]) -> list[tuple]:
    day = {"year": 2026, "month": 7, "day": 10}
    query = {
        "metadata": {
            "title": title,
            "dataRange": {"range": "CUSTOM_DATES", "customStartDate": day, "customEndDate": day},
            "format": "CSV",
        },
        "params": {"type": "STANDARD", "groupBys": dimensions, "filters": filters, "metrics": metrics, "options": {}},
        "schedule": {"frequency": "ONE_TIME"},
    }
    return [
        ("POST", f"{_BID_MANAGER}/queries?alt=json", query),
        ("POST", f"{_BID_MANAGER}/queries/q:run?synchronous=false&alt=json", None),
        ("GET", f"{_BID_MANAGER}/queries/q/reports/r?alt=json", None),
        ("DELETE", f"{_BID_MANAGER}/queries/q?", None),
    ]


class TestRequests:
    """What each asset sends, on one service of its own since the transport is not thread-safe, and returns."""

    def test_line_items_stats_runs_one_query_for_its_day(self, google, connection, download):
        google.serve({"queryId": "q"}, {"key": {"reportId": "r"}}, _DONE_REPORT)

        rows = _run(connection, "line_items_stats")

        assert google.requests == _report_requests(
            "line_items_stats_2026-07-10",
            constants.LINE_ITEM_DIMENSIONS,
            constants.LINE_ITEM_METRICS,
            [{"type": "FILTER_PARTNER", "value": "123"}],
        )
        assert google.builds == [constants.DBM_API_SERVICE]
        assert download.downloads == [{"url": "https://storage/report.csv", "timeout": None}]
        assert rows == [
            {
                "Date": "2026/07/10",
                "Advertiser ID": 9,
                "Active View: Viewable Impressions": 90,
                "Active View: % Viewable Impressions": 85.5,
                "Impressions": 100,
            },
            {
                "Date": "2026/07/10",
                "Advertiser ID": 9,
                "Active View: Viewable Impressions": 180,
                "Active View: % Viewable Impressions": 72.1,
                "Impressions": 200,
            },
        ]

    def test_line_items_stats_by_country_narrows_to_the_advertiser(self, google, connection, download):
        google.serve({"queryId": "q"}, {"key": {"reportId": "r"}}, _DONE_REPORT)

        rows = _run(connection, "line_items_stats_by_country", advertiser_id="9")

        assert google.requests == _report_requests(
            "line_items_stats_by_country_2026-07-10",
            constants.LINE_ITEM_COUNTRY_DIMENSIONS,
            constants.LINE_ITEM_COUNTRY_METRICS,
            [{"type": "FILTER_PARTNER", "value": "123"}, {"type": "FILTER_ADVERTISER", "value": "9"}],
        )
        assert google.builds == [constants.DBM_API_SERVICE]
        assert len(rows) == 2

    @pytest.mark.parametrize("strict", [False, True])
    def test_partners_lists_every_partner_and_stamps_the_day(self, google, connection, strict: bool):
        other = {**_PARTNER, "partnerId": "456", "exchangeConfig": {"enabledExchanges": []}}
        google.serve({"partners": [_PARTNER], "nextPageToken": "next"}, {"partners": [other]})

        rows = _run(connection, "partners")

        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/partners?alt=json", None),
            ("GET", f"{_DISPLAY_VIDEO}/partners?pageToken=next&alt=json", None),
        ]
        assert google.builds == [constants.DV_API_SERVICE]
        source = _source()
        table = TableData.from_data(source.normalizer.normalize(rows)).reconcile(schemas.Partners, strict=strict)
        first, second = table.to_dict("records")
        assert first["date"] == second["date"] == _DAY
        assert first["partner_id"] == "123"
        assert first["exchange_config_enabled_exchanges"] == json.dumps(
            [{"exchange": "EXCHANGE_GOOGLE_AD_MANAGER", "googleAdManagerAgencyId": "1"}]
        )
        assert second["exchange_config_enabled_exchanges"] == "[]"

    def test_audiences_list_the_partners_audiences(self, google, connection):
        google.serve({"firstPartyAndPartnerAudiences": [{"name": "a"}]})

        rows = _run(connection, "audiences")

        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences?partnerId=123&alt=json", None)
        ]
        assert google.builds == [constants.DV_API_SERVICE]
        assert rows == [{"name": "a", "date": _DAY}]

    def test_audiences_list_the_advertisers_audiences_when_set(self, google, connection):
        rows = _run(connection, "audiences", advertiser_id="9")

        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences?advertiserId=9&alt=json", None)
        ]
        assert rows == []

    def test_custom_audiences_read_the_configured_audience(self, google, connection):
        google.serve({"name": "audience", "audienceSize": "10"})

        rows = _run(connection, "custom_audiences", audience_id="7")

        assert google.requests == [
            ("GET", f"{_DISPLAY_VIDEO}/firstPartyAndPartnerAudiences/7?partnerId=123&alt=json", None)
        ]
        assert google.builds == [constants.DV_API_SERVICE]
        assert rows == [{"name": "audience", "audienceSize": "10", "date": _DAY}]


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_custom_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DisplayVideo360Normalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 2
        # The % -> pct behavior must survive in the child pod.
        assert normalizer.column_name("Active View: % Viewable Impressions") == ("active_view_pct_viewable_impressions")
