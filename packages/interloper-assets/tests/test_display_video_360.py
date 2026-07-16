"""Regression tests for the DisplayVideo360 source.

Partner objects nest configuration two levels deep (``dataAccessConfig.sdfConfig.*``,
``adServerConfig.measurementConfig.dv360ToCm*``). The source-level normalizer must
flatten them and split digit groups (``dv360ToCm`` → ``dv_360_to_cm``) onto the
schema columns — and survive the host→child DAG-spec round-trip.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.display_video_360 import schemas
from interloper_assets.display_video_360.source import (
    DisplayVideo360,
    DisplayVideo360Normalizer,
    _parse_report_csv,
)

_ASSET_KEY = "partners"

_REPORT_CSV = (
    "Date,Advertiser ID,Active View: Viewable Impressions,Active View: % Viewable Impressions,Impressions\n"
    "2026/07/10,9,90,85.5,100\n"
    "2026/07/10,9,180,72.1,200\n"
    ",,270,,300\n"
    "\n"
    "Report Time:,2026/07/11\n"
)

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
    "exchangeConfig": {"enabledExchanges": json.dumps([{"exchange": "EXCHANGE_GOOGLE_AD_MANAGER"}])},
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
            asset.data(context=object(), connection=object())


class TestReports:
    """Bid Manager report filters, CSV parsing, and header normalization."""

    def test_filters_partner_only(self):
        assert _source()._report_filters == [{"type": "FILTER_PARTNER", "value": "123"}]

    def test_filters_narrowed_by_advertiser(self):
        assert _source(advertiser_id="9")._report_filters == [
            {"type": "FILTER_PARTNER", "value": "123"},
            {"type": "FILTER_ADVERTISER", "value": "9"},
        ]

    def test_parse_report_csv_drops_footer(self):
        rows = _parse_report_csv(_REPORT_CSV)
        assert len(rows) == 2
        assert rows[0]["Impressions"] == 100
        assert rows[1]["Impressions"] == 200

    def test_parse_report_csv_no_data_marker(self):
        assert _parse_report_csv("Date,Impressions\nNo data returned by the reporting service.,\n") == []

    def test_percent_headers_do_not_collide(self):
        normalizer = DisplayVideo360Normalizer(snake_case_digits=True, flatten_max_level=2)
        assert normalizer.column_name("Active View: % Viewable Impressions") == (
            "active_view_pct_viewable_impressions"
        )
        assert normalizer.column_name("Active View: Viewable Impressions") == "active_view_viewable_impressions"
        assert normalizer.column_name("CM360 Placement ID") == "cm_360_placement_id"
        assert normalizer.column_name("YouTube Revenue (eCPV) (Adv Currency)") == (
            "you_tube_revenue_e_cpv_adv_currency"
        )

    def test_parsed_report_conforms_to_schema(self):
        rows = _parse_report_csv(_REPORT_CSV)
        normalized = _source().normalizer.normalize(rows)
        conformer = Representation.of(normalized).conformer
        df = conformer.reconcile(normalized, schemas.LineItemsStats)
        conformer.validate(df, schemas.LineItemsStats)  # must not raise
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

        conformer = Representation.of(normalized).conformer
        df = conformer.reconcile(normalized, schemas.Partners)
        conformer.validate(df, schemas.Partners)  # must not raise
        record = df.to_dict("records")[0]
        assert record["ad_server_config_measurement_config_dv_360_to_cm_cost_reporting_enabled"] is True
        assert record["data_access_config_sdf_config_admin_email"] == "admin@digitl.com"

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        assert len(src.assets) == 5
        for asset in src.assets:
            assert isinstance(asset.normalizer, DisplayVideo360Normalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_custom_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == _ASSET_KEY)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DisplayVideo360Normalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 2
        # The % -> pct behavior must survive in the child pod.
        assert normalizer.column_name("Active View: % Viewable Impressions") == (
            "active_view_pct_viewable_impressions"
        )
