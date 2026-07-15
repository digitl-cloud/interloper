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
from interloper_assets.display_video_360.source import DisplayVideo360

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
            asset.data(connection=object())


class TestNormalizerMapping:
    """A realistic partner payload flattens exactly onto the Partners schema."""

    def test_partner_payload_maps_onto_schema(self):
        src = _source()
        normalizer = src.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        normalized = normalizer.normalize([_PARTNER])

        fields = set(schemas.Partners.model_fields)
        assert set(normalized.columns) == fields

        conformer = Representation.of(normalized).conformer
        df = conformer.reconcile(normalized, schemas.Partners)
        conformer.validate(df, schemas.Partners)  # must not raise
        record = df.to_dict("records")[0]
        assert record["ad_server_config_measurement_config_dv_360_to_cm_cost_reporting_enabled"] is True
        assert record["data_access_config_sdf_config_admin_email"] == "admin@digitl.com"

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        assert len(src.assets) == 3
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer config."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == _ASSET_KEY)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, DataFrameNormalizer)
        assert normalizer.snake_case_digits is True
        assert normalizer.flatten_max_level == 2
