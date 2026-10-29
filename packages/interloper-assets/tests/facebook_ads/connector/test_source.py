"""Regression tests for the FacebookAds source.

Facebook insights nest ``actions``/``action_values`` lists that
``FacebookActionsNormalizer`` pivots into one column per action type
(``actions`` -> ``actions_link_click``); entity reports nest dicts
(``creative``) that the base normalizer flattens. These tests pin the
pivot/sanitize logic, that the custom normalizer survives the host→child spec
round-trip, and that frames reconcile against the ported schemas (missing
nullable columns are filled, so partial action sets are fine).
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import interloper as il
import pandas as pd
import pytest
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_ads import constants
from interloper_assets.facebook_ads.api import FacebookGraphApi
from interloper_assets.facebook_ads.connection import FacebookAdsConnection
from interloper_assets.facebook_ads.connector import schemas
from interloper_assets.facebook_ads.connector import source as connector
from interloper_assets.facebook_ads.connector.source import PIVOT_COLUMNS, FacebookActionsNormalizer, FacebookAds


def _source() -> Any:
    return FacebookAds(id="src-1", account_id="123")  # ty: ignore[missing-argument]


class TestSourceNormalizer:
    def test_all_assets_use_the_actions_normalizer(self):
        for asset in _source().assets:
            assert isinstance(asset.normalizer, FacebookActionsNormalizer), type(asset).key

    def test_all_ten_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "campaigns_stats",
            "ads_stats",
            "ads_stats_by_age_gender",
            "ads_stats_by_country",
            "videos_stats",
            "custom_audiences",
            "ads",
            "campaigns",
            "ad_accounts",
            "adsets",
        }


class TestActionsNormalizer:
    """The action-list pivot is the crux of the Facebook port."""

    def test_pivots_actions_into_per_action_type_columns(self):
        rows = [
            {
                "date_start": "2026-06-10",
                "account_id": "123",
                "ad_id": "456",
                # link_click appears twice (action-device breakdown) -> summed.
                "actions": [
                    {"action_type": "link_click", "action_device": "mobile", "value": "5"},
                    {"action_type": "link_click", "action_device": "desktop", "value": "2"},
                    {"action_type": "video_view", "value": "3"},
                ],
                "action_values": [
                    {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "12.5"},
                ],
            }
        ]
        df = FacebookActionsNormalizer().normalize(rows)
        assert df.loc[0, "actions_link_click"] == 7.0  # summed across devices
        assert df.loc[0, "actions_video_view"] == 3.0
        # the "." in the action type is sanitized to "_"
        assert df.loc[0, "action_values_offsite_conversion_fb_pixel_purchase"] == 12.5
        assert "actions" not in df.columns  # original list column dropped

    def test_empty_rows_yield_empty_frame(self):
        assert FacebookActionsNormalizer().normalize([]).empty


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.operations if type(a).key == key)

    def test_custom_normalizer_survives_roundtrip(self):
        norm = self._child("ads_stats").normalizer
        assert isinstance(norm, FacebookActionsNormalizer)  # not degraded to the base
        assert norm.flatten_max_level == 1

    def test_insights_row_reconciles_against_ads_schema(self):
        child = self._child("ads_stats")
        rows = [
            {"date_start": "2026-06-10", "account_id": "123", "actions": [{"action_type": "link_click", "value": "7"}]}
        ]
        normalized = child.normalizer.normalize(rows)
        assert "actions_link_click" in normalized.columns
        out = TableData.from_data(normalized).reconcile(schemas.AdsStats)
        assert int(out.loc[0, "actions_link_click"]) == 7

    def test_sparse_row_passes_validation(self):
        """A sparse insights frame still passes validation.

        Only a few of the 200 fields arrive, and every schema field is
        optional, so absent action types are not 'Field required' errors.
        """
        df = pd.DataFrame([{"date_start": "2026-06-10", "account_id": "123", "actions_link_click": 7}])
        TableData.from_data(df).reconcile(schemas.AdsStats, strict=True)  # must not raise

    def test_entity_row_keeps_the_creative_whole_and_reconciles(self):
        child = self._child("ads")
        rows = [{"id": "456", "name": "My Ad", "creative": {"id": "789"}}]
        normalized = child.normalizer.normalize(rows)
        assert "creative_id" not in normalized.columns
        out = TableData.from_data(normalized).reconcile(schemas.Ads)
        assert json.loads(out.loc[0, "creative"]) == {"id": "789"}


class TestSchemaParity:
    """Both stats assets fetch the same action arrays; the schemas must not drop them unevenly."""

    _ACTION_FAMILIES = (
        "actions_",
        "action_values_",
        "cost_per_action_type_",
        "cost_per_unique_action_type_",
        "unique_actions_",
    )

    def test_ads_stats_declares_every_campaign_action_column(self):
        campaigns = schemas.CampaignsStats.model_fields
        ads = schemas.AdsStats.model_fields
        expected = {name: info for name, info in campaigns.items() if name.startswith(self._ACTION_FAMILIES)}

        missing = sorted(set(expected) - set(ads))
        assert not missing, f"AdsStats lacks {len(missing)} campaign action columns: {missing}"

        mismatched = {
            name: (str(info.annotation), str(ads[name].annotation))
            for name, info in expected.items()
            if info.annotation != ads[name].annotation
        }
        assert not mismatched, mismatched

    def test_ads_stats_declares_every_requested_scalar(self):
        # Breakdown dimensions come back as columns too, though they are not in the fields list.
        breakdowns = {"publisher_platform", "platform_position", "impression_device"}
        requested = {field for field in constants.ADS_INSIGHT_FIELDS if field not in PIVOT_COLUMNS} | breakdowns

        missing = sorted(requested - set(schemas.AdsStats.model_fields))
        assert not missing, f"requested but undeclared on AdsStats (silently dropped): {missing}"

    def test_campaigns_stats_declares_the_shared_scalars(self):
        for name in ("attribution_setting", "quality_ranking", "converted_product_quantity"):
            assert name in schemas.CampaignsStats.model_fields, name

    @pytest.mark.parametrize(
        ("fields", "schema"),
        [
            (constants.AD_ACCOUNT_ENTITY_FIELDS, schemas.AdAccounts),
            (constants.ADSET_ENTITY_FIELDS, schemas.Adsets),
            (constants.AD_ENTITY_FIELDS, schemas.Ads),
            (constants.CAMPAIGN_ENTITY_FIELDS, schemas.Campaigns),
        ],
    )
    def test_entity_schemas_declare_exactly_the_requested_fields(self, fields: list[str], schema: type[il.Schema]):
        declared = set(schema.model_fields) - {"date"}
        assert sorted(set(fields) - declared) == [], "requested but undeclared (silently dropped)"
        assert sorted(declared - set(fields)) == [], "declared but never requested (always NULL)"


class TestNewEntities:
    """Ad accounts and ad sets keep nested objects whole, under the vendor's name."""

    def _child(self, key: str) -> Any:
        return TestSpecRoundtripAndReconcile()._child(key)

    def test_entity_normalizer_survives_roundtrip_without_flattening(self):
        for key in ("ad_accounts", "adsets", "ads", "campaigns"):
            normalizer = self._child(key).normalizer
            assert isinstance(normalizer, FacebookActionsNormalizer), key
            assert normalizer.flatten_max_level == 0, key

    def test_adset_targeting_lands_whole_as_json(self):
        child = self._child("adsets")
        rows = [
            {
                "id": "1",
                "targeting": {"geo_locations": {"countries": ["DE"]}, "age_min": 18},
                "promoted_object": {"pixel_id": "9", "custom_event_type": "PURCHASE"},
                "date": "2026-10-01",
            }
        ]
        normalized = child.normalizer.normalize(rows)
        assert not [column for column in normalized.columns if column.startswith("targeting_")]
        out = TableData.from_data(normalized).reconcile(schemas.Adsets)
        assert json.loads(out.loc[0, "targeting"]) == {"geo_locations": {"countries": ["DE"]}, "age_min": 18}
        assert json.loads(out.loc[0, "promoted_object"])["pixel_id"] == "9"

    def test_ad_account_keeps_the_vendors_business_name(self):
        child = self._child("ad_accounts")
        rows = [{"id": "act_1", "business": {"id": "5", "name": "Acme Holding"}, "business_name": "Acme GmbH"}]
        normalized = child.normalizer.normalize(rows)
        out = TableData.from_data(normalized).reconcile(schemas.AdAccounts)
        assert out.loc[0, "business_name"] == "Acme GmbH"

    def test_adset_instants_and_minor_unit_amounts_are_typed(self):
        child = self._child("adsets")
        rows = [
            {
                "id": "1",
                "created_time": "2026-09-01T10:00:00+0200",
                "updated_time": "2026-09-02T10:00:00+0200",
                "daily_budget": "1500",
                "budget_remaining": "0",
            }
        ]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.Adsets, strict=True)
        assert out.loc[0, "created_time"] == pd.Timestamp("2026-09-01T08:00:00", tz="UTC")
        assert (out.loc[0, "daily_budget"], out.loc[0, "budget_remaining"]) == (1500, 0)

    def test_ad_account_instants_and_minor_unit_amounts_are_typed(self):
        child = self._child("ad_accounts")
        rows = [{"id": "act_1", "created_time": "2020-01-15T09:30:00+0100", "amount_spent": "123456", "spend_cap": "0"}]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.AdAccounts, strict=True)
        assert out.loc[0, "created_time"] == pd.Timestamp("2020-01-15T08:30:00", tz="UTC")
        assert (out.loc[0, "amount_spent"], out.loc[0, "spend_cap"]) == (123456, 0)

    def test_campaign_objects_stay_whole_and_its_instants_and_amounts_are_typed(self):
        child = self._child("campaigns")
        adlabels = [{"id": "7", "name": "brand"}]
        rows = [
            {
                "id": "1",
                "adlabels": adlabels,
                "created_time": "2026-09-01T10:00:00+0200",
                "daily_budget": "1500",
                "spend_cap": "0",
                "start_time": "2026-09-05T00:00:00+0200",
            }
        ]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.Campaigns, strict=True)
        assert json.loads(out.loc[0, "adlabels"]) == adlabels
        assert out.loc[0, "created_time"] == pd.Timestamp("2026-09-01T08:00:00", tz="UTC")
        assert (out.loc[0, "daily_budget"], out.loc[0, "spend_cap"]) == (1500, 0)
        assert out.loc[0, "start_time"] == "2026-09-05T00:00:00+0200"

    def test_ad_instants_bid_and_objects_are_typed(self):
        child = self._child("ads")
        rows = [
            {
                "id": "1",
                "created_time": "2026-09-01T10:00:00+0200",
                "bid_amount": 250,
                "adlabels": [{"id": "7", "name": "brand"}],
                "targeting": {"geo_locations": {"countries": ["DE"]}},
            }
        ]
        out = TableData.from_data(child.normalizer.normalize(rows)).reconcile(schemas.Ads, strict=True)
        assert out.loc[0, "created_time"] == pd.Timestamp("2026-09-01T08:00:00", tz="UTC")
        assert out.loc[0, "bid_amount"] == 250
        assert json.loads(out.loc[0, "adlabels"]) == [{"id": "7", "name": "brand"}]
        assert json.loads(out.loc[0, "targeting"]) == {"geo_locations": {"countries": ["DE"]}}


class _FakeObject:
    def __init__(self, data: dict) -> None:
        self.data = data

    def export_all_data(self) -> dict:
        return self.data


class TestEntityReads:
    def test_every_request_goes_through_the_graph_engine(self):
        connection = FacebookAdsConnection(id="fb", access_token="x", app_id="1", app_secret="x")
        assert isinstance(connection.api, FacebookGraphApi)

    def test_the_account_requests_no_field_that_needs_a_page(self):
        # Facebook answers 400 for the whole account when one of these comes without a page_id
        for name in ("show_checkout_experience", "has_page_authorized_adaccount"):
            assert name not in constants.AD_ACCOUNT_ENTITY_FIELDS

    def test_an_insights_report_runs_as_an_async_job_and_returns_its_rows(self, monkeypatch):
        class FakeRow:
            def __init__(self, data: dict) -> None:
                self._json = data

        class FakeJob:
            def api_get(self) -> FakeRow:
                return FakeRow({"async_status": "Job Completed", "async_percent_completion": 100})

            def get_result(self) -> list[FakeRow]:
                return [FakeRow({"campaign_id": "c1", "spend": "1.5"})]

        class FakeAccount:
            def get_insights(self, **kwargs: Any) -> FakeJob:
                assert (kwargs["fields"], kwargs["params"], kwargs["is_async"]) == (["spend"], {"level": "ad"}, True)
                return FakeJob()

        monkeypatch.setattr(connector, "_account", lambda connection, account_id: FakeAccount())
        rows = connector._get_ads_insights(_source().connection, "123", fields=["spend"], params={"level": "ad"})
        assert rows == [{"campaign_id": "c1", "spend": "1.5"}]

    def test_entity_assets_read_their_edges_and_stamp_the_day(self, monkeypatch):
        class FakeAccount:
            def get_ads(self, **kwargs: Any) -> list[_FakeObject]:
                assert kwargs["fields"] == constants.AD_ENTITY_FIELDS
                return [_FakeObject({"id": "a1"}), _FakeObject({"id": "a2"})]

            def get_ad_sets(self, **kwargs: Any) -> list[_FakeObject]:
                assert kwargs["fields"] == constants.ADSET_ENTITY_FIELDS
                return [_FakeObject({"id": "s1"})]

            def api_get(self, **kwargs: Any) -> _FakeObject:
                assert kwargs["fields"] == constants.AD_ACCOUNT_ENTITY_FIELDS
                return _FakeObject({"id": "act_123", "account_id": "123"})

        monkeypatch.setattr(connector, "_account", lambda connection, account_id: FakeAccount())
        source = _source()
        day = dt.date(2026, 10, 1)

        def run(key: str) -> list[dict]:
            asset = next(a for a in source.assets if type(a).key == key)
            return asset.data(context=il.ExecutionContext(asset, il.TimePartition(value=day).window))

        assert run("ads") == [{"id": "a1", "date": day}, {"id": "a2", "date": day}]
        assert run("adsets") == [{"id": "s1", "date": day}]
        assert run("ad_accounts") == [{"id": "act_123", "account_id": "123", "date": day}]


def test_isinstance_of_dataframe_normalizer():
    # The custom normalizer is a DataFrameNormalizer, so it inherits its config.
    assert isinstance(FacebookActionsNormalizer(), DataFrameNormalizer)
