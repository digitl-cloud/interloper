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

from typing import Any

import pandas as pd
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_ads import constants, schemas
from interloper_assets.facebook_ads.source import PIVOT_COLUMNS, FacebookActionsNormalizer, FacebookAds


def _source() -> Any:
    return FacebookAds(id="src-1", account_id="123")  # ty: ignore[unknown-argument]


class TestSourceNormalizer:
    def test_all_assets_use_the_actions_normalizer(self):
        for asset in _source().assets:
            assert isinstance(asset.normalizer, FacebookActionsNormalizer), type(asset).key

    def test_all_eight_assets_present(self):
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
        out = Representation.of(normalized).conformer.reconcile(normalized, schemas.AdsStats)
        assert int(out.loc[0, "actions_link_click"]) == 7

    def test_sparse_row_passes_validation(self):
        """A sparse insights frame still passes validation.

        Only a few of the 200 fields arrive, and every schema field is
        optional, so absent action types are not 'Field required' errors.
        """
        df = pd.DataFrame([{"date_start": "2026-06-10", "account_id": "123", "actions_link_click": 7}])
        Representation.of(df).conformer.validate(df, schemas.AdsStats)  # must not raise

    def test_entity_row_flattens_creative_and_reconciles(self):
        child = self._child("ads")
        rows = [{"id": "456", "name": "My Ad", "creative": {"id": "789", "name": "Creative A"}}]
        normalized = child.normalizer.normalize(rows)
        assert "creative_id" in normalized.columns  # nested dict flattened by the normalizer
        Representation.of(normalized).conformer.reconcile(normalized, schemas.Ads)  # must not raise


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


def test_isinstance_of_dataframe_normalizer():
    # The custom normalizer is a DataFrameNormalizer, so it inherits its config.
    assert isinstance(FacebookActionsNormalizer(), DataFrameNormalizer)
