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
from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.representation import representation_for
from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_ads import schemas
from interloper_assets.facebook_ads.source import FacebookActionsNormalizer, FacebookAds


def _source() -> Any:
    return FacebookAds(id="src-1", account_id="123")  # ty: ignore[unknown-argument]


class TestSourceNormalizer:
    def test_all_assets_use_the_actions_normalizer(self):
        for asset in _source().assets:
            assert isinstance(asset.normalizer, FacebookActionsNormalizer), type(asset).key

    def test_all_eight_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "campaigns",
            "ads",
            "ads_by_age_gender",
            "ads_by_country",
            "videos",
            "custom_audiences",
            "ads_metadata",
            "campaigns_metadata",
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
        return next(a for a in child_dag.assets if type(a).key == key)

    def test_custom_normalizer_survives_roundtrip(self):
        norm = self._child("ads").normalizer
        assert isinstance(norm, FacebookActionsNormalizer)  # not degraded to the base
        assert norm.flatten_max_level == 1

    def test_insights_row_reconciles_against_ads_schema(self):
        child = self._child("ads")
        rows = [
            {"date_start": "2026-06-10", "account_id": "123", "actions": [{"action_type": "link_click", "value": "7"}]}
        ]
        normalized = child.normalizer.normalize(rows)
        assert "actions_link_click" in normalized.columns
        out = representation_for(normalized).conformer.reconcile(normalized, schemas.Ads)
        assert int(out.loc[0, "actions_link_click"]) == 7

    def test_sparse_row_passes_validation(self):
        """Insights are sparse: a frame with only a few of the 75 fields must
        pass the asset's (non-strict) validation — every schema field is
        optional, so absent action types are not 'Field required' errors."""
        df = pd.DataFrame([{"date_start": "2026-06-10", "account_id": "123", "actions_link_click": 7}])
        representation_for(df).conformer.validate(df, schemas.Ads)  # must not raise

    def test_metadata_row_flattens_creative_and_reconciles(self):
        child = self._child("ads_metadata")
        rows = [{"id": "456", "name": "My Ad", "creative": {"id": "789", "name": "Creative A"}}]
        normalized = child.normalizer.normalize(rows)
        assert "creative_id" in normalized.columns  # nested dict flattened by the normalizer
        representation_for(normalized).conformer.reconcile(normalized, schemas.AdsMetadata)  # must not raise


def test_isinstance_of_dataframe_normalizer():
    # The custom normalizer is a DataFrameNormalizer, so it inherits its config.
    assert isinstance(FacebookActionsNormalizer(), DataFrameNormalizer)
