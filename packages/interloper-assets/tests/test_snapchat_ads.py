"""Regression tests for the SnapchatAds source.

Snapchat stats rows nest metrics under a ``stats`` dict (flattened and
de-prefixed) or, for dimensioned reports, a ``dimension_stats`` list (exploded
into one row per dimension). The source frames those and stamps a ``date``
partition column; metadata records are JSON-normalized. These tests pin the
framing helpers and that the results reconcile against the ported schemas.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.representation import representation_for
from interloper_pandas import DataFrameNormalizer

from interloper_assets.snapchat_ads import schemas
from interloper_assets.snapchat_ads.source import SnapchatAds, _frame_metadata, _frame_report


def _source() -> Any:
    return SnapchatAds(id="src-1", account_id="acc-1")  # ty: ignore[unknown-argument]


class TestSource:
    def test_all_assets_inherit_the_normalizer(self):
        for asset in _source().assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key

    def test_nine_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "ads",
            "campaigns",
            "ads_by_country",
            "videos_by_os",
            "ad_account_metadata",
            "ad_accounts_metadata",
            "ads_metadata",
            "ad_squads_metadata",
            "campaigns_metadata",
        }


class TestFraming:
    def test_report_strips_stats_prefix_and_stamps_date(self):
        # Non-dimensioned report row: metrics nested under "stats".
        rows = [{"id": "ad1", "type": "AD", "stats": {"impressions": "100", "spend": "5", "swipes": "3"}}]
        df = _frame_report(rows, dt.date(2026, 6, 10))
        assert {"id", "type", "impressions", "spend", "swipes", "date"} <= set(df.columns)
        assert "stats_impressions" not in df.columns
        assert df.loc[0, "date"] == dt.date(2026, 6, 10)

    def test_report_explodes_dimension_stats(self):
        # Dimensioned report row: one entry per dimension value under "dimension_stats".
        rows = [
            {
                "id": "ad1",
                "type": "AD",
                "dimension_stats": [
                    {"country": "us", "impressions": "10"},
                    {"country": "fr", "impressions": "7"},
                ],
            }
        ]
        df = _frame_report(rows, dt.date(2026, 6, 10))
        assert len(df) == 2
        assert set(df["country"]) == {"us", "fr"}
        assert (df["date"] == dt.date(2026, 6, 10)).all()

    def test_report_empty_rows(self):
        assert "date" in _frame_report([], dt.date(2026, 6, 10)).columns

    def test_metadata_flattens_nested_records(self):
        df = _frame_metadata([{"id": "ad1", "name": "My Ad", "creative_id": "c1"}])
        assert {"id", "name", "creative_id"} <= set(df.columns)


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.assets if type(a).key == key)

    def test_ads_row_conforms_after_roundtrip(self):
        child = self._child("ads")
        assert isinstance(child.normalizer, DataFrameNormalizer)
        rows = [{"id": "ad1", "type": "AD", "stats": {"impressions": "100", "spend": "5"}}]
        normalized = child.normalizer.normalize(_frame_report(rows, dt.date(2026, 6, 10)))
        out = representation_for(normalized).conformer.reconcile(normalized, schemas.Ads)
        assert int(out.loc[0, "impressions"]) == 100

    def test_ads_metadata_conforms_after_roundtrip(self):
        child = self._child("ads_metadata")
        normalized = child.normalizer.normalize(_frame_metadata([{"id": "ad1", "name": "My Ad"}]))
        representation_for(normalized).conformer.reconcile(normalized, schemas.AdsMetadata)  # must not raise
