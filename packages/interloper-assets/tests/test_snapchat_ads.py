"""Regression tests for the SnapchatAds source.

Stats rows nest metrics under a ``stats`` dict (flattened and de-prefixed) or,
for dimensioned reports, a ``dimension_stats`` list (exploded into one row per
dimension) — reshaped by ``SnapchatStatsNormalizer``. Entity records are
flattened by a configured ``DataFrameNormalizer``. These tests pin the
normalizers, that they survive the host→child spec round-trip (the AmazonAds
failure mode), and that results reconcile against the ported schemas.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from interloper.dag.base import DAG
from interloper.dag.spec import DAGSpec
from interloper.representation import representation_for
from interloper_pandas import DataFrameNormalizer

from interloper_assets.snapchat_ads import schemas
from interloper_assets.snapchat_ads.source import SnapchatAds, SnapchatStatsNormalizer, _with_date


def _source() -> Any:
    return SnapchatAds(id="src-1", account_id="acc-1")  # ty: ignore[unknown-argument]


class TestSource:
    def test_nine_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "ads_stats",
            "campaigns_stats",
            "ads_stats_by_country",
            "videos_stats_by_os",
            "ad_account",
            "ad_accounts",
            "ads",
            "ad_squads",
            "campaigns",
        }

    def test_stats_assets_use_stats_normalizer(self):
        by_key = {type(a).key: a for a in _source().assets}
        for key in ("ads_stats", "campaigns_stats", "ads_stats_by_country", "videos_stats_by_os"):
            assert isinstance(by_key[key].normalizer, SnapchatStatsNormalizer), key

    def test_entity_assets_use_flattening_normalizer(self):
        by_key = {type(a).key: a for a in _source().assets}
        for key in ("ads", "ad_squads", "campaigns", "ad_account"):
            norm = by_key[key].normalizer
            assert isinstance(norm, DataFrameNormalizer) and not isinstance(norm, SnapchatStatsNormalizer), key
            assert norm.flatten_max_level == 3
            assert norm.drop_na_columns is True


class TestStatsNormalizer:
    def test_strips_stats_prefix_and_keeps_date(self):
        rows = _with_date(
            [{"id": "ad1", "type": "AD", "stats": {"impressions": "100", "spend": "5"}}], dt.date(2026, 6, 10)
        )
        df = SnapchatStatsNormalizer().normalize(rows)
        assert {"id", "type", "impressions", "spend", "date"} <= set(df.columns)
        assert "stats_impressions" not in df.columns

    def test_explodes_dimension_stats(self):
        rows = _with_date(
            [{"id": "ad1", "dimension_stats": [{"country": "us"}, {"country": "fr"}]}],
            dt.date(2026, 6, 10),
        )
        df = SnapchatStatsNormalizer().normalize(rows)
        assert len(df) == 2
        assert set(df["country"]) == {"us", "fr"}
        assert (df["date"] == dt.date(2026, 6, 10)).all()

    def test_empty(self):
        assert SnapchatStatsNormalizer().normalize([]).empty


class TestSpecRoundtripAndReconcile:
    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.assets if type(a).key == key)

    def test_stats_normalizer_survives_roundtrip(self):
        # The custom subclass must reconstruct in the child (not degrade to the base).
        assert isinstance(self._child("ads_stats").normalizer, SnapchatStatsNormalizer)

    def test_entity_normalizer_survives_roundtrip(self):
        norm = self._child("ads").normalizer
        assert isinstance(norm, DataFrameNormalizer) and not isinstance(norm, SnapchatStatsNormalizer)
        assert norm.flatten_max_level == 3 and norm.drop_na_columns is True

    def test_ads_row_conforms_after_roundtrip(self):
        child = self._child("ads_stats")
        rows = _with_date([{"id": "ad1", "type": "AD", "stats": {"impressions": "100"}}], dt.date(2026, 6, 10))
        normalized = child.normalizer.normalize(rows)
        out = representation_for(normalized).conformer.reconcile(normalized, schemas.AdsStats)
        assert int(out.loc[0, "impressions"]) == 100

    def test_ads_entity_conforms_after_roundtrip(self):
        child = self._child("ads")
        normalized = child.normalizer.normalize([{"id": "ad1", "name": "My Ad"}])
        representation_for(normalized).conformer.reconcile(normalized, schemas.Ads)  # must not raise
