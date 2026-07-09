"""Regression tests for the TikTok Ads source.

TikTok integrated reports return each row as ``{"dimensions": {...},
"metrics": {...}}``; the source's ``TiktokStatsNormalizer`` merges both into a
flat record (de-prefixed) before column normalization. Entity records carry
list/dict fields (``ad_texts``, ``image_ids``, …) that the flat schemas type as
strings; the conformer JSON-encodes those nested values when casting to the
declared ``str`` type, so the entity assets need no bespoke normalizer. These
tests pin both reshapes, that every asset gets the right
normalizer, and that the normalizer survives the host→child spec round-trip and
reconciles against the ported schemas.
"""

from __future__ import annotations

from typing import Any

from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.tiktok_ads import schemas
from interloper_assets.tiktok_ads.source import TiktokAds, TiktokStatsNormalizer


def _source() -> Any:
    return TiktokAds(id="src-1", advertiser_id="123")  # ty: ignore[unknown-argument]


class TestSourceNormalizer:
    def test_all_eight_assets_present(self):
        keys = {type(a).key for a in _source().assets}
        assert keys == {
            "ads_stats",
            "ads_stats_by_country",
            "ads_stats_by_age_gender",
            "ads_stats_by_platform",
            "videos_stats_by_platform",
            "ads",
            "campaigns",
            "advertisers",
        }

    def test_stats_assets_use_the_stats_normalizer(self):
        for key in (
            "ads_stats",
            "ads_stats_by_country",
            "ads_stats_by_age_gender",
            "ads_stats_by_platform",
            "videos_stats_by_platform",
        ):
            asset = next(a for a in _source().assets if type(a).key == key)
            assert isinstance(asset.normalizer, TiktokStatsNormalizer), key

    def test_entity_assets_use_the_base_normalizer(self):
        # Entity assets use a plain DataFrameNormalizer (not the stats reshape);
        # nested str-field encoding is handled downstream by the conformer.
        for key in ("ads", "campaigns", "advertisers"):
            asset = next(a for a in _source().assets if type(a).key == key)
            assert isinstance(asset.normalizer, DataFrameNormalizer), key
            assert not isinstance(asset.normalizer, TiktokStatsNormalizer), key
            assert asset.normalizer.drop_na_columns is True, key


class TestStatsNormalizer:
    """Merging the dimensions/metrics envelope is the crux of the report port."""

    def test_dimensions_and_metrics_merge_into_flat_columns(self):
        rows = [
            {
                "dimensions": {"ad_id": "456", "stat_time_day": "2026-06-14 00:00:00"},
                "metrics": {"spend": "12.50", "clicks": "3"},
                "date": "2026-06-14",
            }
        ]
        df = TiktokStatsNormalizer().normalize(rows)
        assert "ad_id" in df.columns and "spend" in df.columns
        assert "dimensions" not in df.columns and "metrics" not in df.columns
        assert df.loc[0, "ad_id"] == "456"
        assert df.loc[0, "spend"] == "12.50"
        assert df.loc[0, "date"] == "2026-06-14"


class TestSpecRoundtripAndReconcile:
    """Normalizers survive the host→child spec round-trip and rows reconcile."""

    def _child(self, key: str) -> Any:
        src = _source()
        asset = next(a for a in src.assets if type(a).key == key)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        return next(a for a in child_dag.assets if type(a).key == key)

    def test_report_row_reconciles_against_ads_schema(self):
        child = self._child("ads_stats")
        assert isinstance(child.normalizer, TiktokStatsNormalizer)
        rows = [
            {
                "dimensions": {"ad_id": "456", "stat_time_day": "2026-06-14 00:00:00"},
                "metrics": {"spend": "12.50", "clicks": "3", "cpc": "4.16"},
                "date": "2026-06-14",
            }
        ]
        normalized = child.normalizer.normalize(rows)
        reconciled = Representation.of(normalized).conformer.reconcile(normalized, schemas.AdsStats)
        assert float(reconciled.loc[0, "spend"]) == 12.5
        assert int(reconciled.loc[0, "clicks"]) == 3

    def test_entity_row_reconciles_against_ads_schema(self):
        child = self._child("ads")
        assert isinstance(child.normalizer, DataFrameNormalizer)
        rows = [{"ad_id": "456", "ad_name": "My Ad", "ad_texts": ["hello"], "image_ids": ["img1"]}]
        normalized = child.normalizer.normalize(rows)
        # list fields land as JSON strings on the str-typed schema columns
        reconciled = Representation.of(normalized).conformer.reconcile(normalized, schemas.Ads)
        assert reconciled.loc[0, "ad_texts"] == '["hello"]'
        assert reconciled.loc[0, "image_ids"] == '["img1"]'
