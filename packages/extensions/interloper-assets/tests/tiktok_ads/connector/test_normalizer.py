"""Tests for ``TiktokStatsNormalizer``, which flattens an integrated-report row."""

from __future__ import annotations

from interloper.representation import TableData

from interloper_assets.tiktok_ads.connector import schemas
from interloper_assets.tiktok_ads.connector.normalizer import TiktokStatsNormalizer


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

    def test_a_metric_that_does_not_apply_is_null(self):
        rows = [
            {
                "dimensions": {"ad_id": "456", "stat_time_day": "2026-06-14 00:00:00"},
                "metrics": {"cost_per_secondary_goal_result": "-", "spend": "12.50"},
            },
            {
                "dimensions": {"ad_id": "789", "stat_time_day": "2026-06-14 00:00:00"},
                "metrics": {"cost_per_secondary_goal_result": "0.00", "spend": "-"},
            },
        ]
        df = TiktokStatsNormalizer().normalize(rows)
        reconciled = TableData.from_data(df).reconcile(schemas.AdsStats)
        assert reconciled["cost_per_secondary_goal_result"].isna().tolist() == [True, False]
        assert reconciled.loc[1, "cost_per_secondary_goal_result"] == 0.0
        assert reconciled["spend"].isna().tolist() == [False, True]

    def test_a_dimension_spelled_as_a_dash_is_kept(self):
        rows = [{"dimensions": {"ad_id": "456", "country_code": "-"}, "metrics": {"spend": "1"}}]
        assert TiktokStatsNormalizer().normalize(rows).loc[0, "country_code"] == "-"
