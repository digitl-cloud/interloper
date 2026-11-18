"""Tests for ``SnapchatStatsNormalizer``, which flattens and explodes stats rows."""

from __future__ import annotations

import datetime as dt

from interloper_assets.snapchat_ads.connector.normalizer import SnapchatStatsNormalizer


class TestStatsNormalizer:
    def test_strips_stats_prefix_and_keeps_date(self):
        rows = [
            {"id": "ad1", "type": "AD", "stats": {"impressions": "100", "spend": "5"}, "date": dt.date(2026, 6, 10)}
        ]
        df = SnapchatStatsNormalizer().normalize(rows)
        assert {"id", "type", "impressions", "spend", "date"} <= set(df.columns)
        assert "stats_impressions" not in df.columns

    def test_explodes_dimension_stats(self):
        rows = [{"id": "ad1", "dimension_stats": [{"country": "us"}, {"country": "fr"}], "date": dt.date(2026, 6, 10)}]
        df = SnapchatStatsNormalizer().normalize(rows)
        assert len(df) == 2
        assert set(df["country"]) == {"us", "fr"}
        assert (df["date"] == dt.date(2026, 6, 10)).all()

    def test_empty(self):
        assert SnapchatStatsNormalizer().normalize([]).empty
