"""Tests for ``TheTradeDeskNormalizer``, which reads International-format reports."""

from __future__ import annotations

import datetime as dt

from interloper.representation import TableData

from interloper_assets.thetradedesk.connector import schemas
from interloper_assets.thetradedesk.connector.normalizer import TheTradeDeskNormalizer

# A CSV row as csv.DictReader yields it: every value a string, international format.
_ROW = {
    "Date": "10/07/2026",
    "Partner ID": "qz94j5z",
    "Ad Group": "AG, One",
    "Impressions": "1000",
    "TTD Cost (USD)": "12,34",
    "Player 25% Complete": "7",
    "Player 50% Complete": "4",
    "Sampled Tracked Impressions": "",
}


def _normalizer() -> TheTradeDeskNormalizer:
    return TheTradeDeskNormalizer(replace_empty_strings=True)


class TestNormalizer:
    """International-format values and % headers land on the schema columns."""

    def test_percent_headers(self):
        normalizer = _normalizer()
        assert normalizer.column_name("Player 25% Complete") == "player_25pct_complete"
        assert normalizer.column_name("TTD Cost (USD)") == "ttd_cost_usd"
        assert normalizer.column_name("Ad Group Base Bid CPM (Adv Currency)") == ("ad_group_base_bid_cpm_adv_currency")

    def test_decimal_commas_and_day_first_dates(self):
        df = _normalizer().normalize([_ROW])
        record = df.to_dict("records")[0]
        assert record["date"] == dt.date(2026, 7, 10)  # day-first, not October 7
        assert record["ttd_cost_usd"] == 12.34
        # Strings containing commas that are not numbers pass through untouched.
        assert record["ad_group"] == "AG, One"

    def test_row_conforms_to_schema(self):
        df = _normalizer().normalize([_ROW])
        assert set(df.columns) <= set(schemas.AdGroupsStats.model_fields)
        table = TableData.from_data(df)
        conformed = table.reconcile(schemas.AdGroupsStats)
        TableData.from_data(conformed).reconcile(schemas.AdGroupsStats, strict=True)  # must not raise
        record = conformed.to_dict("records")[0]
        assert record["impressions"] == 1000
        assert record["player_25pct_complete"] == 7
        assert record["sampled_tracked_impressions"] is None  # "" -> None -> NA
