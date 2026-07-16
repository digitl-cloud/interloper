"""Regression tests for the TheTradeDesk source.

MyReports are requested in "International" format: metric values carry decimal
commas ("12,34"), the date column is DD/MM/YYYY, and quartile headers spell
``%`` ("Player 25% Complete" → ``player_25pct_complete``). The custom
normalizer must convert all three so the generic casts succeed — and survive
the host→child DAG-spec round-trip.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation

from interloper_assets.thetradedesk import schemas
from interloper_assets.thetradedesk.source import TheTradeDesk, TheTradeDeskNormalizer

_ASSET_KEY = "ad_groups_stats"

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


def _source() -> Any:
    return TheTradeDesk(id="src-1", partner_id="qz94j5z")  # ty: ignore[unknown-argument]


def _normalizer() -> TheTradeDeskNormalizer:
    return TheTradeDeskNormalizer(replace_empty_strings=True)


class TestNormalizer:
    """International-format values and % headers land on the schema columns."""

    def test_percent_headers(self):
        normalizer = _normalizer()
        assert normalizer.column_name("Player 25% Complete") == "player_25pct_complete"
        assert normalizer.column_name("TTD Cost (USD)") == "ttd_cost_usd"
        assert normalizer.column_name("Ad Group Base Bid CPM (Adv Currency)") == (
            "ad_group_base_bid_cpm_adv_currency"
        )

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
        conformer = Representation.of(df).conformer
        conformed = conformer.reconcile(df, schemas.AdGroupsStats)
        conformer.validate(conformed, schemas.AdGroupsStats)  # must not raise
        record = conformed.to_dict("records")[0]
        assert record["impressions"] == 1000
        assert record["player_25pct_complete"] == 7
        assert record["sampled_tracked_impressions"] is None  # "" -> None -> NA

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        assert len(src.assets) == 1
        for asset in src.assets:
            assert isinstance(asset.normalizer, TheTradeDeskNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_custom_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.assets if type(a).key == _ASSET_KEY)

        normalizer = child_asset.normalizer
        assert isinstance(normalizer, TheTradeDeskNormalizer)
        assert normalizer.replace_empty_strings is True
        # International-format handling must survive in the child pod.
        record = normalizer.normalize([_ROW]).to_dict("records")[0]
        assert record["date"] == dt.date(2026, 7, 10)
        assert record["ttd_cost_usd"] == 12.34
