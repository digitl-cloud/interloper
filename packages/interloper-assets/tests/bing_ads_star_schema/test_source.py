"""Tests for the Bing Ads star schema placeholder: its wiring, ahead of any data."""

from __future__ import annotations

import datetime as dt

import interloper as il
import pytest
from interloper.errors import DataNotFoundError

from interloper_assets.bing_ads_star_schema.source import BingAdsStarSchema


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


@pytest.mark.parametrize(
    ("asset", "name", "key"),
    [
        ("fact_ad_performance", "ads_stats", "bing_ads.ads_stats"),
    ],
)
def test_raw_upstreams_fan_in_every_account_and_are_optional(asset: str, name: str, key: str | list[str]) -> None:
    relation = getattr(BingAdsStarSchema, asset).relations[name]
    assert (relation.key, relation.many, relation.optional) == (key, True, True)


def test_dimensions_are_built_from_the_facts() -> None:
    assert BingAdsStarSchema.sibling_bindings() == {
        "dim_campaigns": {"fact_ad_performance": "fact_ad_performance"},
        "dim_ads": {"fact_ad_performance": "fact_ad_performance"},
        "dim_accounts": {"fact_ad_performance": "fact_ad_performance"},
    }


def test_materializes_no_rows_yet() -> None:
    memory = il.MemoryDestination()
    star = BingAdsStarSchema(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))

    il.DAG(star).materialize(partition)

    for asset in star.assets:
        with pytest.raises(DataNotFoundError):
            memory.read(il.IOContext(asset=asset, partition_or_window=partition))
