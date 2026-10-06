"""Tests for the Amazon Ads star schema placeholder: its wiring, ahead of any data."""

from __future__ import annotations

import datetime as dt

import interloper as il
import pytest
from interloper.errors import DataNotFoundError

from interloper_assets.amazon_ads_star_schema.source import AmazonAdsStarSchema


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


@pytest.mark.parametrize(
    ("asset", "name", "key"),
    [
        (
            "fact_campaigns_stats",
            "campaigns_stats",
            [
                "amazon_ads.products_campaigns_stats",
                "amazon_ads.brands_campaigns_stats",
                "amazon_ads.display_campaigns_stats",
            ],
        ),
        ("dim_accounts", "profiles", "amazon_ads.profiles"),
    ],
)
def test_raw_upstreams_fan_in_every_account_and_are_optional(asset: str, name: str, key: str | list[str]) -> None:
    relation = getattr(AmazonAdsStarSchema, asset).relations[name]
    assert (relation.key, relation.many, relation.optional) == (key, True, True)


def test_dimensions_are_built_from_the_facts() -> None:
    assert AmazonAdsStarSchema.sibling_bindings() == {
        "dim_campaigns": {"fact_campaigns_stats": "fact_campaigns_stats"},
        "dim_accounts": {"fact_campaigns_stats": "fact_campaigns_stats"},
    }


def test_materializes_no_rows_yet() -> None:
    memory = il.MemoryDestination()
    star = AmazonAdsStarSchema(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))

    il.DAG(star).materialize(partition)

    for asset in star.assets:
        with pytest.raises(DataNotFoundError):
            memory.read(il.IOContext(asset=asset, partition_or_window=partition))
