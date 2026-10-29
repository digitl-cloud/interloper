"""Tests for the The Trade Desk star schema placeholder: its wiring, ahead of any data."""

from __future__ import annotations

import datetime as dt

import interloper as il
import pytest
from interloper.errors import DataNotFoundError

from interloper_assets.thetradedesk.star_schema.source import TheTradeDeskStarSchema


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


@pytest.mark.parametrize(
    ("asset", "name", "key"),
    [
        ("fact_ad_group_performance", "ad_groups_stats", "the_trade_desk.ad_groups_stats"),
    ],
)
def test_raw_upstreams_fan_in_every_account_and_are_optional(asset: str, name: str, key: str | list[str]) -> None:
    relation = getattr(TheTradeDeskStarSchema, asset).relations[name]
    assert (relation.key, relation.many, relation.optional) == (key, True, True)


def test_dimensions_are_unpartitioned_placeholders() -> None:
    star = TheTradeDeskStarSchema()
    dimensions = {asset.key: asset for asset in star.assets if asset.key.startswith("dim_")}

    assert TheTradeDeskStarSchema.sibling_bindings() == {}
    assert {key: asset.partitioning for key, asset in dimensions.items()} == dict.fromkeys(
        ("dim_campaigns", "dim_ad_groups", "dim_accounts")
    )


def test_materializes_no_rows_yet() -> None:
    memory = il.MemoryDestination()
    star = TheTradeDeskStarSchema(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))

    il.DAG(star).materialize(partition)

    for asset in star.assets:
        window = partition.window if asset.partitioning is not None else None
        with pytest.raises(DataNotFoundError):
            memory.read(il.IOContext(asset=asset, window=window))
