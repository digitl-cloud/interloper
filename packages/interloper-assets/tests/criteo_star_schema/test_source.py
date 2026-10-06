"""Tests for the Criteo star schema placeholder: its wiring, ahead of any data."""

from __future__ import annotations

import datetime as dt

import interloper as il
import pytest
from interloper.errors import DataNotFoundError

from interloper_assets.criteo_star_schema.source import CriteoStarSchema


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


@pytest.mark.parametrize(
    ("asset", "name", "key"),
    [
        ("fact_ads_stats", "ads_stats", "criteo.ads_stats"),
        ("dim_campaigns", "campaigns", "criteo.campaigns_stats"),
    ],
)
def test_raw_upstreams_fan_in_every_account_and_are_optional(asset: str, name: str, key: str | list[str]) -> None:
    relation = getattr(CriteoStarSchema, asset).relations[name]
    assert (relation.key, relation.many, relation.optional) == (key, True, True)


def test_dimensions_are_built_from_the_facts() -> None:
    assert CriteoStarSchema.sibling_bindings() == {
        "dim_campaigns": {"fact_ads_stats": "fact_ads_stats"},
        "dim_ads": {"fact_ads_stats": "fact_ads_stats"},
        "dim_accounts": {"fact_ads_stats": "fact_ads_stats"},
    }


def test_materializes_no_rows_yet() -> None:
    memory = il.MemoryDestination()
    star = CriteoStarSchema(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))

    il.DAG(star).materialize(partition)

    for asset in star.assets:
        with pytest.raises(DataNotFoundError):
            memory.read(il.IOContext(asset=asset, partition_or_window=partition))
