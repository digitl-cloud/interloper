"""Tests for the campaign matcher source: a many-valued wildcard upstream over every `campaigns` asset."""

from __future__ import annotations

import datetime as dt
from typing import Any

import interloper as il
import pytest
from interloper.errors import ConfigError

from interloper_assets.campaign_matcher.source import CampaignMatcher

PARTITION = il.TimePartitionConfig(column="date")


class CampaignsSchema(il.Schema):
    date: dt.date | None
    id: str
    name: str


def _connector(source_key: str, names: list[str]) -> type[il.Source]:
    @il.source(key=source_key)
    class Connector(il.Source):
        @il.asset(schema=CampaignsSchema, partitioning=PARTITION, tags=["Entity"])
        def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [
                {"date": context.partition_date, "id": f"{source_key}-{i}", "name": name}
                for i, name in enumerate(names)
            ]

    return Connector


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


def test_relation_is_a_many_valued_wildcard() -> None:
    relation = CampaignMatcher.campaign_matches.relations["campaigns"]  # ty: ignore[unresolved-attribute]
    assert (relation.kind, relation.key, relation.many, relation.optional) == ("asset", "*.campaigns", True, False)


def test_dag_binds_every_campaigns_asset_and_matches_each_leg() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["Summer Sale ", "brand"])(destinations=[memory])
    tt = _connector("tt_like", ["summer sale"])(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    dag = il.DAG(fb, tt, matcher)
    assert set(dag.get_predecessors(matcher.campaign_matches.id)) == {fb.campaigns.id, tt.campaigns.id}
    partition = il.TimePartition(dt.date(2026, 9, 1))
    dag.materialize(partition)
    rows = memory.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))
    assert sorted(r["canonical_name"] for r in rows) == ["brand", "summer sale", "summer sale"]
    assert {r["source_key"] for r in rows} == {"fb_like", "tt_like"}


def test_leg_without_data_is_skipped_not_fatal() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["x"])(destinations=[memory])
    tt = _connector("tt_like", ["y"])(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    tt.campaigns.materializable = False  # nothing written for tt: its leg reads as None
    partition = il.TimePartition(dt.date(2026, 9, 1))
    il.DAG(fb, tt, matcher).materialize(partition)
    rows = memory.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))
    assert [r["source_key"] for r in rows] == ["fb_like"]


def test_matcher_alone_reads_bound_upstreams_read_only() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["x"])(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))
    il.DAG(fb).materialize(partition)
    matcher = CampaignMatcher(destinations=[memory])
    matcher.campaign_matches.bind("campaigns", fb.campaigns)
    dag = il.DAG(matcher)
    assert dag.operation_map[fb.campaigns.id].materializable is False
    dag.materialize(partition)
    assert len(memory.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))) == 1


def test_matcher_with_no_campaigns_in_dag_is_a_build_error() -> None:
    with pytest.raises(ConfigError, match="campaigns"):
        il.DAG(CampaignMatcher(destinations=[il.MemoryDestination()]))
