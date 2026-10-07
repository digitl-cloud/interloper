"""Tests for the campaign performance analysis placeholder: its wiring over the star schemas and the matcher."""

from __future__ import annotations

import datetime as dt

import interloper as il
import pytest
from interloper.errors import DataNotFoundError

from interloper_assets.campaign_matcher.source import CampaignMatcher
from interloper_assets.campaign_performance_analysis.source import CampaignPerformanceAnalysis
from interloper_assets.facebook_ads_star_schema.source import FacebookAdsStarSchema
from interloper_assets.thetradedesk_star_schema.source import TheTradeDeskStarSchema


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


def test_performance_fans_in_every_star_schema_fact() -> None:
    relation = CampaignPerformanceAnalysis.campaign_performance.relations["performance"]  # ty: ignore[unresolved-attribute]
    assert relation.keys == ["*.fact_ad_performance", "*.fact_ad_group_performance", "*.fact_campaign_performance"]
    assert (relation.many, relation.optional) == (True, True)


def test_matches_is_the_single_optional_matcher_output() -> None:
    relation = CampaignPerformanceAnalysis.campaign_performance.relations["matches"]  # ty: ignore[unresolved-attribute]
    assert (relation.key, relation.many, relation.optional) == ("campaign_matcher.campaign_matches", False, True)


def test_dag_binds_the_star_schema_facts_and_the_matcher() -> None:
    memory = il.MemoryDestination()
    facebook = FacebookAdsStarSchema(destinations=[memory])
    trade_desk = TheTradeDeskStarSchema(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    analysis = CampaignPerformanceAnalysis(destinations=[memory])

    dag = il.DAG(facebook, trade_desk, matcher, analysis)

    assert set(dag.get_predecessors(analysis.campaign_performance.id)) == {
        facebook.fact_ad_performance.id,
        trade_desk.fact_ad_group_performance.id,
        matcher.campaign_matches.id,
    }


def test_materializes_no_rows_yet() -> None:
    memory = il.MemoryDestination()
    analysis = CampaignPerformanceAnalysis(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))

    il.DAG(analysis).materialize(partition)

    with pytest.raises(DataNotFoundError):
        memory.read(il.IOContext(asset=analysis.campaign_performance, partition_or_window=partition))
