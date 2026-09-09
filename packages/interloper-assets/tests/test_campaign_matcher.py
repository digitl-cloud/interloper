"""Tests for the campaign matcher source: a many-valued wildcard upstream over every `campaigns` asset."""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Any

import interloper as il
import pytest
from interloper.errors import ConfigError

from interloper_assets.campaign_matcher.source import CampaignMatcher, normalise

PARTITION = il.TimePartitionConfig(column="date")


class CampaignsSchema(il.Schema):
    date: dt.date | None
    id: str
    name: str


class TiktokLikeCampaignsSchema(il.Schema):
    date: dt.date | None
    campaign_id: str
    campaign_name: str


class NeitherSpellingCampaignsSchema(il.Schema):
    date: dt.date | None
    unrelated_field: str


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


def _tiktok_like_connector(source_key: str, names: list[str]) -> type[il.Source]:
    @il.source(key=source_key)
    class Connector(il.Source):
        @il.asset(schema=TiktokLikeCampaignsSchema, partitioning=PARTITION, tags=["Entity"])
        def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [
                {"date": context.partition_date, "campaign_id": f"{source_key}-{i}", "campaign_name": name}
                for i, name in enumerate(names)
            ]

    return Connector


def _neither_spelling_connector(source_key: str, count: int) -> type[il.Source]:
    @il.source(key=source_key)
    class Connector(il.Source):
        @il.asset(schema=NeitherSpellingCampaignsSchema, partitioning=PARTITION, tags=["Entity"])
        def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [{"date": context.partition_date, "unrelated_field": str(i)} for i in range(count)]

    return Connector


@pytest.fixture(autouse=True)
def _clear_memory() -> None:
    il.MemoryDestination.clear()


def test_relation_is_a_many_valued_wildcard() -> None:
    relation = CampaignMatcher.campaign_matches.relations["campaigns"]  # ty: ignore[unresolved-attribute]
    assert (relation.kind, relation.key, relation.many, relation.optional) == ("asset", "*.campaigns", True, False)


def _matches(matcher: il.Source, *connectors: il.Source) -> list[dict[str, Any]]:
    memory = matcher.destinations[0]
    partition = il.TimePartition(dt.date(2026, 9, 1))
    il.DAG(*connectors, matcher).materialize(partition)
    return memory.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))


def test_dag_binds_every_campaigns_asset_and_matches_each_leg() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["Summer Sale ", "brand"])(destinations=[memory])
    tt = _connector("tt_like", ["summer sale"])(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    dag = il.DAG(fb, tt, matcher)
    assert set(dag.get_predecessors(matcher.campaign_matches.id)) == {fb.campaigns.id, tt.campaigns.id}

    rows = _matches(matcher, fb, tt)

    assert sorted(r["canonical_name"] for r in rows) == ["brand", "summer sale", "summer sale"]
    assert {r["platform"] for r in rows} == {"fb_like", "tt_like"}
    summer = [r for r in rows if r["canonical_name"] == "summer sale"]
    assert len({r["match_id"] for r in summer}) == 1
    assert all(r["similarity"] == 1.0 for r in rows)


def test_platform_and_account_identify_the_campaign_owner() -> None:
    memory = il.MemoryDestination()

    @il.source(key="acct_like")
    class Connector(il.Source):
        account_id: str = il.InputField(default="", discriminator=True)

        @il.asset(schema=CampaignsSchema, partitioning=PARTITION, tags=["Entity"])
        def campaigns(self, context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [{"date": context.partition_date, "id": "1", "name": "x"}]

    connector = Connector(account_id="act_42", destinations=[memory])  # ty: ignore[unknown-argument]
    rows = _matches(CampaignMatcher(destinations=[memory]), connector)

    assert (rows[0]["platform"], rows[0]["account"], rows[0]["campaign_id"]) == ("acct_like", "act_42", "1")


def test_match_ids_are_stable_across_runs() -> None:
    memory = il.MemoryDestination()
    first = _matches(CampaignMatcher(destinations=[memory]), _connector("fb_like", ["Brand"])(destinations=[memory]))
    il.MemoryDestination.clear()
    second = _matches(CampaignMatcher(destinations=[memory]), _connector("tt_like", ["brand"])(destinations=[memory]))
    assert first[0]["match_id"] == second[0]["match_id"]


class TestNormalise:
    def test_case_whitespace_punctuation_and_unicode_fold_away(self) -> None:
        assert normalise("  Summer   Sale!  ") == "summer sale"
        assert normalise("Été-2026 / Brand") == "été 2026 brand"
        assert normalise("ＢＲＡＮＤ") == "brand"

    def test_key_pattern_selects_the_identifying_part(self) -> None:
        pattern = re.compile(r"^[a-z]+_(?P<key>[a-z0-9]+)_")
        assert normalise("acme_summer26_awareness_de", pattern) == "summer26"
        assert normalise("no convention here", pattern) == "no convention here"


def test_key_pattern_on_the_source_matches_by_convention() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["fb_summer26_awareness"])(destinations=[memory])
    tt = _connector("tt_like", ["tt_summer26_video"])(destinations=[memory])
    matcher = CampaignMatcher(key_pattern=r"^[a-z]+_(?P<key>[a-z0-9]+)_", destinations=[memory])  # ty: ignore[unknown-argument]

    rows = _matches(matcher, fb, tt)

    assert {r["canonical_name"] for r in rows} == {"summer26"}
    assert len({r["match_id"] for r in rows}) == 1


def test_similarity_threshold_merges_near_duplicates() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["Summer Sale 2026"])(destinations=[memory])
    tt = _connector("tt_like", ["Summer Sale 2O26"])(destinations=[memory])

    strict = _matches(CampaignMatcher(destinations=[memory]), fb, tt)
    assert len({r["match_id"] for r in strict}) == 2

    il.MemoryDestination.clear()
    lenient = _matches(CampaignMatcher(similarity_threshold=0.9, destinations=[memory]), fb, tt)  # ty: ignore[unknown-argument]
    assert len({r["match_id"] for r in lenient}) == 1
    assert {r["canonical_name"] for r in lenient} == {"summer sale 2026"}
    assert sorted(r["similarity"] for r in lenient) == [pytest.approx(0.9375), 1.0]


def test_leg_without_data_is_skipped_not_fatal() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["x"])(destinations=[memory])
    tt = _connector("tt_like", ["y"])(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    tt.campaigns.materializable = False  # nothing written for tt: its leg reads as None
    partition = il.TimePartition(dt.date(2026, 9, 1))
    il.DAG(fb, tt, matcher).materialize(partition)
    rows = memory.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))
    assert [r["campaign_id"] for r in rows] == ["fb_like-0"]


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


def test_matches_across_connector_schemas_with_different_field_names() -> None:
    memory = il.MemoryDestination()
    fb = _connector("fb_like", ["Summer Sale"])(destinations=[memory])
    tt = _tiktok_like_connector("tt_like", ["Brand Awareness"])(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))
    il.DAG(fb, tt, matcher).materialize(partition)
    rows = memory.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))
    by_id = {r["campaign_id"]: r for r in rows}
    assert by_id["fb_like-0"]["canonical_name"] == "summer sale"
    assert by_id["tt_like-0"]["canonical_name"] == "brand awareness"


def test_a_row_with_neither_spelling_is_still_emitted_with_empty_placeholders() -> None:
    """Pins current behaviour: the placeholder matcher does not filter rows missing both id/name spellings."""
    memory = il.MemoryDestination()
    fb = _neither_spelling_connector("fb_like", 1)(destinations=[memory])
    matcher = CampaignMatcher(destinations=[memory])
    partition = il.TimePartition(dt.date(2026, 9, 1))
    il.DAG(fb, matcher).materialize(partition)
    rows = memory.read(il.IOContext(asset=matcher.campaign_matches, partition_or_window=partition))
    assert len(rows) == 1
    assert rows[0]["campaign_id"] == ""
    assert rows[0]["canonical_name"] == ""


def test_example_manifest_loads_and_wires_without_redeclaring_what_cascades(monkeypatch: pytest.MonkeyPatch) -> None:
    """The shipped manifest is what a user writes: the job's destination reaches every asset by cascade."""
    pytest.importorskip("interloper_google_cloud")
    for name in (
        "FACEBOOK_ADS_ACCESS_TOKEN",
        "FACEBOOK_ADS_ACCOUNT_ID",
        "FACEBOOK_ADS_APP_ID",
        "FACEBOOK_ADS_APP_SECRET",
        "GCP_SERVICE_ACCOUNT_KEY",
        "TIKTOK_ADS_ACCESS_TOKEN",
        "TIKTOK_ADS_ADVERTISER_ID",
    ):
        monkeypatch.setenv(name, "placeholder")
    manifest = Path(__file__).parents[3] / "examples" / "campaign_matcher.yaml"

    dag = il.DAG.from_spec_file(manifest)
    matches = next(operation for operation in dag.operations if operation.key == "campaign_matches")
    assert isinstance(matches, il.Asset)

    assert "destinations" not in manifest.read_text().split("key: campaign_matcher")[1].split("assets:")[0]
    assert sorted(dag.operation_map[i].qualified_key for i in dag.get_predecessors(matches.id)) == [
        "facebook_ads.campaigns",
        "tiktok_ads.campaigns",
    ]
    assert [destination.key for destination in matches.destinations] == ["bigquery_destination"]
