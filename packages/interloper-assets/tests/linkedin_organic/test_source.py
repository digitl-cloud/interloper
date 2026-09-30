"""Regression tests for the LinkedIn Organic source.

The statistics endpoints date their rows only by the request (lifetime follower
counts) or by epoch-millisecond ``timeRange`` instants (page and share
statistics), so every asset stamps ``date``; the source's
``LinkedinOrganicNormalizer`` flattens the nested ``totalPageStatistics`` /
``totalShareStatistics`` blocks to their full vendor path and converts the
instants to UTC datetimes. Requests are written as raw Rest.li 2.0 query strings
and taxonomies follow ``paging.links``; both are pinned against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import httpx2
import interloper as il
from interloper.asset.context import ExecutionContext
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation

from interloper_assets.linkedin_organic.connection import LinkedinOrganicConnection
from interloper_assets.linkedin_organic.source import LinkedinOrganic, LinkedinOrganicNormalizer

REPORT_ASSETS = ("followers_stats", "page_stats", "share_stats")
ENTITY_ASSETS = ("industries", "job_functions", "seniorities")
DAY = dt.date(2026, 9, 15)
DAY_START_MILLIS = 1789430400000
DAY_END_MILLIS = 1789516800000


def _source(connection: LinkedinOrganicConnection | None = None) -> Any:
    if connection is None:
        return LinkedinOrganic(id="src-1", organization_id="2414183")
    return LinkedinOrganic(id="src-1", organization_id="2414183", connection=connection)


def _asset(source: Any, key: str) -> Any:
    return next(a for a in source.assets if type(a).key == key)


def _faked_source(handler: Any) -> Any:
    connection = LinkedinOrganicConnection(client_id="cid", client_secret="secret", refresh_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(
        "https://api.linkedin.com/rest", transport=httpx2.MockTransport(handler)
    )
    return _source(connection)


async def _run(source: Any, key: str) -> list[dict[str, Any]]:
    asset = _asset(source, key)
    context = ExecutionContext(
        asset_key=asset.key,
        partitioning=asset.partitioning,
        partition_or_window=il.TimePartition(value=DAY),
    )
    return await asset.data(context=context)


def _child(key: str) -> Any:
    source = _source()
    asset = _asset(source, key)
    spec_json = DAG(source).mini_dag(asset.id).to_spec().model_dump(mode="json")
    child_dag = DAGSpec(**spec_json).reconstruct()
    return next(a for a in child_dag.operations if type(a).key == key)


def _reconcile(key: str, rows: list[dict[str, Any]]) -> Any:
    child = _child(key)
    normalized = child.normalizer.normalize([{**row, "date": DAY} for row in rows])
    return Representation.of(normalized).reconcile(child.schema).iloc[0]


FOLLOWER_ROW = {
    "followerCountsBySeniority": [
        {"followerCounts": {"organicFollowerCount": 4, "paidFollowerCount": 0}, "seniority": "urn:li:seniority:2"}
    ],
    "followerCountsByAssociationType": [],
    "organizationalEntity": "urn:li:organization:2414183",
}

PAGE_ROW = {
    "organization": "urn:li:organization:2414183",
    "timeRange": {"start": DAY_START_MILLIS, "end": DAY_END_MILLIS},
    "totalPageStatistics": {
        "clicks": {
            "desktopCustomButtonClickCounts": [{"customButtonType": "VISIT_WEBSITE", "clicks": 3}],
            "mobileCustomButtonClickCounts": [],
        },
        "views": {
            "allPageViews": {"pageViews": 17, "uniquePageViews": 9},
            "careersPageViews": {"pageViews": 2, "uniquePageViews": 1},
            "desktopLifeAtPageViews": {"pageViews": 1},
        },
    },
}

SHARE_ROW = {
    "organizationalEntity": "urn:li:organization:2414183",
    "timeRange": {"start": DAY_START_MILLIS, "end": DAY_END_MILLIS},
    "totalShareStatistics": {
        "clickCount": 12,
        "engagement": 0.0075,
        "likeCount": -1,
        "commentCount": 2,
        "shareCount": 0,
        "impressionCount": 331,
        "uniqueImpressionsCount": 203,
    },
}

INDUSTRY_ROW = {
    "id": 96,
    "name": {"localized": {"en_US": "IT Services and IT Consulting", "de_DE": "IT-Dienstleistungen"}},
    "parentIndustries": ["urn:li:industry:6"],
    "childrenIndustries": [],
}

FUNCTION_ROW = {
    "id": 22,
    "name": {"localized": {"en_US": "Purchasing"}, "preferredLocale": {"country": "US", "language": "en"}},
}


class TestSourceNormalizer:
    def test_asset_keys(self):
        assert {type(a).key for a in _source().assets} == set(REPORT_ASSETS + ENTITY_ASSETS)

    def test_every_asset_uses_the_source_normalizer(self):
        for key in REPORT_ASSETS + ENTITY_ASSETS:
            normalizer = _asset(_source(), key).normalizer
            assert isinstance(normalizer, LinkedinOrganicNormalizer), key
            assert normalizer.flatten_max_level == 3, key
            assert normalizer.epoch_columns == ["time_range_start", "time_range_end"], key

    def test_normalizer_survives_the_roundtrip(self):
        for key in REPORT_ASSETS + ENTITY_ASSETS:
            child = _child(key).normalizer
            assert isinstance(child, LinkedinOrganicNormalizer), key
            assert child.model_dump() == _asset(_source(), key).normalizer.model_dump(), key


class TestReconcile:
    def test_follower_facets_land_as_json(self):
        row = _reconcile("followers_stats", [FOLLOWER_ROW])
        assert row["organizational_entity"] == "urn:li:organization:2414183"
        assert '"urn:li:seniority:2"' in row["follower_counts_by_seniority"]
        assert row["date"] == DAY

    def test_page_views_flatten_to_the_full_vendor_path(self):
        row = _reconcile("page_stats", [PAGE_ROW])
        assert int(row["total_page_statistics_views_all_page_views_page_views"]) == 17
        assert int(row["total_page_statistics_views_all_page_views_unique_page_views"]) == 9
        assert int(row["total_page_statistics_views_careers_page_views_page_views"]) == 2
        assert int(row["total_page_statistics_views_desktop_life_at_page_views_page_views"]) == 1
        assert '"VISIT_WEBSITE"' in row["total_page_statistics_clicks_desktop_custom_button_click_counts"]
        assert row["time_range_start"] == dt.datetime(2026, 9, 15, tzinfo=dt.timezone.utc)
        assert row["time_range_end"] == dt.datetime(2026, 9, 16, tzinfo=dt.timezone.utc)
        assert row["date"] == DAY

    def test_share_statistics_are_typed(self):
        row = _reconcile("share_stats", [SHARE_ROW])
        assert int(row["total_share_statistics_impression_count"]) == 331
        assert int(row["total_share_statistics_unique_impressions_count"]) == 203
        assert int(row["total_share_statistics_like_count"]) == -1
        assert float(row["total_share_statistics_engagement"]) == 0.0075

    def test_industry_names_flatten_per_locale(self):
        row = _reconcile("industries", [INDUSTRY_ROW])
        assert int(row["id"]) == 96
        assert row["name_localized_en_us"] == "IT Services and IT Consulting"
        assert row["name_localized_de_de"] == "IT-Dienstleistungen"
        assert row["parent_industries"] == '["urn:li:industry:6"]'

    def test_function_carries_its_preferred_locale(self):
        row = _reconcile("job_functions", [FUNCTION_ROW])
        assert row["name_localized_en_us"] == "Purchasing"
        assert row["name_preferred_locale_country"] == "US"
        assert row["name_preferred_locale_language"] == "en"


class TestPartitionColumns:
    def test_every_asset_partitions_on_a_stamped_date(self):
        for key in REPORT_ASSETS + ENTITY_ASSETS:
            asset = _asset(_source(), key)
            assert asset.partitioning.column == "date", key
            assert asset.schema.model_fields["date"].annotation == (dt.date | None), key

    def test_no_schema_declares_a_second_day(self):
        for key in REPORT_ASSETS + ENTITY_ASSETS:
            fields = _asset(_source(), key).schema.model_fields
            days = [name for name, field in fields.items() if field.annotation == (dt.date | None)]
            assert days == ["date"], key

    def test_tags_follow_the_grain(self):
        for key in REPORT_ASSETS:
            assert _asset(_source(), key).tags == ["Report"], key
        for key in ENTITY_ASSETS:
            assert _asset(_source(), key).tags == ["Entity"], key


class TestRequests:
    async def test_daily_statistics_send_a_raw_utc_day_window(self):
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(200, json={"elements": [SHARE_ROW]})

        rows = await _run(_faked_source(handler), "share_stats")
        assert rows == [{**SHARE_ROW, "date": DAY}]
        assert seen[0].url.path == "/rest/organizationalEntityShareStatistics"
        query = seen[0].url.query.decode()
        assert "q=organizationalEntity&organizationalEntity=urn%3Ali%3Aorganization%3A2414183" in query
        assert (
            f"timeIntervals=(timeGranularityType:DAY,timeRange:(start:{DAY_START_MILLIS},end:{DAY_END_MILLIS}))"
            in query
        )

    async def test_page_statistics_use_the_organization_finder(self):
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(200, json={"elements": [PAGE_ROW]})

        await _run(_faked_source(handler), "page_stats")
        assert seen[0].url.path == "/rest/organizationPageStatistics"
        assert (
            seen[0]
            .url.query.decode()
            .startswith("q=organization&organization=urn%3Ali%3Aorganization%3A2414183&timeIntervals=")
        )

    async def test_taxonomy_follows_next_links_until_none(self):
        seen: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(str(request.url))
            if "start=" not in str(request.url):
                links = [{"rel": "next", "href": "/rest/industryTaxonomyVersions/DEFAULT/industries?start=1&count=1"}]
                return httpx2.Response(200, json={"elements": [{"id": 1}], "paging": {"links": links}})
            links = [{"rel": "prev", "href": "/rest/industryTaxonomyVersions/DEFAULT/industries?start=0&count=1"}]
            return httpx2.Response(200, json={"elements": [{"id": 2}], "paging": {"links": links}})

        rows = await _run(_faked_source(handler), "industries")
        assert [row["id"] for row in rows] == [1, 2]
        assert seen == [
            "https://api.linkedin.com/rest/industryTaxonomyVersions/DEFAULT/industries",
            "https://api.linkedin.com/rest/industryTaxonomyVersions/DEFAULT/industries?start=1&count=1",
        ]
