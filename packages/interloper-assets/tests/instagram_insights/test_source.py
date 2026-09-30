"""Regression tests for the Instagram Insights source.

Account insights come back metric-major, as ``values`` time series
(``account_stats``), ``total_value`` totals (``engagement_stats``) or
``total_value.breakdowns`` results (``followers_stats_by_*``); each asset
pivots its shape into rows. ``media_stats`` lifts every media item's nested
insights into columns, requesting each item's insights with the metric set of
its product type. No response carries a report day, so every asset stamps ``date``.
Requests run against a mock transport.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

import httpx2
import interloper as il
from interloper.asset.context import ExecutionContext
from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.instagram_insights import constants, schemas
from interloper_assets.instagram_insights.connection import InstagramInsightsConnection
from interloper_assets.instagram_insights.source import InstagramInsights

REPORT_ASSETS = (
    "account_stats",
    "engagement_stats",
    "followers_stats_by_age_gender",
    "followers_stats_by_city",
    "followers_stats_by_country",
    "media_stats",
)
ENTITY_ASSETS = ("media", "profiles")
DAY = dt.date(2026, 9, 15)
GRAPH = "https://graph.facebook.com/v26.0"


def _source(connection: InstagramInsightsConnection | None = None) -> Any:
    if connection is None:
        return InstagramInsights(id="src-1", account_id="1784")
    return InstagramInsights(id="src-1", account_id="1784", connection=connection)


def _asset(source: Any, key: str) -> Any:
    return next(a for a in source.assets if type(a).key == key)


def _faked_source(handler: Any) -> Any:
    connection = InstagramInsightsConnection(client_id="cid", client_secret="secret", refresh_token="token")
    connection.__dict__["client"] = il.AsyncRESTClient(GRAPH, transport=httpx2.MockTransport(handler))
    return _source(connection)


def _serving(body: dict[str, Any], urls: list[httpx2.URL] | None = None) -> Any:
    def handler(request: httpx2.Request) -> httpx2.Response:
        if urls is not None:
            urls.append(request.url)
        return httpx2.Response(200, json=body)

    return handler


async def _run(source: Any, key: str, day: dt.date = DAY) -> list[dict[str, Any]]:
    asset = _asset(source, key)
    context = ExecutionContext(
        asset_key=asset.key,
        partitioning=asset.partitioning,
        partition_or_window=il.TimePartition(value=day),
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
    return Representation.of(child.normalizer.normalize(rows)).reconcile(child.schema).iloc[0]


class TestSource:
    def test_asset_keys(self):
        assert {type(a).key for a in _source().assets} == {*REPORT_ASSETS, *ENTITY_ASSETS}

    def test_every_asset_uses_the_plain_source_normalizer(self):
        for key in (*REPORT_ASSETS, *ENTITY_ASSETS):
            normalizer = _asset(_source(), key).normalizer
            assert type(normalizer) is DataFrameNormalizer, key
            assert normalizer.flatten_max_level == 0, key

    def test_normalizer_survives_the_spec_roundtrip(self):
        for key in (*REPORT_ASSETS, *ENTITY_ASSETS):
            assert type(_child(key).normalizer) is DataFrameNormalizer, key


class TestPartitionColumns:
    """No Instagram response carries the report day, so reports and entities alike stamp ``date``."""

    def test_every_asset_partitions_on_the_stamped_date(self):
        for key in (*REPORT_ASSETS, *ENTITY_ASSETS):
            asset = _asset(_source(), key)
            assert asset.partitioning is not None and asset.partitioning.column == "date", key
            assert asset.schema.model_fields["date"].annotation == (dt.date | None), key

    def test_tags_follow_the_row_grain(self):
        for key in REPORT_ASSETS:
            assert _asset(_source(), key).tags == ["Report"], key
        for key in ENTITY_ASSETS:
            assert _asset(_source(), key).tags == ["Entity"], key


class TestAccountStats:
    BODY: ClassVar[dict[str, Any]] = {
        "data": [
            {
                "name": "follower_count",
                "period": "day",
                "values": [{"value": 12, "end_time": "2026-09-16T07:00:00+0000"}],
            },
            {"name": "reach", "period": "day", "values": [{"value": 340, "end_time": "2026-09-16T07:00:00+0000"}]},
        ]
    }

    async def test_time_series_pivot_into_one_row_per_end_time(self):
        urls: list[httpx2.URL] = []
        rows = await _run(_faked_source(_serving(self.BODY, urls)), "account_stats", dt.date.today())
        assert urls[0].path == "/v26.0/1784/insights"
        assert urls[0].params["metric"] == "follower_count,reach"
        assert len(rows) == 1 and rows[0]["follower_count"] == 12 and rows[0]["reach"] == 340

    async def test_follower_count_is_not_requested_beyond_30_days(self):
        urls: list[httpx2.URL] = []
        await _run(_faked_source(_serving({"data": []}, urls)), "account_stats", dt.date(2020, 1, 1))
        assert urls[0].params["metric"] == "reach"

    async def test_row_reconciles_against_the_schema(self):
        rows = await _run(_faked_source(_serving(self.BODY)), "account_stats")
        out = _reconcile("account_stats", rows)
        assert int(out["reach"]) == 340
        assert out["end_time"].isoformat().startswith("2026-09-16T07:00:00")
        assert str(out["date"])[:10] == "2026-09-15"


class TestEngagementStats:
    async def test_total_values_pivot_into_one_row(self):
        body = {
            "data": [
                {"name": "likes", "period": "day", "total_value": {"value": 25}},
                {"name": "accounts_engaged", "period": "day", "total_value": {"value": 9}},
            ]
        }
        urls: list[httpx2.URL] = []
        rows = await _run(_faked_source(_serving(body, urls)), "engagement_stats")
        assert urls[0].params["metric_type"] == "total_value"
        out = _reconcile("engagement_stats", rows)
        assert int(out["likes"]) == 25 and int(out["accounts_engaged"]) == 9
        assert str(out["date"])[:10] == "2026-09-15"

    async def test_no_insights_yield_no_row(self):
        assert await _run(_faked_source(_serving({"data": []})), "engagement_stats") == []


class TestFollowersStats:
    async def test_age_gender_results_are_keyed_by_their_dimension_keys(self):
        body = {
            "data": [
                {
                    "name": "follower_demographics",
                    "period": "lifetime",
                    "total_value": {
                        "breakdowns": [
                            {
                                "dimension_keys": ["gender", "age"],
                                "results": [
                                    {"dimension_values": ["F", "18-24"], "value": 120},
                                    {"dimension_values": ["M", "25-34"], "value": 80},
                                ],
                            }
                        ]
                    },
                }
            ]
        }
        urls: list[httpx2.URL] = []
        rows = await _run(_faked_source(_serving(body, urls)), "followers_stats_by_age_gender")
        assert urls[0].params["breakdown"] == "gender,age"
        assert urls[0].params["period"] == "lifetime"
        assert [(r["gender"], r["age"], r["follower_demographics"]) for r in rows] == [
            ("F", "18-24", 120),
            ("M", "25-34", 80),
        ]
        out = _reconcile("followers_stats_by_age_gender", rows)
        assert out["gender"] == "F" and int(out["follower_demographics"]) == 120

    async def test_country_breakdown(self):
        body = {
            "data": [
                {
                    "name": "follower_demographics",
                    "total_value": {
                        "breakdowns": [
                            {"dimension_keys": ["country"], "results": [{"dimension_values": ["DE"], "value": 7}]}
                        ]
                    },
                }
            ]
        }
        rows = await _run(_faked_source(_serving(body)), "followers_stats_by_country")
        assert rows == [{"country": "DE", "follower_demographics": 7, "date": DAY}]


class TestEntities:
    async def test_profile_row_reconciles(self):
        body = {"followers_count": 1500, "follows_count": 90, "media_count": 310, "id": "1784"}
        rows = await _run(_faked_source(_serving(body)), "profiles")
        out = _reconcile("profiles", rows)
        assert out["id"] == "1784" and int(out["followers_count"]) == 1500
        assert str(out["date"])[:10] == "2026-09-15"

    async def test_media_follows_every_page(self):
        pages = {
            None: {
                "data": [{"id": "1", "timestamp": "2026-09-14T10:00:00+0000"}],
                "paging": {"next": f"{GRAPH}/1784/media?after=p2"},
            },
            "p2": {"data": [{"id": "2", "timestamp": "2020-01-01T10:00:00+0000", "is_comment_enabled": True}]},
        }

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(200, json=pages[request.url.params.get("after")])

        rows = await _run(_faked_source(handler), "media")
        assert [r["id"] for r in rows] == ["1", "2"]
        out = _reconcile("media", rows)
        assert out["timestamp"].isoformat().startswith("2026-09-14T10:00:00")


class TestMediaStats:
    MEDIA: ClassVar[list[dict[str, Any]]] = [
        {"id": "m1", "timestamp": "2026-09-14T10:00:00+0000", "media_product_type": "FEED"},
        {
            "id": "m2",
            "timestamp": "2026-09-10T10:00:00+0000",
            "media_product_type": "REELS",
            "boost_eligibility_info": {"eligible_to_boost": True},
        },
        {"id": "m3", "timestamp": "2026-09-01T10:00:00+0000", "media_product_type": "AD"},
    ]
    STORY: ClassVar[dict[str, Any]] = {
        "id": "s1",
        "timestamp": "2026-09-15T08:00:00+0000",
        "media_product_type": "STORY",
    }
    INSIGHTS: ClassVar[dict[str, list[dict[str, Any]]]] = {
        "m1": [{"name": "likes", "period": "lifetime", "values": [{"value": 31}]}],
        "m2": [
            {"name": "views", "period": "lifetime", "values": [{"value": 900}]},
            {"name": "reels_skip_rate", "period": "lifetime", "values": [{"value": 0.42}]},
        ],
        "s1": [{"name": "navigation", "period": "lifetime", "values": [{"value": 14}]}],
    }

    def _handler(self, urls: list[httpx2.URL]) -> Any:
        def handler(request: httpx2.Request) -> httpx2.Response:
            urls.append(request.url)
            path = request.url.path
            if path.endswith("/media"):
                return httpx2.Response(200, json={"data": self.MEDIA})
            if path.endswith("/stories"):
                return httpx2.Response(200, json={"data": [self.STORY]})
            media_id = path.split("/")[-2]
            return httpx2.Response(200, json={"data": self.INSIGHTS[media_id]})

        return handler

    async def test_media_window_is_the_lookback_in_unix_seconds(self):
        urls: list[httpx2.URL] = []
        await _run(_faked_source(self._handler(urls)), "media_stats")
        media_url = next(url for url in urls if url.path.endswith("/media"))
        assert media_url.params["since"] == "1773878400"  # 2026-03-19T00:00:00Z
        assert media_url.params["until"] == "1789516800"  # 2026-09-16T00:00:00Z
        assert "insights" not in media_url.params["fields"]

    async def test_each_product_type_requests_only_its_own_metrics(self):
        urls: list[httpx2.URL] = []
        await _run(_faked_source(self._handler(urls)), "media_stats")
        metrics = {url.path.split("/")[-2]: url.params["metric"].split(",") for url in urls if "metric" in url.params}
        assert metrics == {
            "m1": constants.FEED_METRICS,
            "m2": constants.REELS_METRICS,
            "s1": constants.STORY_METRICS,
        }  # m3 is an AD, which has no metric set
        assert all("impressions" not in requested for requested in metrics.values())

    def test_schema_is_the_union_of_the_metric_sets(self):
        union = {*constants.FEED_METRICS, *constants.REELS_METRICS, *constants.STORY_METRICS}
        fields = set(schemas.MediaStats.model_fields)
        assert union <= fields
        assert "impressions" not in fields

    async def test_insights_are_lifted_into_columns(self):
        rows = await _run(_faked_source(self._handler([])), "media_stats")
        assert [row["id"] for row in rows] == ["m1", "m2", "m3", "s1"]
        assert rows[0]["likes"] == 31
        assert rows[1]["views"] == 900
        assert rows[3]["navigation"] == 14
        assert all(row["date"] == DAY for row in rows)

    async def test_row_reconciles_against_the_schema(self):
        rows = await _run(_faked_source(self._handler([])), "media_stats")
        out = _reconcile("media_stats", [rows[1]])
        assert int(out["views"]) == 900
        assert float(out["reels_skip_rate"]) == 0.42
        assert out["boost_eligibility_info"] == '{"eligible_to_boost": true}'
        assert out["timestamp"].isoformat().startswith("2026-09-10T10:00:00")
        assert str(out["date"])[:10] == "2026-09-15"
