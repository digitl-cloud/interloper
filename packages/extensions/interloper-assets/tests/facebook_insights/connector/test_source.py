"""Regression tests for the Facebook Insights source.

Graph API insights are metric-major (``{name, values: [{value, end_time}]}``);
``page_stats`` pivots them into one row per ``end_time`` and ``posts_stats``
lifts each post's lifetime insights into columns, both through the shared
``_insight_values`` helper, which keys breakdown values on
``<metric>_<breakdown>_<value>``. Neither response carries a report day, so both
stamp ``date``. Every Graph API client the source builds runs against one mock
transport, so each test sees the Page token lookup made with the user token and
every Page request made with the Page token.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.asset.context import ExecutionContext
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.facebook_insights import constants
from interloper_assets.facebook_insights.connection import FacebookInsightsConnection
from interloper_assets.facebook_insights.connector.source import FacebookInsights, _insight_values
from interloper_assets.meta import api as graph
from interloper_assets.meta.api import GraphApiError

ASSETS = ("page_stats", "posts_stats")
DAY = dt.date(2026, 9, 15)
GRAPH = "https://graph.facebook.com/v26.0"


def _connection() -> FacebookInsightsConnection:
    return FacebookInsightsConnection(client_id="cid", client_secret="secret", refresh_token="user-token")


def _source(connection: FacebookInsightsConnection | None = None) -> Any:
    if connection is None:
        return FacebookInsights(
            id="src-1",
            page_id="42",
            connection=FacebookInsightsConnection(client_id="x", client_secret="x", refresh_token="x"),
        )
    return FacebookInsights(id="src-1", page_id="42", connection=connection)


def _asset(source: Any, key: str) -> Any:
    return next(a for a in source.assets if type(a).key == key)


def _graph(monkeypatch: Any, handler: Any) -> None:
    """Send every Graph API client built during the test through *handler*.

    Args:
        monkeypatch: Replaces ``il.AsyncRESTClient`` for the test only.
        handler: Answers every request those clients send.
    """

    class GraphClient(il.AsyncRESTClient):
        def __init__(self, base_url: str, **kwargs: Any) -> None:
            super().__init__(base_url, transport=httpx2.MockTransport(handler), **kwargs)

    monkeypatch.setattr(il, "AsyncRESTClient", GraphClient)


def _faked_source(monkeypatch: Any, handler: Any) -> Any:
    """Build a source whose Page requests go to *handler*.

    The Page token lookup (``GET /42?fields=access_token``) is answered here
    with ``page-token``; every other request goes to *handler*.

    Args:
        monkeypatch: Routes the Graph API clients for the test only.
        handler: Answers every request but the Page token lookup.

    Returns:
        The source, for page ``42`` on a connection holding ``user-token``.
    """

    def page_token_or(request: httpx2.Request) -> httpx2.Response:
        if request.url.path == "/v26.0/42":
            return httpx2.Response(200, json={"access_token": "page-token", "id": "42"})
        return handler(request)

    _graph(monkeypatch, page_token_or)
    return _source(_connection())


def _sent(request: httpx2.Request) -> tuple[str, str, list[tuple[str, str]], str, bytes]:
    """Snapshot what a request sends, at send time: the paginator advances the same request object.

    Args:
        request: The request as the transport receives it.

    Returns:
        Its method, URL without the query, query parameters in order, ``Authorization`` header and body.
    """
    params = list(request.url.params.multi_items())
    url = str(request.url.copy_with(query=None))
    return request.method, url, params, request.headers["Authorization"], request.content


async def _run(source: Any, key: str) -> list[dict[str, Any]]:
    asset = _asset(source, key)
    context = ExecutionContext(asset, il.TimePartition(value=DAY).window)
    return await asset.data(context=context)


def _child(key: str) -> Any:
    source = _source()
    asset = _asset(source, key)
    spec_json = DAG(source).mini_dag(asset.id).to_spec().model_dump(mode="json")
    child_dag = DAGSpec(**spec_json).reconstruct()
    return next(a for a in child_dag.operations if type(a).key == key)


def _reconcile(key: str, rows: list[dict[str, Any]]) -> Any:
    child = _child(key)
    return TableData.from_data(child.normalizer.normalize(rows)).reconcile(child.schema)


PAGE_INSIGHTS = [
    {
        "name": "page_follows",
        "period": "day",
        "values": [{"value": 1200, "end_time": "2026-09-16T07:00:00+0000"}],
        "id": "42/insights/page_follows/day",
    },
    {
        "name": "page_actions_post_reactions_total",
        "period": "day",
        "values": [{"value": {"like": 5, "love": 2}, "end_time": "2026-09-16T07:00:00+0000"}],
        "id": "42/insights/page_actions_post_reactions_total/day",
    },
    {
        "name": "page_video_views_by_paid_non_paid",
        "period": "day",
        "values": [{"value": {"total": 9, "paid": 4, "unpaid": 5}, "end_time": "2026-09-16T07:00:00+0000"}],
        "id": "42/insights/page_video_views_by_paid_non_paid/day",
    },
]


def _breakdown_insight(metric: str, breakdown: str, values: dict[str, int], end_time: str | None) -> dict[str, Any]:
    entries = []
    for flag, value in values.items():
        entry: dict[str, Any] = {"value": value, breakdown: flag}
        if end_time:
            entry["end_time"] = end_time
        entries.append(entry)
    return {"name": metric, "period": "day", "values": entries}


class TestSource:
    def test_asset_keys(self):
        assert {type(a).key for a in _source().assets} == set(ASSETS)

    def test_every_asset_uses_the_flattening_source_normalizer(self):
        for key in ASSETS:
            normalizer = _asset(_source(), key).normalizer
            assert type(normalizer) is DataFrameNormalizer, key
            assert normalizer.flatten_max_level == 1, key

    def test_normalizer_survives_the_spec_roundtrip(self):
        for key in ASSETS:
            child = _child(key)
            assert type(child.normalizer) is DataFrameNormalizer, key
            assert child.normalizer.flatten_max_level == 1, key


class TestPartitionColumns:
    """Neither the page insights nor the post lifetime insights carry a report day, so both stamp ``date``."""

    def test_reports_partition_on_the_stamped_date(self):
        for key in ASSETS:
            asset = _asset(_source(), key)
            assert asset.tags == ["Report"], key
            assert asset.partitioning is not None and asset.partitioning.column == "date", key
            assert asset.schema.model_fields["date"].annotation == (dt.date | None), key


class TestInsightValues:
    def test_plain_value_keeps_the_metric_name(self):
        insight = {"name": "post_clicks", "values": [{"value": 3}]}
        assert list(_insight_values(insight)) == [(None, "post_clicks", 3)]

    def test_breakdown_value_is_keyed_by_breakdown_and_value(self):
        insight = _breakdown_insight("post_media_view", "is_from_ads", {"0": 7, "1": 2}, None)
        assert list(_insight_values(insight)) == [
            (None, "post_media_view_is_from_ads_0", 7),
            (None, "post_media_view_is_from_ads_1", 2),
        ]


class TestPageAccess:
    """Both reports read the Page's edges as the Page, with a token the user token derives."""

    async def test_derives_the_page_token_with_the_user_token(self, monkeypatch: Any):
        lookups: list[tuple[str, str, list[tuple[str, str]], str, bytes]] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/v26.0/42":
                lookups.append(_sent(request))
                return httpx2.Response(200, json={"access_token": "page-token", "id": "42"})
            return httpx2.Response(200, json={"data": []})

        _graph(monkeypatch, handler)
        for key in ASSETS:
            await _run(_source(_connection()), key)

        lookup = ("GET", f"{GRAPH}/42", [("fields", "access_token")], "Bearer user-token", b"")
        assert lookups == [lookup, lookup]

    @pytest.mark.parametrize("key", ASSETS)
    async def test_a_user_who_cannot_act_as_the_page_fails_the_report(self, monkeypatch: Any, key: str):
        _graph(monkeypatch, lambda request: httpx2.Response(200, json={"id": "42"}))

        with pytest.raises(PermissionError, match="cannot obtain a Page access token for page 42"):
            await _run(_source(_connection()), key)


class TestPageStats:
    async def test_insights_and_breakdowns_pivot_into_one_row_per_end_time(self, monkeypatch: Any):
        requests: list[httpx2.Request] = []
        end_time = "2026-09-16T07:00:00+0000"

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            breakdown = request.url.params.get("breakdown")
            if breakdown is None:
                return httpx2.Response(200, json={"data": PAGE_INSIGHTS, "paging": {}})
            data = [_breakdown_insight("page_media_view", breakdown, {"0": 30, "1": 10}, end_time)]
            return httpx2.Response(200, json={"data": data})

        rows = await _run(_faked_source(monkeypatch, handler), "page_stats")

        assert [r.url.params.get("breakdown") for r in requests] == [None, "is_from_ads", "is_from_followers"]
        # Graph keys a daily value by its period end, so the day's window ends the next day
        assert all((r.url.params["since"], r.url.params["until"]) == ("2026-09-15", "2026-09-16") for r in requests)
        assert all(r.url.params["period"] == "day" for r in requests)
        assert len(rows) == 1
        assert rows[0]["page_follows"] == 1200
        assert rows[0]["page_media_view_is_from_ads_1"] == 10
        assert rows[0]["page_media_view_is_from_followers_0"] == 30
        assert rows[0]["date"] == DAY

    async def test_sends_each_insights_request_once_as_the_page(self, monkeypatch: Any):
        sent: list[tuple[str, str, list[tuple[str, str]], str, bytes]] = []
        end_time = "2026-09-16T07:00:00+0000"
        next_link = f"{GRAPH}/42/insights?period=day&since=2026-09-16&until=2026-09-17"

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(_sent(request))
            breakdown = request.url.params.get("breakdown")
            if breakdown is None:
                return httpx2.Response(200, json={"data": PAGE_INSIGHTS, "paging": {"next": next_link}})
            data = [_breakdown_insight("page_media_view", breakdown, {"0": 30, "1": 10}, end_time)]
            return httpx2.Response(200, json={"data": data, "paging": {"cursors": {"before": "b", "after": "a"}}})

        rows = await _run(_faked_source(monkeypatch, handler), "page_stats")

        window = [("period", "day"), ("since", "2026-09-15"), ("until", "2026-09-16")]
        page = "Bearer page-token"
        assert sent == [
            (
                "GET",
                f"{GRAPH}/42/insights",
                [("metric", ",".join(constants.INSIGHTS_PAGE_METRICS)), *window],
                page,
                b"",
            ),
            (
                "GET",
                f"{GRAPH}/42/insights",
                [("metric", "page_media_view"), ("breakdown", "is_from_ads"), *window],
                page,
                b"",
            ),
            (
                "GET",
                f"{GRAPH}/42/insights",
                [("metric", "page_media_view"), ("breakdown", "is_from_followers"), *window],
                page,
                b"",
            ),
        ]
        assert rows == [
            {
                "end_time": end_time,
                "date": DAY,
                "page_follows": 1200,
                "page_actions_post_reactions_total": {"like": 5, "love": 2},
                "page_video_views_by_paid_non_paid": {"total": 9, "paid": 4, "unpaid": 5},
                "page_media_view_is_from_ads_0": 30,
                "page_media_view_is_from_ads_1": 10,
                "page_media_view_is_from_followers_0": 30,
                "page_media_view_is_from_followers_1": 10,
            }
        ]

    async def test_reads_the_partition_day_only_never_the_next_window(self, monkeypatch: Any):
        # Graph pages an insights edge by time window: next asks the same query for the following days
        requests: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(request)
            since = dt.date.fromisoformat(request.url.params["since"])
            until = since + dt.timedelta(days=1)
            end_time = f"{until.isoformat()}T07:00:00+0000"
            insight = {"name": "page_follows", "period": "day", "values": [{"value": since.day, "end_time": end_time}]}
            next_since = until.isoformat()
            next_until = (until + dt.timedelta(days=1)).isoformat()
            next_window = f"{GRAPH}/42/insights?period=day&since={next_since}&until={next_until}"
            paging = {"next": next_window} if since < DAY + dt.timedelta(days=3) else {}
            return httpx2.Response(200, json={"data": [insight], "paging": paging})

        rows = await _run(_faked_source(monkeypatch, handler), "page_stats")

        assert rows == [{"end_time": "2026-09-16T07:00:00+0000", "date": DAY, "page_follows": 15}]
        assert len(requests) == 1 + len(constants.MEDIA_VIEW_BREAKDOWNS)

    async def test_a_refused_insights_request_raises(self, monkeypatch: Any):
        refusal = {"error": {"message": "(#100) The value must be a valid insights metric", "code": 100}}
        source = _faked_source(monkeypatch, lambda request: httpx2.Response(400, json=refusal))

        reason = r"\(HTTP 400, code 100\): \(#100\) The value must be a valid insights metric"
        with pytest.raises(GraphApiError, match=reason):
            await _run(source, "page_stats")

    def test_row_reconciles_with_flattened_dict_metrics(self):
        row = {
            "end_time": "2026-09-16T07:00:00+0000",
            "date": DAY,
            "page_follows": 1200,
            "page_actions_post_reactions_total": {"like": 5, "love": 2},
            "page_video_views_by_paid_non_paid": {"total": 9, "paid": 4, "unpaid": 5},
            "page_media_view_is_from_ads_1": 10,
        }
        out = _reconcile("page_stats", [row]).iloc[0]
        assert int(out["page_follows"]) == 1200
        assert int(out["page_actions_post_reactions_total_like"]) == 5
        assert int(out["page_video_views_by_paid_non_paid_unpaid"]) == 5
        assert int(out["page_media_view_is_from_ads_1"]) == 10
        assert out["end_time"].isoformat().startswith("2026-09-16T07:00:00")
        assert str(out["date"])[:10] == "2026-09-15"


POST = {
    "id": "42_1001",
    "message": "Hello",
    "created_time": "2026-09-01T10:00:00+0000",
    "updated_time": "2026-09-10T10:00:00+0000",
    "status_type": "added_photos",
    "permalink_url": "https://facebook.com/42/posts/1001",
    "insights": {
        "data": [
            {
                "name": "post_clicks",
                "period": "lifetime",
                "values": [{"value": 12}],
                "id": "42_1001/insights/post_clicks/lifetime",
            },
            {
                "name": "post_clicks",
                "period": "day",
                "values": [{"value": 1, "end_time": "2026-09-15T07:00:00+0000"}],
                "id": "42_1001/insights/post_clicks/day",
            },
            {
                "name": "post_reactions_by_type_total",
                "period": "lifetime",
                "values": [{"value": {"like": 8, "love": 1}}],
                "id": "42_1001/insights/post_reactions_by_type_total/lifetime",
            },
            {
                "name": "post_clicks_by_type",
                "period": "lifetime",
                "values": [{"value": {"link clicks": 4, "other clicks": 6}}],
                "id": "42_1001/insights/post_clicks_by_type/lifetime",
            },
        ]
    },
}
STORY = {
    "id": "42_2002",
    "created_time": "2026-09-14T10:00:00+0000",
    "updated_time": "2026-09-14T10:00:00+0000",
    "insights": {
        "data": [{"name": "post_media_view", "period": "lifetime", "values": [{"value": 50}], "id": "x/lifetime"}]
    },
}
STALE_POST = {"id": "42_3003", "status_type": "mobile_status_update", "updated_time": "2025-01-01T00:00:00+0000"}
UNDATED_POST = {"id": "42_4004", "status_type": "mobile_status_update"}
UNSUPPORTED = {"error": {"message": "(#100) Invalid parameter", "type": "OAuthException", "code": 100}}


class TestPostsStats:
    def _handler(
        self,
        requests: list[httpx2.URL],
        fail_breakdown: str | None = None,
        failure: httpx2.Response | None = None,
    ) -> Any:
        def handler(request: httpx2.Request) -> httpx2.Response:
            # the paginator advances the same request object, so keep its URL at send time
            requests.append(request.url)
            path = request.url.path
            if path.endswith("/published_posts"):
                # the first page links to a second one holding posts outside the window
                if request.url.params.get("after") == "c2":
                    return httpx2.Response(200, json={"data": [STALE_POST, UNDATED_POST]})
                next_url = f"{GRAPH}/42/published_posts?after=c2"
                return httpx2.Response(200, json={"data": [POST], "paging": {"next": next_url}})
            if path.endswith("/stories"):
                return httpx2.Response(200, json={"data": [STORY]})
            breakdown = request.url.params["breakdown"]
            if breakdown == fail_breakdown:
                return failure or httpx2.Response(400, json=UNSUPPORTED)
            data = [_breakdown_insight("post_media_view", breakdown, {"0": 70, "1": 30}, None)]
            return httpx2.Response(200, json={"data": data})

        return handler

    async def test_sends_the_post_and_story_pages_then_each_feed_post_breakdown_as_the_page(self, monkeypatch: Any):
        sent: list[tuple[str, str, list[tuple[str, str]], str, bytes]] = []
        respond = self._handler([])

        def handler(request: httpx2.Request) -> httpx2.Response:
            sent.append(_sent(request))
            return respond(request)

        rows = await _run(_faked_source(monkeypatch, handler), "posts_stats")

        insights = f"insights.metric({','.join(constants.INSIGHTS_POST_METRICS)}){{name,values,id}}"
        window = [("since", "2026-03-19"), ("until", "2026-09-16")]
        page = "Bearer page-token"
        post_fields = [("fields", ",".join([*constants.POST_FIELDS, insights])), *window]
        story_fields = [("fields", ",".join([*constants.STORY_FIELDS, insights])), *window]
        assert sent[:3] == [
            ("GET", f"{GRAPH}/42/published_posts", post_fields, page, b""),
            ("GET", f"{GRAPH}/42/published_posts", [("after", "c2")], page, b""),
            ("GET", f"{GRAPH}/42/stories", story_fields, page, b""),
        ]
        # the breakdowns run concurrently, so they arrive in no fixed order
        assert sorted(sent[3:]) == [
            (
                "GET",
                f"{GRAPH}/42_1001/insights",
                [("metric", "post_media_view"), ("breakdown", breakdown), ("period", "lifetime")],
                page,
                b"",
            )
            for breakdown in ("is_from_ads", "is_from_followers")
        ]
        assert rows == [
            {
                "id": "42_1001",
                "message": "Hello",
                "created_time": "2026-09-01T10:00:00+0000",
                "updated_time": "2026-09-10T10:00:00+0000",
                "status_type": "added_photos",
                "permalink_url": "https://facebook.com/42/posts/1001",
                "post_clicks": 12,
                "post_reactions_by_type_total": {"like": 8, "love": 1},
                "post_clicks_by_type": {"link clicks": 4, "other clicks": 6},
                "post_media_view_is_from_ads_0": 70,
                "post_media_view_is_from_ads_1": 30,
                "post_media_view_is_from_followers_0": 70,
                "post_media_view_is_from_followers_1": 30,
                "date": DAY,
            },
            {
                "id": "42_2002",
                "created_time": "2026-09-14T10:00:00+0000",
                "updated_time": "2026-09-14T10:00:00+0000",
                "post_media_view": 50,
                "date": DAY,
            },
        ]

    async def test_posts_and_stories_carry_lifetime_metrics_and_breakdowns(self, monkeypatch: Any):
        requests: list[httpx2.URL] = []
        rows = await _run(_faked_source(monkeypatch, self._handler(requests)), "posts_stats")

        windows = {url.path: (url.params["since"], url.params["until"]) for url in requests if "since" in url.params}
        assert windows == {
            "/v26.0/42/published_posts": ("2026-03-19", "2026-09-16"),
            "/v26.0/42/stories": ("2026-03-19", "2026-09-16"),
        }
        assert [row["id"] for row in rows] == ["42_1001", "42_2002"]  # stale and undated posts dropped

        post, story = rows
        assert post["post_clicks"] == 12  # the lifetime copy, not the /day one
        assert post["post_reactions_by_type_total"] == {"like": 8, "love": 1}
        assert post["post_media_view_is_from_ads_1"] == 30
        assert post["post_media_view_is_from_followers_0"] == 70
        assert "insights" not in post
        assert post["date"] == DAY
        assert story["post_media_view"] == 50
        assert "post_media_view_is_from_ads_1" not in story  # stories take no breakdown

    async def test_an_unsupported_breakdown_leaves_its_columns_empty(self, monkeypatch: Any):
        handler = self._handler([], fail_breakdown="is_from_ads")
        rows = await _run(_faked_source(monkeypatch, handler), "posts_stats")
        assert "post_media_view_is_from_ads_1" not in rows[0]
        assert rows[0]["post_media_view_is_from_followers_1"] == 30

    @pytest.mark.parametrize(
        "failure",
        [
            httpx2.Response(429, json={"error": {"message": "rate limited", "code": 4}}),
            httpx2.Response(403, json={"error": {"message": "permission", "code": 10}}),
            httpx2.Response(500, text="oops"),
            httpx2.Response(500, json=UNSUPPORTED),
            httpx2.Response(400, text="<html>bad request</html>"),
            httpx2.Response(400, json={"error": {"message": "token expired", "code": 190}}),
        ],
    )
    async def test_any_other_breakdown_failure_raises(self, monkeypatch: Any, failure: httpx2.Response):
        async def no_wait(seconds: float) -> None:
            pass

        # a throttle that never lifts is raised once its budget is spent
        monkeypatch.setattr(graph, "sleep", no_wait)
        handler = self._handler([], fail_breakdown="is_from_ads", failure=failure)
        with pytest.raises(httpx2.HTTPStatusError):
            await _run(_faked_source(monkeypatch, handler), "posts_stats")

    async def test_row_reconciles_against_the_schema(self, monkeypatch: Any):
        rows = await _run(_faked_source(monkeypatch, self._handler([])), "posts_stats")
        out = _reconcile("posts_stats", rows)
        post = out.iloc[0]
        assert post["id"] == "42_1001"
        assert int(post["post_reactions_by_type_total_like"]) == 8
        assert int(post["post_clicks_by_type_link_clicks"]) == 4
        assert int(post["post_media_view_is_from_ads_0"]) == 70
        assert post["created_time"].isoformat().startswith("2026-09-01T10:00:00")
        assert str(post["date"])[:10] == "2026-09-15"
