"""Regression tests for the Brandwatch (Falcon.io) source.

The Measure API returns metrics keyed like ``channel/reactions_by_type/anger/day``
with daily ``{value, date, channelId}`` points. The source cleans each metric id
to its schema column and pivots the points into one row per day, then the
source-level ``DataFrameNormalizer`` conforms it — a chain that must survive the
host→child DAG-spec round-trip.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any

import httpx2
import interloper as il
import pytest
from interloper.asset import Asset
from interloper.dag import DAG, DAGSpec
from interloper.representation import TableData
from interloper_pandas import DataFrameNormalizer

from interloper_assets.brandwatch import constants
from interloper_assets.brandwatch.connection import BrandwatchConnection
from interloper_assets.brandwatch.connector import schemas
from interloper_assets.brandwatch.connector.source import Brandwatch, _clean_metric, _reshape

_ASSET_KEY = "facebook_stats"

_NETWORKS = {
    "facebook": (constants.FACEBOOK_METRICS, schemas.FacebookStats),
    "instagram": (constants.INSTAGRAM_METRICS, schemas.InstagramStats),
    "linkedin": (constants.LINKEDIN_METRICS, schemas.LinkedinStats),
    "twitter": (constants.TWITTER_METRICS, schemas.TwitterStats),
    "youtube": (constants.YOUTUBE_METRICS, schemas.YoutubeStats),
}


def _source() -> Any:
    return Brandwatch(id="src-1", channel_id="chan-1", connection=BrandwatchConnection(api_key="x"))


class TestMetricSchemaCoverage:
    """Every configured metric id must map to a column on its network schema."""

    def test_cleaned_metrics_are_schema_fields(self):
        for network, (metric_ids, schema) in _NETWORKS.items():
            fields = set(schema.model_fields)
            cleaned = [_clean_metric(m) for m in metric_ids]
            assert len(cleaned) == len(set(cleaned)), f"{network}: duplicate cleaned metric names"
            missing = [c for c in cleaned if c not in fields]
            assert not missing, f"{network}: metrics with no schema field: {missing}"

    def test_clean_metric_handles_nested_paths(self):
        assert _clean_metric("channel/fans/day") == "fans"
        assert _clean_metric("channel/reactions_by_type/anger/day") == "reactions_by_type_anger"
        assert _clean_metric("channel/views_by_logged_in_out/logged_in/day") == "views_by_logged_in_out_logged_in"


class TestReshape:
    """Daily points pivot into one row per day keyed by schema columns."""

    def test_reshape_pivots_and_conforms(self):
        insights = {
            "channel/fans/day": [{"value": 1000, "date": "2026-07-10", "channelId": "chan-1"}],
            "channel/views_by_logged_in_out/logged_in/day": [
                {"value": 42, "date": "2026-07-10", "channelId": "chan-1"}
            ],
            "channel/engagement_rate/day": [{"value": 0.12, "date": "2026-07-10", "channelId": "chan-1"}],
        }
        rows = _reshape(insights, "chan-1", dt.date(2026, 7, 10))
        assert len(rows) == 1
        assert rows[0]["channel_id"] == "chan-1"
        assert rows[0]["fans"] == 1000
        assert rows[0]["views_by_logged_in_out_logged_in"] == 42

        normalized = DataFrameNormalizer().normalize(rows)
        table = TableData.from_data(normalized)
        df = table.reconcile(schemas.FacebookStats)
        TableData.from_data(df).reconcile(schemas.FacebookStats, strict=True)  # must not raise
        record = df.to_dict("records")[0]
        assert record["date"] == dt.date(2026, 7, 10)
        assert record["engagement_rate"] == 0.12

    def test_reshape_falls_back_to_partition_date(self):
        insights = {"channel/fans/day": [{"value": 5, "channelId": "chan-1"}]}
        rows = _reshape(insights, "chan-1", dt.date(2026, 7, 10))
        assert rows[0]["date"] == dt.date(2026, 7, 10)

    def test_reshape_skips_points_that_are_not_objects(self):
        insights = {"channel/fans/day": [None, "5", {"value": 5, "date": "2026-07-10", "channelId": "chan-1"}]}
        rows = _reshape(insights, "chan-1", dt.date(2026, 7, 10))
        assert rows == [{"channel_id": "chan-1", "date": "2026-07-10", "fans": 5}]


class TestSourceNormalizer:
    """The decorator-configured normalizer must reach every asset instance."""

    def test_source_instance_has_dataframe_normalizer(self):
        assert isinstance(_source().normalizer, DataFrameNormalizer)

    def test_all_assets_inherit_the_normalizer(self):
        src = _source()
        assert len(src.assets) == 5
        for asset in src.assets:
            assert isinstance(asset.normalizer, DataFrameNormalizer), type(asset).key


class TestSpecRoundtrip:
    """The host→child spec round-trip must preserve the normalizer subclass."""

    def test_child_asset_keeps_dataframe_normalizer(self):
        src = _source()
        asset = next(a for a in src.assets if type(a).key == _ASSET_KEY)
        spec_json = DAG(src).mini_dag(asset.id).to_spec().model_dump(mode="json")
        child_dag = DAGSpec(**spec_json).reconstruct()
        child_asset = next(a for a in child_dag.operations if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset, Asset)
        assert isinstance(child_asset.normalizer, DataFrameNormalizer)


def _sent(request: httpx2.Request) -> httpx2.Request:
    """Copy a request as it is sent.

    The paginating client sends every page with one request object, rewriting
    its URL, so a test keeps what each send carried rather than the object.

    Args:
        request: The request the mock transport received.

    Returns:
        An independent copy of its method, URL, headers and body.
    """
    return httpx2.Request(request.method, request.url, headers=request.headers, content=request.content)


class TestRequests:
    """What each asset sends to the Measure API, and the rows it makes of the answers.

    The mock answers each insights request ``READY`` at once, then its metrics
    over two pages: the first half of the batch, then the rest. Each point's
    value is its own metric id, so a row shows which point landed where.
    """

    def _run(self, key: str) -> tuple[list[dict[str, Any]], list[httpx2.Request]]:
        requests: list[httpx2.Request] = []
        batches: dict[str, list[str]] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            requests.append(_sent(request))
            if request.method == "POST":
                request_id = f"request-{len(batches) + 1}"
                batches[request_id] = json.loads(request.content)["metricIds"]
                return httpx2.Response(200, json={"insightsRequestId": request_id})
            metric_ids = batches[request.url.path.rsplit("/", 1)[-1]]
            half = len(metric_ids) // 2
            page = request.url.params.get("page")
            if page is None:
                return httpx2.Response(200, json={"status": "READY"})
            points = {
                metric_id: [{"value": metric_id, "date": "2026-10-01", "channelId": "chan-1"}]
                for metric_id in (metric_ids[:half] if page == "1" else metric_ids[half:])
            }
            paging = {"nextPage": 2} if page == "1" else {}
            return httpx2.Response(200, json={"data": {"insights": points, "paging": paging}})

        connection = BrandwatchConnection(api_key="key")
        connection.__dict__["client"] = il.AsyncRESTClient(
            constants.BASE_URL, params={"param_key": "key"}, transport=httpx2.MockTransport(handler)
        )
        source = Brandwatch(id="src-1", channel_id="chan-1", connection=connection)
        asset = next(a for a in source.assets if type(a).key == key)
        context = il.ExecutionContext(asset, il.TimePartition(value=dt.date(2026, 10, 1)).window)
        return asyncio.run(asset.data(context=context)), requests

    @pytest.mark.parametrize(
        ("key", "metric_ids"),
        [
            ("facebook_stats", constants.FACEBOOK_METRICS),
            ("instagram_stats", constants.INSTAGRAM_METRICS),
            ("linkedin_stats", constants.LINKEDIN_METRICS),
            ("twitter_stats", constants.TWITTER_METRICS),
            ("youtube_stats", constants.YOUTUBE_METRICS),
        ],
    )
    def test_each_batch_of_metrics_is_requested_polled_and_read_page_by_page(self, key: str, metric_ids: list[str]):
        rows, requests = self._run(key)

        size = constants.MAX_METRICS_PER_REQUEST
        batches = [metric_ids[i : i + size] for i in range(0, len(metric_ids), size)]
        assert [(request.method, request.url.path, request.url.query) for request in requests] == [
            step
            for index in range(1, len(batches) + 1)
            for step in (
                ("POST", "/measure/v2/insights/channel", b"param_key=key"),
                ("GET", f"/measure/v2/insights/request-{index}", b"param_key=key"),
                ("GET", f"/measure/v2/insights/request-{index}", b"param_key=key&page=1"),
                ("GET", f"/measure/v2/insights/request-{index}", b"param_key=key&page=2"),
            )
        ]
        posts = [request for request in requests if request.method == "POST"]
        assert [json.loads(request.content) for request in posts] == [
            {"since": "2026-10-01", "until": "2026-10-01", "channelIds": ["chan-1"], "metricIds": batch}
            for batch in batches
        ]
        assert {tuple(json.loads(request.content)) for request in posts} == {
            ("since", "until", "channelIds", "metricIds")
        }
        assert {request.headers["content-type"] for request in posts} == {"application/json"}
        assert all(request.content == b"" for request in requests if request.method == "GET")

        expected = {"channel_id": "chan-1", "date": "2026-10-01"} | {
            _clean_metric(metric_id): metric_id for metric_id in metric_ids
        }
        assert rows == [expected]
        assert list(rows[0]) == list(expected)
