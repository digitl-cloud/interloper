"""Regression tests for the Brandwatch (Falcon.io) source.

The Measure API returns metrics keyed like ``channel/reactions_by_type/anger/day``
with daily ``{value, date, channelId}`` points. The source cleans each metric id
to its schema column and pivots the points into one row per day, then the
source-level ``DataFrameNormalizer`` conforms it — a chain that must survive the
host→child DAG-spec round-trip.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from interloper.dag import DAGSpec
from interloper.dag.base import DAG
from interloper.representation import Representation
from interloper_pandas import DataFrameNormalizer

from interloper_assets.brandwatch import constants, schemas
from interloper_assets.brandwatch.source import Brandwatch, _clean_metric, _reshape

_ASSET_KEY = "facebook_stats"

_NETWORK_SCHEMA = {
    "facebook": schemas.FacebookStats,
    "instagram": schemas.InstagramStats,
    "linkedin": schemas.LinkedinStats,
    "twitter": schemas.TwitterStats,
    "youtube": schemas.YoutubeStats,
}


def _source() -> Any:
    return Brandwatch(id="src-1", channel_id="chan-1")  # ty: ignore[unknown-argument]


class TestMetricSchemaCoverage:
    """Every configured metric id must map to a column on its network schema."""

    def test_cleaned_metrics_are_schema_fields(self):
        for network, metric_ids in constants.NETWORK_METRICS.items():
            fields = set(_NETWORK_SCHEMA[network].model_fields)
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
        conformer = Representation.of(normalized).conformer
        df = conformer.reconcile(normalized, schemas.FacebookStats)
        conformer.validate(df, schemas.FacebookStats)  # must not raise
        record = df.to_dict("records")[0]
        assert record["date"] == dt.date(2026, 7, 10)
        assert record["engagement_rate"] == 0.12

    def test_reshape_falls_back_to_partition_date(self):
        insights = {"channel/fans/day": [{"value": 5, "channelId": "chan-1"}]}
        rows = _reshape(insights, "chan-1", dt.date(2026, 7, 10))
        assert rows[0]["date"] == dt.date(2026, 7, 10)


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
        child_asset = next(a for a in child_dag.assets if type(a).key == _ASSET_KEY)
        assert isinstance(child_asset.normalizer, DataFrameNormalizer)
