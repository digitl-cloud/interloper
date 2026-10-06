"""Tests for ``interloper_db.store.insights.coverage``."""

from __future__ import annotations

import datetime as dt
from uuid import uuid4

import interloper as il

from interloper_db.models import Component
from interloper_db.store.insights.coverage import AssetEvidence, CoverageRow, DayCounts, JobCoverage, PartitionSpan


class TestPartitionSpan:
    """Keys of any granularity span whole days."""

    def test_hourly_monthly_and_yearly_keys_span_their_days(self):
        assert PartitionSpan.from_key("2026-08-12T23") == (dt.date(2026, 8, 12), dt.date(2026, 8, 12))
        assert PartitionSpan.from_key("2026-02") == (dt.date(2026, 2, 1), dt.date(2026, 2, 28))
        assert PartitionSpan.from_key("2025") == (dt.date(2025, 1, 1), dt.date(2025, 12, 31))

    def test_spans_enclose_from_the_earliest_first_to_the_latest_last_day(self):
        spans = [PartitionSpan.from_key(key) for key in ("2026-08-15", "2026-08", "2026-07-31T23")]

        assert PartitionSpan.from_spans(spans) == (dt.date(2026, 7, 31), dt.date(2026, 8, 31))


class TestJobCoverage:
    def test_a_success_covers_a_key_and_only_an_unhealed_failure_fails_it(self):
        orders, refunds = uuid4(), uuid4()
        rows = [
            CoverageRow(orders, "2026-07-01", succeeded=True, failed=True, failed_run_id=uuid4()),
            CoverageRow(orders, "2026-07-02", succeeded=False, failed=True, failed_run_id=uuid4()),
            CoverageRow(orders, "2026-07-03", succeeded=False, failed=False, failed_run_id=None),
        ]

        coverage = JobCoverage.from_rows(uuid4(), ["2026-07-01", "2026-07-02"], {orders: "orders", refunds: "r"}, rows)

        assert [(a.asset_key, a.covered, a.failed) for a in coverage.assets] == [
            ("orders", {"2026-07-01"}, {"2026-07-02"}),
            ("r", frozenset(), frozenset()),
        ]


class TestDayCounts:
    def test_an_asset_owing_nothing_inside_the_window_has_no_days(self):
        asset = Component(org_id=uuid4(), kind="asset", key="orders")
        row = CoverageRow(asset.id, "2026-07-01", succeeded=True, failed=False, failed_run_id=None)
        partitioning = {asset.id: il.TimePartitionConfig(column="date")}
        spans = {asset.id: PartitionSpan.from_key("2026-07-01")}
        [evidence] = AssetEvidence.from_components([asset], partitioning, [row], spans)
        now = dt.datetime(2026, 8, 13, tzinfo=dt.timezone.utc)

        assert DayCounts.from_asset(evidence, dt.date(2026, 6, 1), dt.date(2026, 6, 30), now) is None
        assert DayCounts.from_asset(evidence, dt.date(2026, 7, 1), dt.date(2026, 7, 31), now) is not None
