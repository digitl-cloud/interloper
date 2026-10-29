"""Tests for ``interloper_db.store.insights.coverage``."""

from __future__ import annotations

import datetime as dt
from uuid import UUID, uuid4

import interloper as il

from interloper_db.models import Component
from interloper_db.store.insights.coverage import (
    AssetEvidence,
    CoverageRow,
    DayCounts,
    JobCoverage,
    Observations,
    PartitionSpan,
    Period,
    Verdict,
)

_NOW = dt.datetime(2026, 8, 13, 9, 30, tzinfo=dt.timezone.utc)


def _row(asset_id: UUID, key: str, verdict: str, failed_run_id: UUID | None = None) -> CoverageRow:
    """One asset-partition's evidence: ``success``, ``failed`` or ``open`` (attempted, no verdict yet).

    Args:
        asset_id: The asset.
        key: The partition key.
        verdict: What its executions came to.
        failed_run_id: The failed run, for a ``failed`` row; a fresh id when left out.

    Returns:
        The row.
    """
    failed = verdict == "failed"
    return CoverageRow(
        asset_id,
        key,
        succeeded=verdict == "success",
        failed=failed,
        failed_run_id=(failed_run_id or uuid4()) if failed else None,
    )


def _snapshot(granularity: il.TimeGranularity, keys: list[str], rows: list[CoverageRow]) -> AssetEvidence:
    """One snapshot asset's evidence, its attempted span the span of *keys*.

    Args:
        granularity: The snapshot's granularity.
        keys: Its attempted partition keys, all-time.
        rows: Its coverage rows inside the window read, against a placeholder asset id.

    Returns:
        The evidence, rows rebound to the asset.
    """
    asset = Component(org_id=uuid4(), kind="asset", key="campaigns")
    partitioning = {asset.id: il.SnapshotPartitionConfig(column="date", granularity=granularity)}
    spans = {asset.id: PartitionSpan.from_spans([PartitionSpan.from_key(key) for key in keys])}
    bound = [row._replace(asset_id=asset.id) for row in rows]
    [evidence] = AssetEvidence.from_components([asset], partitioning, bound, spans)
    return evidence


class TestPartitionSpan:
    """Keys of any granularity span whole days."""

    def test_hourly_monthly_and_yearly_keys_span_their_days(self):
        assert PartitionSpan.from_key("2026-08-12T23") == (dt.date(2026, 8, 12), dt.date(2026, 8, 12))
        assert PartitionSpan.from_key("2026-02") == (dt.date(2026, 2, 1), dt.date(2026, 2, 28))
        assert PartitionSpan.from_key("2025") == (dt.date(2025, 1, 1), dt.date(2025, 12, 31))

    def test_spans_enclose_from_the_earliest_first_to_the_latest_last_day(self):
        spans = [PartitionSpan.from_key(key) for key in ("2026-08-15", "2026-08", "2026-07-31T23")]

        assert PartitionSpan.from_spans(spans) == (dt.date(2026, 7, 31), dt.date(2026, 8, 31))


class TestPeriod:
    """A key's period, comparable across granularities, and whether it has ended."""

    def test_a_period_ends_where_the_next_begins(self):
        assert Period.from_key("2026-08") == (dt.datetime(2026, 8, 1), dt.datetime(2026, 9, 1))
        assert Period.from_key("2026-08-12T23") == (dt.datetime(2026, 8, 12, 23), dt.datetime(2026, 8, 13))

    def test_a_period_closes_at_its_end(self):
        assert Period.from_key("2026-08-12").closed(_NOW)
        assert not Period.from_key("2026-08-13").closed(_NOW)
        assert Period.from_key("2026-08-13T08").closed(_NOW)
        assert not Period.from_key("2026-08-13T09").closed(_NOW)


class TestObservations:
    """A cell takes the verdict of the snapshot periods overlapping it."""

    def test_a_current_period_fails_and_a_past_one_is_missed(self):
        failed_run = uuid4()
        observations = Observations(
            [_row(uuid4(), "2026-08-12", "failed"), _row(uuid4(), "2026-08-13", "failed", failed_run)]
        )

        assert observations.verdict(Period.from_key("2026-08-12"), _NOW) == (Verdict.MISSED, None)
        assert observations.verdict(Period.from_key("2026-08-13"), _NOW) == (Verdict.FAILED, failed_run)

    def test_a_period_never_attempted_is_owed_nothing(self):
        observations = Observations([_row(uuid4(), "2026-08-10", "success")])

        assert observations.verdict(Period.from_key("2026-08-11"), _NOW) is None

    def test_an_attempt_without_a_verdict_is_pending_while_current(self):
        observations = Observations([_row(uuid4(), "2026-08-13", "open")])

        assert observations.verdict(Period.from_key("2026-08-13"), _NOW) == (Verdict.PENDING, None)

    def test_a_coarser_period_covers_every_finer_cell_inside_it(self):
        observations = Observations([_row(uuid4(), "2026-07", "success")])

        assert observations.verdict(Period.from_key("2026-07-01"), _NOW) == (Verdict.COVERED, None)
        assert observations.verdict(Period.from_key("2026-07-31"), _NOW) == (Verdict.COVERED, None)
        assert observations.verdict(Period.from_key("2026-08-01"), _NOW) is None

    def test_a_coarser_cell_is_covered_once_any_finer_period_is(self):
        observations = Observations(
            [_row(uuid4(), "2026-08-12T05", "failed"), _row(uuid4(), "2026-08-12T09", "success")]
        )

        assert observations.verdict(Period.from_key("2026-08-12"), _NOW) == (Verdict.COVERED, None)


class TestJobCoverage:
    def test_a_success_covers_a_key_and_only_an_unhealed_failure_fails_it(self):
        orders, refunds = uuid4(), uuid4()
        rows = [
            CoverageRow(orders, "2026-07-01", succeeded=True, failed=True, failed_run_id=uuid4()),
            CoverageRow(orders, "2026-07-02", succeeded=False, failed=True, failed_run_id=uuid4()),
            CoverageRow(orders, "2026-07-03", succeeded=False, failed=False, failed_run_id=None),
        ]
        keys = ["2026-07-01", "2026-07-02"]

        coverage = JobCoverage.from_rows(
            uuid4(), keys, {orders: "orders", refunds: "r"}, rows, snapshots=frozenset(), now=_NOW
        )

        assert [(a.asset_key, a.expected, a.covered, a.failed) for a in coverage.assets] == [
            ("orders", set(keys), {"2026-07-01"}, {"2026-07-02"}),
            ("r", set(keys), frozenset(), frozenset()),
        ]

    def test_a_snapshot_owes_only_the_keys_it_observed_in_and_misses_the_past_ones(self):
        monthly, daily = uuid4(), uuid4()
        rows = [
            _row(monthly, "2026-07", "success"),
            _row(daily, "2026-07-30", "failed"),
            _row(daily, "2026-08-13", "failed"),
        ]
        keys = ["2026-07-30", "2026-07-31", "2026-08-13"]

        coverage = JobCoverage.from_rows(
            uuid4(), keys, {monthly: "m", daily: "d"}, rows, snapshots=frozenset({monthly, daily}), now=_NOW
        )

        by_key = {asset.asset_key: asset for asset in coverage.assets}
        assert (by_key["m"].expected, by_key["m"].covered) == ({"2026-07-30", "2026-07-31"}, by_key["m"].expected)
        assert (by_key["d"].expected, by_key["d"].failed, by_key["d"].missed) == (
            {"2026-08-13"},
            {"2026-08-13"},
            {"2026-07-30"},
        )


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

    def test_a_daily_snapshot_owes_its_observed_days_and_misses_the_past_failures(self):
        failed_run = uuid4()
        rows = [
            _row(uuid4(), "2026-08-10", "success"),
            _row(uuid4(), "2026-08-11", "failed"),
            _row(uuid4(), "2026-08-13", "failed", failed_run),
        ]
        keys = ["2026-08-10", "2026-08-13"]
        evidence = _snapshot(il.TimeGranularity.DAY, keys, rows)

        days = DayCounts.from_asset(evidence, dt.date(2026, 8, 1), dt.date(2026, 8, 31), _NOW)

        assert days == DayCounts(
            start=dt.date(2026, 8, 10),
            expected=[1, 0, 0, 1],
            covered=[1, 0, 0, 0],
            failed=[0, 0, 0, 1],
            failed_run_ids={3: failed_run},
            missed=[0, 1, 0, 0],
        )

    def test_a_snapshot_never_attempted_or_outside_the_window_has_no_days(self):
        never = _snapshot(il.TimeGranularity.DAY, ["2026-08-10"], [])
        never = AssetEvidence(never.asset, never.group, never.partitioning, never.scheduled, [], None)
        observed = _snapshot(il.TimeGranularity.DAY, ["2026-08-10"], [_row(uuid4(), "2026-08-10", "success")])

        assert DayCounts.from_asset(never, dt.date(2026, 8, 1), dt.date(2026, 8, 31), _NOW) is None
        assert DayCounts.from_asset(observed, dt.date(2026, 7, 1), dt.date(2026, 7, 31), _NOW) is None

    def test_a_monthly_snapshot_covers_each_day_of_its_month_up_to_today(self):
        evidence = _snapshot(il.TimeGranularity.MONTH, ["2026-08"], [_row(uuid4(), "2026-08", "success")])

        days = DayCounts.from_asset(evidence, dt.date(2026, 8, 10), dt.date(2026, 8, 31), _NOW)

        assert days is not None
        assert (days.start, days.expected, days.covered) == (dt.date(2026, 8, 10), [1] * 4, [1] * 4)

    def test_an_hourly_snapshot_owes_one_partition_a_day(self):
        rows = [
            _row(uuid4(), "2026-08-12T05", "failed"),
            _row(uuid4(), "2026-08-12T09", "success"),
            _row(uuid4(), "2026-08-13T09", "open"),
        ]
        evidence = _snapshot(il.TimeGranularity.HOUR, ["2026-08-12T05", "2026-08-13T09"], rows)

        days = DayCounts.from_asset(evidence, dt.date(2026, 8, 1), dt.date(2026, 8, 31), _NOW)

        assert days is not None
        assert (days.expected, days.covered, days.failed, days.missed) == ([1, 1], [1, 0], [0, 0], [0, 0])
