"""Tests for ``interloper_db.store.insights.coverage``."""

from __future__ import annotations

import datetime as dt
from uuid import UUID, uuid4

import interloper as il

from interloper_db.models import Component
from interloper_db.store.insights.coverage import (
    AssetPartitions,
    DayCoverage,
    GroupCoverage,
    JobCoverage,
    PartitionResult,
    Period,
    Verdict,
)

_NOW = dt.datetime(2026, 8, 13, 9, 30, tzinfo=dt.timezone.utc)
DAY = il.TimeGranularity.DAY


def _result(asset_id: UUID, key: str, verdict: str, failed_run_id: UUID | None = None) -> PartitionResult:
    """One asset-partition's result: ``success``, ``failed`` or ``open`` (attempted, no verdict yet).

    Args:
        asset_id: The asset.
        key: The partition key.
        verdict: What its executions came to.
        failed_run_id: The failed run, for a ``failed`` result; a fresh id when left out.

    Returns:
        The result.
    """
    failed = verdict == "failed"
    return PartitionResult(
        asset_id,
        key,
        succeeded=verdict == "success",
        failed=failed,
        failed_run_id=(failed_run_id or uuid4()) if failed else None,
    )


def _asset(key: str, start: str | None = None, source: Component | None = None) -> Component:
    """An asset row, with its history start and its source.

    Args:
        key: The asset's key.
        start: Its own history start, as stored.
        source: The source it belongs to.

    Returns:
        The row.
    """
    asset = Component(org_id=uuid4(), kind="asset", key=key, config={"start": start} if start else None, parent=source)
    if source is not None:
        asset.parent_id = source.id
    return asset


def _partitions(
    partitioning: il.TimePartitionConfig | None, results: list[PartitionResult], start: str | None = None
) -> AssetPartitions:
    """One asset's partitions, its results rebound to it.

    Args:
        partitioning: The asset's partitioning; ``None`` for one the catalog has none for.
        results: Its results, against a placeholder asset id.
        start: The asset's history start, if any.

    Returns:
        The asset's partitions.
    """
    asset = _asset("campaigns", start=start)
    bound = [result._replace(asset_id=asset.id) for result in results]
    partitionings = {asset.id: partitioning} if partitioning is not None else {}
    [partitions] = AssetPartitions.from_results([asset], partitionings, bound)
    return partitions


def _snapshot(
    granularity: il.TimeGranularity, results: list[PartitionResult], start: str | None = None
) -> AssetPartitions:
    """One snapshot asset's partitions.

    Args:
        granularity: The snapshot's granularity.
        results: Its results, against a placeholder asset id.
        start: The asset's history start, if any.

    Returns:
        The asset's partitions.
    """
    return _partitions(il.SnapshotPartitionConfig(column="date", granularity=granularity), results, start)


def _calendar(
    partitioning: il.TimePartitionConfig,
    keys: list[str],
    results: list[PartitionResult],
    since: dt.date,
    until: dt.date,
    *,
    start: str | None = None,
    now: dt.datetime = _NOW,
) -> DayCoverage | None:
    """One standalone asset's days on the calendar, its attempted period the period of *keys*.

    Args:
        partitioning: The asset's partitioning.
        keys: Its attempted partition keys, all-time; none for an asset never attempted.
        results: Its results inside the window read, against a placeholder asset id.
        since: First day of the window.
        until: Last day of the window, inclusive.
        start: The asset's history start, if any.
        now: The reference instant.

    Returns:
        Its days, or ``None`` when it has none in the window.
    """
    asset = _asset("campaigns", start=start)
    attempted = {asset.id: Period.enclosing([Period.from_key(key) for key in keys])} if keys else {}
    bound = [result._replace(asset_id=asset.id) for result in results]
    groups = GroupCoverage.from_components(
        [asset], {asset.id: partitioning}, bound, attempted, since=since, until=until, now=now
    )
    return groups[0].days if groups else None


class TestPeriod:
    """A key's period, comparable across granularities, the days it reaches and whether it has ended."""

    def test_a_period_ends_where_the_next_begins(self):
        assert Period.from_key("2026-08") == (dt.datetime(2026, 8, 1), dt.datetime(2026, 9, 1))
        assert Period.from_key("2026-08-12T23") == (dt.datetime(2026, 8, 12, 23), dt.datetime(2026, 8, 13))

    def test_hourly_monthly_and_yearly_periods_reach_their_days(self):
        days = [
            (period.first_day, period.last_day) for period in map(Period.from_key, ("2026-08-12T23", "2026-02", "2025"))
        ]

        assert days == [
            (dt.date(2026, 8, 12), dt.date(2026, 8, 12)),
            (dt.date(2026, 2, 1), dt.date(2026, 2, 28)),
            (dt.date(2025, 1, 1), dt.date(2025, 12, 31)),
        ]

    def test_periods_enclose_from_the_earliest_start_to_the_latest_end(self):
        enclosing = Period.enclosing([Period.from_key(key) for key in ("2026-08-15", "2026-08", "2026-07-31T23")])

        assert (enclosing.first_day, enclosing.last_day) == (dt.date(2026, 7, 31), dt.date(2026, 8, 31))

    def test_a_period_closes_at_its_end(self):
        assert Period.from_key("2026-08-12").closed(_NOW)
        assert not Period.from_key("2026-08-13").closed(_NOW)
        assert Period.from_key("2026-08-13T08").closed(_NOW)
        assert not Period.from_key("2026-08-13T09").closed(_NOW)


class TestDatedVerdict:
    """A dated asset's period is successful, failed or missing from its history start."""

    def test_a_success_heals_a_failure_and_nothing_attempted_is_missing(self):
        failed_run = uuid4()
        partitions = _partitions(
            il.TimePartitionConfig(column="date"),
            [
                PartitionResult(uuid4(), "2026-07-01", succeeded=True, failed=True, failed_run_id=uuid4()),
                _result(uuid4(), "2026-07-02", "failed", failed_run),
                _result(uuid4(), "2026-07-03", "open"),
            ],
        )

        verdicts = [partitions.verdict(Period.from_key(f"2026-07-0{day}"), _NOW) for day in range(1, 5)]

        assert verdicts == [
            (Verdict.SUCCESSFUL, None),
            (Verdict.FAILED, failed_run),
            (Verdict.MISSING, None),
            (Verdict.MISSING, None),
        ]

    def test_a_period_before_the_history_start_does_not_count(self):
        partitions = _partitions(il.TimePartitionConfig(column="date"), [], start="2026-07-02")

        assert partitions.verdict(Period.from_key("2026-07-01"), _NOW) is None
        assert partitions.verdict(Period.from_key("2026-07-02"), _NOW) == (Verdict.MISSING, None)

    def test_an_asset_the_catalog_has_no_partitioning_for_reads_as_dated(self):
        partitions = _partitions(None, [_result(uuid4(), "2026-07-01", "success")])

        assert partitions.windowed
        assert partitions.verdict(Period.from_key("2026-07-01"), _NOW) == (Verdict.SUCCESSFUL, None)


class TestSnapshotVerdict:
    """A period takes the verdict of the snapshot periods overlapping it."""

    def test_a_current_period_fails_and_a_past_one_is_missed(self):
        failed_run = uuid4()
        partitions = _snapshot(
            DAY, [_result(uuid4(), "2026-08-12", "failed"), _result(uuid4(), "2026-08-13", "failed", failed_run)]
        )

        assert partitions.verdict(Period.from_key("2026-08-12"), _NOW) == (Verdict.MISSED, None)
        assert partitions.verdict(Period.from_key("2026-08-13"), _NOW) == (Verdict.FAILED, failed_run)

    def test_a_period_never_attempted_does_not_count(self):
        partitions = _snapshot(DAY, [_result(uuid4(), "2026-08-10", "success")])

        assert partitions.verdict(Period.from_key("2026-08-11"), _NOW) is None

    def test_an_attempt_without_a_verdict_is_missing_while_current(self):
        partitions = _snapshot(DAY, [_result(uuid4(), "2026-08-13", "open")])

        assert partitions.verdict(Period.from_key("2026-08-13"), _NOW) == (Verdict.MISSING, None)

    def test_a_coarser_period_is_successful_over_every_finer_period_inside_it(self):
        partitions = _snapshot(il.TimeGranularity.MONTH, [_result(uuid4(), "2026-07", "success")])

        assert partitions.verdict(Period.from_key("2026-07-01"), _NOW) == (Verdict.SUCCESSFUL, None)
        assert partitions.verdict(Period.from_key("2026-07-31"), _NOW) == (Verdict.SUCCESSFUL, None)
        assert partitions.verdict(Period.from_key("2026-08-01"), _NOW) is None

    def test_a_coarser_period_is_successful_once_any_finer_one_is(self):
        partitions = _snapshot(
            il.TimeGranularity.HOUR,
            [_result(uuid4(), "2026-08-12T05", "failed"), _result(uuid4(), "2026-08-12T09", "success")],
        )

        assert partitions.verdict(Period.from_key("2026-08-12"), _NOW) == (Verdict.SUCCESSFUL, None)

    def test_since_the_history_start_an_ended_period_without_an_observation_is_missed(self):
        partitions = _snapshot(DAY, [], start="2026-08-10")

        assert partitions.verdict(Period.from_key("2026-08-09"), _NOW) is None
        assert partitions.verdict(Period.from_key("2026-08-10"), _NOW) == (Verdict.MISSED, None)
        assert partitions.verdict(Period.from_key("2026-08-13"), _NOW) is None

    def test_a_day_of_a_month_still_current_is_not_missed(self):
        partitions = _snapshot(il.TimeGranularity.MONTH, [], start="2026-07-01")

        assert partitions.verdict(Period.from_key("2026-07-15"), _NOW) == (Verdict.MISSED, None)
        assert partitions.verdict(Period.from_key("2026-08-01"), _NOW) is None


class TestJobCoverage:
    def test_a_success_heals_a_failure_and_a_key_with_neither_is_missing(self):
        orders, refunds = _asset("orders"), _asset("r")
        results = [
            PartitionResult(orders.id, "2026-07-01", succeeded=True, failed=True, failed_run_id=uuid4()),
            PartitionResult(orders.id, "2026-07-02", succeeded=False, failed=True, failed_run_id=uuid4()),
            PartitionResult(orders.id, "2026-07-03", succeeded=False, failed=False, failed_run_id=None),
        ]
        keys = ["2026-07-01", "2026-07-02"]

        coverage = JobCoverage.from_results(uuid4(), keys, [orders, refunds], {}, results, now=_NOW)

        assert [(a.asset_key, a.successful, a.failed, a.missing) for a in coverage.assets] == [
            ("orders", {"2026-07-01"}, {"2026-07-02"}, frozenset()),
            ("r", frozenset(), frozenset(), set(keys)),
        ]

    def test_a_snapshot_counts_only_the_keys_it_observed_in_and_misses_the_past_ones(self):
        monthly, daily = _asset("m"), _asset("d")
        results = [
            _result(monthly.id, "2026-07", "success"),
            _result(daily.id, "2026-07-30", "failed"),
            _result(daily.id, "2026-08-13", "failed"),
        ]
        keys = ["2026-07-30", "2026-07-31", "2026-08-13"]
        partitionings = {
            monthly.id: il.SnapshotPartitionConfig(column="date", granularity=il.TimeGranularity.MONTH),
            daily.id: il.SnapshotPartitionConfig(column="date"),
        }

        coverage = JobCoverage.from_results(uuid4(), keys, [monthly, daily], partitionings, results, now=_NOW)

        by_key = {asset.asset_key: asset for asset in coverage.assets}
        assert (by_key["m"].successful, by_key["m"].missing) == ({"2026-07-30", "2026-07-31"}, frozenset())
        assert (by_key["d"].failed, by_key["d"].missing, by_key["d"].missed) == (
            {"2026-08-13"},
            frozenset(),
            {"2026-07-30"},
        )

    def test_since_its_sources_start_a_snapshot_misses_the_keys_it_never_observed(self):
        source = Component(org_id=uuid4(), kind="source", key="ads", config={"start": "2026-08-10"})
        entities = _asset("e", source=source)
        results = [_result(entities.id, "2026-08-11", "success")]
        keys = ["2026-08-09", "2026-08-10", "2026-08-11", "2026-08-12", "2026-08-13"]
        partitionings = {entities.id: il.SnapshotPartitionConfig(column="date")}

        [coverage] = JobCoverage.from_results(uuid4(), keys, [entities], partitionings, results, now=_NOW).assets

        assert (coverage.successful, coverage.missing, coverage.missed) == (
            {"2026-08-11"},
            frozenset(),
            {"2026-08-10", "2026-08-12"},
        )

    def test_a_dated_asset_counts_the_keys_from_its_own_start_over_its_sources(self):
        source = Component(org_id=uuid4(), kind="source", key="ads", config={"start": "2026-07-01"})
        orders = _asset("orders", start="2026-07-02", source=source)
        keys = ["2026-07-01", "2026-07-02", "2026-07-03"]

        [coverage] = JobCoverage.from_results(uuid4(), keys, [orders], {}, [], now=_NOW).assets

        assert coverage.missing == {"2026-07-02", "2026-07-03"}


class TestGroupCoverage:
    """The calendar: each asset's days rolled up by group."""

    def test_an_asset_with_no_days_inside_the_window_has_no_group(self):
        dated = il.TimePartitionConfig(column="date")
        results = [_result(uuid4(), "2026-07-01", "success")]
        now = dt.datetime(2026, 8, 13, tzinfo=dt.timezone.utc)

        assert _calendar(dated, ["2026-07-01"], results, dt.date(2026, 6, 1), dt.date(2026, 6, 30), now=now) is None
        assert _calendar(dated, ["2026-07-01"], results, dt.date(2026, 7, 1), dt.date(2026, 7, 31), now=now)

    def test_a_dated_asset_counts_its_successful_failed_and_missing_partitions(self):
        failed_run = uuid4()
        results = [
            _result(uuid4(), "2026-08-10", "success"),
            _result(uuid4(), "2026-08-11", "failed", failed_run),
            _result(uuid4(), "2026-08-12", "open"),
        ]
        keys = ["2026-08-10", "2026-08-12"]

        days = _calendar(
            il.TimePartitionConfig(column="date"), keys, results, dt.date(2026, 8, 1), dt.date(2026, 8, 31)
        )

        assert days == DayCoverage(
            start=dt.date(2026, 8, 10),
            successful=[1, 0, 0],
            failed=[0, 1, 0],
            missing=[0, 0, 1],
            missed=[0, 0, 0],
            failed_run_ids={1: failed_run},
        )

    def test_an_hourly_asset_counts_the_hours_elapsed_today(self):
        results = [_result(uuid4(), "2026-08-12T05", "success"), _result(uuid4(), "2026-08-13T01", "success")]
        hourly = il.TimePartitionConfig(column="date", granularity=il.TimeGranularity.HOUR)

        days = _calendar(hourly, ["2026-08-12T05", "2026-08-13T01"], results, dt.date(2026, 8, 1), dt.date(2026, 8, 31))

        assert days is not None
        assert (days.successful, days.missing) == ([1, 1], [23, 8])

    def test_a_daily_snapshot_counts_its_observed_days_and_misses_the_past_failures(self):
        failed_run = uuid4()
        results = [
            _result(uuid4(), "2026-08-10", "success"),
            _result(uuid4(), "2026-08-11", "failed"),
            _result(uuid4(), "2026-08-13", "failed", failed_run),
        ]
        snapshot = il.SnapshotPartitionConfig(column="date")

        days = _calendar(snapshot, ["2026-08-10", "2026-08-13"], results, dt.date(2026, 8, 1), dt.date(2026, 8, 31))

        assert days == DayCoverage(
            start=dt.date(2026, 8, 10),
            successful=[1, 0, 0, 0],
            failed=[0, 0, 0, 1],
            missing=[0, 0, 0, 0],
            missed=[0, 1, 0, 0],
            failed_run_ids={3: failed_run},
        )

    def test_a_snapshot_with_a_start_misses_every_ended_day_since_without_an_observation(self):
        results = [_result(uuid4(), "2026-08-10", "success"), _result(uuid4(), "2026-08-13", "open")]
        snapshot = il.SnapshotPartitionConfig(column="date")

        days = _calendar(
            snapshot,
            ["2026-08-10", "2026-08-13"],
            results,
            dt.date(2026, 8, 1),
            dt.date(2026, 8, 31),
            start="2026-08-08",
        )

        assert days is not None
        assert (days.start, days.successful, days.missing, days.missed) == (
            dt.date(2026, 8, 8),
            [0, 0, 1, 0, 0, 0],
            [0, 0, 0, 0, 0, 1],
            [1, 1, 0, 1, 1, 0],
        )

    def test_a_snapshot_never_attempted_or_outside_the_window_has_no_group(self):
        snapshot = il.SnapshotPartitionConfig(column="date")
        observed = [_result(uuid4(), "2026-08-10", "success")]

        assert _calendar(snapshot, [], [], dt.date(2026, 8, 1), dt.date(2026, 8, 31)) is None
        assert _calendar(snapshot, ["2026-08-10"], observed, dt.date(2026, 7, 1), dt.date(2026, 7, 31)) is None

    def test_a_monthly_snapshot_is_successful_each_day_of_its_month_up_to_today(self):
        snapshot = il.SnapshotPartitionConfig(column="date", granularity=il.TimeGranularity.MONTH)
        results = [_result(uuid4(), "2026-08", "success")]

        days = _calendar(snapshot, ["2026-08"], results, dt.date(2026, 8, 10), dt.date(2026, 8, 31))

        assert days is not None
        assert (days.start, days.successful) == (dt.date(2026, 8, 10), [1] * 4)

    def test_an_hourly_snapshot_counts_one_partition_a_day(self):
        results = [
            _result(uuid4(), "2026-08-12T05", "failed"),
            _result(uuid4(), "2026-08-12T09", "success"),
            _result(uuid4(), "2026-08-13T09", "open"),
        ]
        snapshot = il.SnapshotPartitionConfig(column="date", granularity=il.TimeGranularity.HOUR)

        days = _calendar(
            snapshot, ["2026-08-12T05", "2026-08-13T09"], results, dt.date(2026, 8, 1), dt.date(2026, 8, 31)
        )

        assert days is not None
        assert (days.successful, days.failed, days.missing, days.missed) == ([1, 0], [0, 0], [0, 1], [0, 0])

    def test_assets_of_one_source_sum_onto_its_group(self):
        source = Component(org_id=uuid4(), kind="source", key="ads", name="Ads")
        first, second = _asset("first", source=source), _asset("second", source=source)
        dated = il.TimePartitionConfig(column="date")
        results = [_result(first.id, "2026-08-10", "success"), _result(second.id, "2026-08-10", "failed")]
        attempted = {asset.id: Period.from_key("2026-08-10") for asset in (first, second)}

        [group] = GroupCoverage.from_components(
            [source, first, second],
            {first.id: dated, second.id: dated},
            results,
            attempted,
            since=dt.date(2026, 8, 1),
            until=dt.date(2026, 8, 31),
            now=_NOW,
        )

        assert group.component is source
        assert (group.days.successful, group.days.failed, group.days.missing) == ([1], [1], [0])
