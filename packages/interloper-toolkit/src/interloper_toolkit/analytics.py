"""Analytics tools — run statistics, partition coverage, and data freshness."""

from __future__ import annotations

import datetime
from typing import Any
from uuid import UUID

from interloper.partitioning.time import TimePartition
from interloper_db import ComponentQuery, RunQuery

from interloper_toolkit.context import ToolkitContext
from interloper_toolkit.models import (
    AssetCoverage,
    AssetCoverageRow,
    FreshnessReport,
    JobFreshness,
    JobStats,
    PartitionCoverage,
    PartitionRange,
    RunHistorySummary,
    RunStats,
    ToolError,
)
from interloper_toolkit.stats import percentile, window

_MISSING_RANGES_LIMIT = 20


def run_history_summary(
    ctx: ToolkitContext,
    component_id: str | None = None,
    days: int = 7,
) -> RunHistorySummary | ToolError:
    """Summarize run statistics over a period.

    Args:
        component_id: Filter to a specific job UUID (optional, all jobs if omitted).
        days: Number of days to look back (default 7).

    Returns aggregate counts (total, success, failed, canceled), success
    rate, and average duration over the runs that executed within the
    period, each run stack counted by its latest attempt.
    """
    try:
        jid = UUID(component_id) if component_id else None
        cutoff = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=days)
        runs = ctx.store.runs.list(ctx.org_id, RunQuery(component_id=jid, after=cutoff, limit=None)).items
        total = len(runs)

        by_status: dict[str, int] = {}
        durations: list[float] = []
        for r in runs:
            by_status[r.status] = by_status.get(r.status, 0) + 1
            if r.started_at and r.completed_at:
                durations.append((r.completed_at - r.started_at).total_seconds())

        success = by_status.get("success", 0)
        return RunHistorySummary(
            period_days=days,
            component_id=component_id,
            total_runs=total,
            by_status=by_status,
            success_rate=round(success / total, 2) if total > 0 else None,
            avg_duration_seconds=round(sum(durations) / len(durations), 1) if durations else None,
        )
    except Exception as e:
        return ToolError(error=str(e))


def partition_coverage(
    ctx: ToolkitContext,
    component_id: str,
    start_date: str,
    end_date: str,
) -> PartitionCoverage | ToolError:
    """Check partition coverage for a job over a date range.

    Args:
        component_id: UUID of the job.
        start_date: Start date in ISO format (YYYY-MM-DD).
        end_date: End date in ISO format (YYYY-MM-DD), inclusive.

    Returns which dates have successful runs and which are missing.
    """
    try:
        job = ctx.store.components.get(UUID(component_id), kind="job", org_id=ctx.org_id)
        runs = ctx.store.runs.list(ctx.org_id, RunQuery(component_id=job.id, status="success", limit=None)).items

        start = datetime.date.fromisoformat(start_date)
        end = datetime.date.fromisoformat(end_date)

        # Coverage is a daily question, so a run covers every day inside its partition.
        covered: set[datetime.date] = set()
        for r in runs:
            if not r.partition_key:
                continue
            p_start, p_end = TimePartition.from_key(r.partition_key).bounds
            if isinstance(p_start, datetime.datetime):
                days = [p_start.date()]
            else:
                days = []
                current = p_start
                while current < p_end:
                    days.append(current)
                    current += datetime.timedelta(days=1)
            covered.update(day for day in days if start <= day <= end)

        # Build expected date range
        expected: list[datetime.date] = []
        current = start
        while current <= end:
            expected.append(current)
            current += datetime.timedelta(days=1)

        missing = sorted(set(expected) - covered)
        coverage_pct = round(len(covered) / len(expected) * 100, 1) if expected else 100.0

        return PartitionCoverage(
            component_id=component_id,
            start_date=start_date,
            end_date=end_date,
            total_days=len(expected),
            covered_days=len(covered),
            missing_days=len(missing),
            coverage_percent=coverage_pct,
            missing_dates=[d.isoformat() for d in missing],
        )
    except Exception as e:
        return ToolError(error=str(e))


def freshness_check(ctx: ToolkitContext) -> FreshnessReport | ToolError:
    """Check data freshness for all jobs.

    Returns the last successful run timestamp for each job and flags
    any that haven't succeeded in over 24 hours.
    """
    try:
        jobs = ctx.store.components.list(ctx.org_id, ComponentQuery(kind=["job"], roots_only=False, limit=None)).items
        now = datetime.datetime.now(tz=datetime.timezone.utc)

        results = []
        for job in jobs:
            if not (job.config or {}).get("enabled", True):
                continue
            component_id = job.id
            runs = ctx.store.runs.list(ctx.org_id, RunQuery(component_id=component_id, status="success", limit=1)).items
            last_success = runs[0] if runs else None

            hours_since = None
            if last_success and last_success.completed_at:
                delta = now - last_success.completed_at
                hours_since = round(delta.total_seconds() / 3600, 1)

            results.append(JobFreshness(
                job=job,
                last_success_at=last_success.completed_at if last_success else None,
                hours_since_success=hours_since,
                stale=hours_since is None or hours_since > 24,
            ))

        stale_count = sum(1 for r in results if r.stale)
        return FreshnessReport(
            total_jobs=len(results),
            stale_count=stale_count,
            jobs=results,
        )
    except Exception as e:
        return ToolError(error=str(e))


def run_stats(
    ctx: ToolkitContext,
    since: str | None = None,
    until: str | None = None,
    component_id: str | None = None,
    limit: int = 25,
    offset: int = 0,
) -> RunStats | ToolError:
    """Per-job run statistics over a window: verdicts, durations and retries.

    Args:
        since: ISO date or datetime the window opens at (default: 7 days ago).
        until: ISO date or datetime the window closes before (default: open).
        component_id: Restrict to this job UUID.
        limit: Maximum number of jobs to return (default 25).
        offset: Number of jobs to skip, for paging past the first page.

    Returns one row per job that ran in the window, most failures first: run
    stacks by final status, attempts, duration p50/p90/max in seconds, and
    how many retried stacks healed or are still failing.
    """
    try:
        start, end = window(since, until, default_days=7)
        query = RunQuery(
            component_id=UUID(component_id) if component_id else None,
            after=start,
            before=end,
            all_attempts=True,
            limit=None,
        )
        runs = ctx.store.runs.list(ctx.org_id, query).items

        stacks: dict[UUID | None, dict[UUID, list[Any]]] = {}
        names: dict[UUID | None, str | None] = {}
        for r in runs:
            stacks.setdefault(r.component_id, {}).setdefault(r.root_run_id, []).append(r)
            names.setdefault(r.component_id, r.target.name if r.target else None)

        rows = []
        for job_id, by_root in stacks.items():
            by_status: dict[str, int] = {}
            durations: list[float] = []
            attempts = retried = healed = still_failing = 0
            for chain in by_root.values():
                latest = max(chain, key=lambda r: r.attempt)
                by_status[latest.status] = by_status.get(latest.status, 0) + 1
                attempts += len(chain)
                durations += [
                    (r.completed_at - r.started_at).total_seconds() for r in chain if r.started_at and r.completed_at
                ]
                if len(chain) > 1:
                    retried += 1
                    healed += latest.status == "success"
                    still_failing += latest.status == "failed"
            rows.append(
                JobStats(
                    job_id=job_id,
                    job_name=names[job_id],
                    stacks=by_status,
                    attempts=attempts,
                    duration_p50_s=_rounded(percentile(durations, 50)),
                    duration_p90_s=_rounded(percentile(durations, 90)),
                    duration_max_s=_rounded(max(durations, default=None)),
                    stacks_retried=retried,
                    healed=healed,
                    still_failing=still_failing,
                )
            )
        rows.sort(key=lambda j: (-j.stacks.get("failed", 0), j.job_name or ""))
        page = rows[offset : offset + limit]
        return RunStats(since=start, until=end, count=len(page), total=len(rows), jobs=page)
    except Exception as e:
        return ToolError(error=str(e))


def asset_coverage(
    ctx: ToolkitContext,
    component_id: str,
    start_key: str,
    end_key: str,
    limit: int = 50,
    offset: int = 0,
) -> AssetCoverage | ToolError:
    """Per-asset partition coverage of a job over a range of partition keys.

    Unlike partition_coverage, which needs a whole run to have succeeded, an
    asset counts as covered for a partition once any run's execution of it
    succeeded, so a run where most assets succeeded shows what is actually
    missing. Only assets that executed at least once in the range appear.

    Args:
        component_id: UUID of the job.
        start_key: First partition key, in the job's granularity (2026-07-01,
            2026-07, 2026, or 2026-07-01T13).
        end_key: Last partition key, inclusive; must share the start key's
            granularity.
        limit: Maximum number of assets to return (default 50).
        offset: Number of assets to skip, for paging past the first page.

    Returns the page of assets, least covered first, each with its covered,
    failed and never-run partition counts and its missing partitions as
    ranges ready for a backfill, plus a rollup of the range's partitions by
    whether all, some or none of the assets are covered.
    """
    try:
        job = ctx.store.components.get(UUID(component_id), kind="job", org_id=ctx.org_id)
        first, last = TimePartition.from_key(start_key), TimePartition.from_key(end_key)
        if first.granularity is not last.granularity:
            return ToolError(error=f"{start_key!r} and {end_key!r} are keys of different granularities")
        granularity = first.granularity
        expected = [granularity.format(value) for value in granularity.period_range(first.value, last.value)]

        rows = ctx.store.executions.partition_coverage(ctx.org_id, job.id, start_key, end_key)
        keys: dict[UUID, str | None] = {}
        attempted: dict[UUID, set[str]] = {}
        covered: dict[UUID, set[str]] = {}
        for row in rows:
            keys.setdefault(row.component_id, row.component_key)
            attempted.setdefault(row.component_id, set()).add(row.partition_key)
            if row.succeeded:
                covered.setdefault(row.component_id, set()).add(row.partition_key)

        assets = []
        for asset_id, asset_key in keys.items():
            done = covered.get(asset_id, set())
            missing = [key for key in expected if key not in done]
            ranges = _ranges(missing, expected)
            assets.append(
                AssetCoverageRow(
                    asset_id=asset_id,
                    asset_key=asset_key,
                    covered=len(done),
                    failed=len(attempted[asset_id] - done),
                    never_run=len(missing) - len(attempted[asset_id] - done),
                    missing=ranges[:_MISSING_RANGES_LIMIT],
                    missing_ranges_total=len(ranges),
                )
            )
        assets.sort(key=lambda a: (a.covered, a.asset_key or ""))

        per_partition = [sum(key in covered.get(asset_id, ()) for asset_id in keys) for key in expected]
        page = assets[offset : offset + limit]
        return AssetCoverage(
            component_id=job.id,
            start_key=start_key,
            end_key=end_key,
            partitions=len(expected),
            all_covered=sum(n == len(keys) for n in per_partition) if keys else 0,
            partly_covered=sum(0 < n < len(keys) for n in per_partition),
            none_covered=sum(n == 0 for n in per_partition) if keys else len(expected),
            count=len(page),
            total=len(assets),
            assets=page,
        )
    except Exception as e:
        return ToolError(error=str(e))


def _ranges(missing: list[str], expected: list[str]) -> list[PartitionRange]:
    """Collapse the missing keys into runs of consecutive partitions.

    Args:
        missing: The uncovered keys, in *expected*'s order.
        expected: Every key of the range, in order.

    Returns:
        One inclusive range per run of consecutive missing keys.
    """
    position = {key: index for index, key in enumerate(expected)}
    ranges: list[PartitionRange] = []
    for key in missing:
        if ranges and position[key] == position[ranges[-1].end_key] + 1:
            ranges[-1].end_key = key
        else:
            ranges.append(PartitionRange(start_key=key, end_key=key))
    return ranges


def _rounded(value: float | None) -> float | None:
    """Round a duration to a tenth of a second, passing ``None`` through.

    Args:
        value: Seconds, or ``None`` when there was nothing to measure.

    Returns:
        The rounded value, or ``None``.
    """
    return round(value, 1) if value is not None else None
