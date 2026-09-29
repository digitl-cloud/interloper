"""Tests for ``interloper_toolkit.analytics``."""

from __future__ import annotations

import datetime
from typing import Any
from uuid import uuid4

from interloper_db import engine as engine_module
from interloper_db.models import Component, Execution, Run
from sqlmodel import Session

from interloper_toolkit import ToolkitContext, analytics


class TestAnalytics:
    def test_partition_coverage_reports_missing_dates(self, ctx: ToolkitContext):
        job = Component(org_id=ctx.org_id, kind="job", key="daily")
        job_id = job.id
        runs = [
            Run(id=uuid4(), org_id=ctx.org_id, component_id=job_id, status="success", partition_key=date.isoformat())
            for date in (datetime.date(2026, 7, 1), datetime.date(2026, 7, 3))
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, *runs])
            session.commit()

        result = analytics.partition_coverage(ctx, str(job_id), "2026-07-01", "2026-07-03")

        assert result.status == "success"
        assert result.covered_days == 2
        assert result.missing_dates == ["2026-07-02"]

    def test_partition_coverage_of_another_orgs_job_is_not_found(self, ctx: ToolkitContext):
        job = Component(org_id=uuid4(), kind="job", key="daily")
        job_id = job.id
        with Session(engine_module.get_engine()) as session:
            session.add(job)
            session.commit()

        result = analytics.partition_coverage(ctx, str(job_id), "2026-07-01", "2026-07-03")

        assert result.status == "error"

    def test_partition_coverage_reads_past_a_thousand_runs(self, ctx: ToolkitContext):
        job = Component(org_id=ctx.org_id, kind="job", key="daily")
        job_id = job.id
        first = datetime.date(2023, 1, 1)
        runs = [
            Run(
                id=uuid4(),
                org_id=ctx.org_id,
                component_id=job_id,
                status="success",
                partition_key=(first + datetime.timedelta(days=i)).isoformat(),
            )
            for i in range(1_100)
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, *runs])
            session.commit()

        result = analytics.partition_coverage(ctx, str(job_id), "2023-01-01", "2023-01-03")

        assert result.status == "success"
        assert result.missing_dates == []

    def test_run_history_summary_counts_past_five_hundred_runs(self, ctx: ToolkitContext):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        runs = [
            Run(
                id=uuid4(),
                org_id=ctx.org_id,
                status="success" if i % 2 else "failed",
                started_at=now - datetime.timedelta(hours=1),
                completed_at=now - datetime.timedelta(minutes=30),
            )
            for i in range(600)
        ]
        month_ago = now - datetime.timedelta(days=30)
        runs.append(Run(id=uuid4(), org_id=ctx.org_id, status="success", started_at=month_ago, completed_at=month_ago))
        with Session(engine_module.get_engine()) as session:
            session.add_all(runs)
            session.commit()

        result = analytics.run_history_summary(ctx, days=7)

        assert result.status == "success"
        assert result.total_runs == 600
        assert result.by_status == {"success": 300, "failed": 300}
        assert result.success_rate == 0.5


class TestRunStats:
    def test_verdicts_durations_and_retries_per_job(self, ctx: ToolkitContext):
        job = Component(org_id=ctx.org_id, kind="job", key="daily", name="Daily")
        job_id = job.id
        now = datetime.datetime.now(tz=datetime.timezone.utc)

        def run(status: str, minutes: int, **fields: Any) -> Run:
            start = now - datetime.timedelta(hours=1)
            return Run(
                id=uuid4(),
                org_id=ctx.org_id,
                component_id=job_id,
                status=status,
                started_at=start,
                completed_at=start + datetime.timedelta(minutes=minutes),
                **fields,
            )

        healed_first = run("failed", 1)
        healed_second = run("success", 2, retry_of=healed_first.id, root_run_id=healed_first.id, attempt=2)
        stuck_first = run("failed", 3)
        stuck_second = run("failed", 4, retry_of=stuck_first.id, root_run_id=stuck_first.id, attempt=2)
        plain = run("success", 10)
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, healed_first, stuck_first, plain])
            session.commit()
            session.add_all([healed_second, stuck_second])
            session.commit()

        result = analytics.run_stats(ctx)

        assert result.status == "success"
        assert result.total == 1
        stats = result.jobs[0]
        assert (stats.job_id, stats.job_name) == (job_id, "Daily")
        assert stats.stacks == {"success": 2, "failed": 1}
        assert stats.attempts == 5
        assert (stats.stacks_retried, stats.healed, stats.still_failing) == (2, 1, 1)
        assert (stats.duration_p50_s, stats.duration_p90_s, stats.duration_max_s) == (180.0, 600.0, 600.0)


class TestAssetCoverage:
    def _seed(self, ctx: ToolkitContext) -> tuple[Any, Any, Any]:
        job = Component(org_id=ctx.org_id, kind="job", key="daily")
        job_id = job.id
        ads, stats = uuid4(), uuid4()
        runs = {
            key: Run(id=uuid4(), org_id=ctx.org_id, component_id=job_id, partition_key=key, status="failed")
            for key in ("2026-07-01", "2026-07-02", "2026-07-03")
        }
        outcomes = [
            ("2026-07-01", ads, "ads", "success"),
            ("2026-07-01", stats, "ads_stats", "failed"),
            ("2026-07-02", ads, "ads", "success"),
            ("2026-07-02", stats, "ads_stats", "success"),
            ("2026-07-03", ads, "ads", "failed"),
        ]
        executions = [
            Execution(run_id=runs[key].id, component_id=asset, org_id=ctx.org_id, component_key=name, status=status)
            for key, asset, name, status in outcomes
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, *runs.values()])
            session.commit()
            session.add_all(executions)
            session.commit()
        return job_id, ads, stats

    def test_partial_runs_show_per_asset_coverage_and_the_rollup(self, ctx: ToolkitContext):
        job_id, ads, stats = self._seed(ctx)

        result = analytics.asset_coverage(ctx, str(job_id), "2026-07-01", "2026-07-04")

        assert result.status == "success"
        assert (result.partitions, result.all_covered, result.partly_covered, result.none_covered) == (4, 1, 1, 2)
        assert [a.asset_id for a in result.assets] == [stats, ads]
        worst = result.assets[0]
        assert (worst.covered, worst.failed, worst.never_run) == (1, 1, 2)
        assert [(r.start_key, r.end_key) for r in worst.missing] == [
            ("2026-07-01", "2026-07-01"),
            ("2026-07-03", "2026-07-04"),
        ]
        assert [(r.start_key, r.end_key) for r in result.assets[1].missing] == [("2026-07-03", "2026-07-04")]

    def test_mixed_granularities_are_refused(self, ctx: ToolkitContext):
        job_id, _, _ = self._seed(ctx)

        assert analytics.asset_coverage(ctx, str(job_id), "2026-07", "2026-07-04").status == "error"
