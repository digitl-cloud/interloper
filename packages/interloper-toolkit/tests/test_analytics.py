"""Tests for ``interloper_toolkit.analytics``."""

from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import pytest
from interloper_db import engine as engine_module
from interloper_db.models import Component, Execution, Run
from sqlmodel import Session

from interloper_toolkit import ToolkitContext, analytics
from interloper_toolkit.models import ToolError


class TestPipelineOverview:
    def test_one_call_carries_the_day_the_jobs_and_what_needs_a_person(self, ctx: ToolkitContext):
        green = Component(org_id=ctx.org_id, kind="job", key="cron_job", name="Green job")
        red = Component(org_id=ctx.org_id, kind="job", key="cron_job", name="Red job")
        red_id = red.id
        finished = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        runs = [
            Run(id=uuid4(), org_id=ctx.org_id, component_id=green.id, status="success", completed_at=finished),
            Run(id=uuid4(), org_id=ctx.org_id, component_id=red_id, status="failed", completed_at=finished),
            Run(id=uuid4(), org_id=ctx.org_id, component_id=green.id, status="queued"),
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([green, red, *runs])
            session.commit()

        result = analytics.pipeline_overview(ctx)

        assert result.status == "success"
        assert (result.runs_succeeded_24h, result.runs_failed_24h, result.running, result.queued) == (1, 1, 0, 1)
        assert (result.jobs, result.jobs_failing, result.jobs_overdue) == (2, 1, 0)
        assert [(job.job_id, job.latest_status) for job in result.failing_jobs] == [(red_id, "failed")]
        assert {row.kind: row.failing for row in result.inventory}["job"] == 1

    def test_a_failed_read_comes_back_as_a_tool_error(self, ctx: ToolkitContext, monkeypatch: pytest.MonkeyPatch):
        def unavailable(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("database unavailable")

        monkeypatch.setattr(ctx.store.insights, "health", unavailable)

        result = analytics.pipeline_overview(ctx)

        assert isinstance(result, ToolError)
        assert result.error == "database unavailable"


class TestJobHealth:
    def test_failing_jobs_come_first_with_their_latest_run(self, ctx: ToolkitContext):
        green = Component(org_id=ctx.org_id, kind="job", key="cron_job", name="A green job")
        red = Component(org_id=ctx.org_id, kind="job", key="cron_job", name="B red job")
        green_id, red_id = green.id, red.id
        finished = datetime.datetime(2026, 7, 16, 12, tzinfo=datetime.timezone.utc)
        runs = [
            Run(id=uuid4(), org_id=ctx.org_id, component_id=green_id, status="success", completed_at=finished),
            Run(id=uuid4(), org_id=ctx.org_id, component_id=red_id, status="failed", completed_at=finished),
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([green, red, *runs])
            session.commit()

        result = analytics.job_health(ctx)

        assert result.status == "success"
        assert (result.failing, result.overdue, result.total) == (1, 0, 2)
        assert [(job.job_id, job.failing, job.latest_status) for job in result.jobs] == [
            (red_id, True, "failed"),
            (green_id, False, "success"),
        ]
        assert result.jobs[1].last_success_at == finished


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
        last_success = plain.completed_at
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
        assert (stats.retried, stats.healed, stats.still_failing) == (2, 1, 1)
        durations = (stats.duration_p50_seconds, stats.duration_p90_seconds, stats.duration_max_seconds)
        assert durations == (180.0, 600.0, 600.0)
        assert stats.last_success_at == last_success


class TestAssetCoverage:
    def _seed(self, ctx: ToolkitContext, create_source: Callable[[UUID, str], Component]) -> tuple[UUID, UUID, UUID]:
        orders = create_source(ctx.org_id, "shop_source").children[0]
        revenue = create_source(ctx.org_id, "finance_source").children[0]
        job = ctx.store.components.create(
            ctx.org_id, kind="job", key="cron_job", relations={"targets": [orders.id, revenue.id]}
        )
        runs = {
            key: Run(id=uuid4(), org_id=ctx.org_id, component_id=job.id, partition_key=key, status="failed")
            for key in ("2026-07-01", "2026-07-02", "2026-07-03")
        }
        outcomes = [
            ("2026-07-01", orders, "success"),
            ("2026-07-01", revenue, "failed"),
            ("2026-07-02", orders, "success"),
            ("2026-07-02", revenue, "success"),
            ("2026-07-03", orders, "failed"),
        ]
        executions = [
            Execution(
                run_id=runs[key].id,
                component_id=asset.id,
                org_id=ctx.org_id,
                component_key=asset.key,
                partition_key=key,
                status=status,
            )
            for key, asset, status in outcomes
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all(runs.values())
            session.commit()
            session.add_all(executions)
            session.commit()
        return job.id, orders.id, revenue.id

    def test_partial_runs_show_per_asset_coverage_and_the_rollup(
        self, ctx: ToolkitContext, create_source: Callable[[UUID, str], Component]
    ):
        job_id, orders, revenue = self._seed(ctx, create_source)

        result = analytics.asset_coverage(ctx, str(job_id), "2026-07-01", "2026-07-04")

        assert result.status == "success"
        assert (result.partitions, result.all_covered, result.partly_covered, result.none_covered) == (4, 1, 1, 2)
        assert [a.asset_id for a in result.assets] == [revenue, orders]
        worst = result.assets[0]
        assert (worst.covered, worst.failed, worst.never_run) == (1, 1, 2)
        assert [(r.start_key, r.end_key) for r in worst.missing] == [
            ("2026-07-01", "2026-07-01"),
            ("2026-07-03", "2026-07-04"),
        ]
        assert [(r.start_key, r.end_key) for r in result.assets[1].missing] == [("2026-07-03", "2026-07-04")]

    def test_mixed_granularities_are_refused(
        self, ctx: ToolkitContext, create_source: Callable[[UUID, str], Component]
    ):
        job_id, _, _ = self._seed(ctx, create_source)

        assert analytics.asset_coverage(ctx, str(job_id), "2026-07", "2026-07-04").status == "error"
