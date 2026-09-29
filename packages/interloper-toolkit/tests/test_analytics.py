"""Tests for ``interloper_toolkit.analytics``."""

from __future__ import annotations

import datetime
from uuid import uuid4

from interloper_db import engine as engine_module
from interloper_db.models import Component, Run
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
