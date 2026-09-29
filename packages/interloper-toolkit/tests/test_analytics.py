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
