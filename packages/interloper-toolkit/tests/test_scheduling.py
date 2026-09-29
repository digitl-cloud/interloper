"""Tests for ``interloper_toolkit.scheduling``."""

from __future__ import annotations

import datetime
from uuid import uuid4

from interloper_db import engine as engine_module
from interloper_db.models import Component, Run
from sqlmodel import Session

from interloper_toolkit import ToolkitContext, scheduling


class TestScheduling:
    def test_get_job_health_computes_success_rate(self, ctx: ToolkitContext):
        job = Component(org_id=ctx.org_id, kind="job", key="daily")
        job_id = job.id
        now = datetime.datetime(2026, 7, 16, 12, 0, tzinfo=datetime.timezone.utc)
        runs = [
            Run(
                id=uuid4(),
                org_id=ctx.org_id,
                component_id=job_id,
                status=status,
                started_at=now,
                completed_at=now + datetime.timedelta(seconds=60),
            )
            for status in ("success", "success", "failed")
        ]
        with Session(engine_module.get_engine()) as session:
            session.add_all([job, *runs])
            session.commit()

        result = scheduling.get_job_health(ctx, str(job_id))

        assert result.status == "success"
        assert result.health.success_rate == 0.67
        assert result.health.avg_duration_seconds == 60.0

    def test_get_job_health_of_another_orgs_job_is_not_found(self, ctx: ToolkitContext):
        job = Component(org_id=uuid4(), kind="job", key="daily")
        job_id = job.id
        with Session(engine_module.get_engine()) as session:
            session.add(job)
            session.commit()

        result = scheduling.get_job_health(ctx, str(job_id))

        assert result.status == "error"
        assert "not found" in result.error

    def test_get_run_detail_of_another_orgs_run_is_not_found(self, ctx: ToolkitContext):
        run = Run(id=uuid4(), org_id=uuid4(), status="failed")
        run_id = run.id
        with Session(engine_module.get_engine()) as session:
            session.add(run)
            session.commit()

        result = scheduling.get_run_detail(ctx, str(run_id))

        assert result.status == "error"
        assert "not found" in result.error
