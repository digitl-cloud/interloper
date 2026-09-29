"""Tests for ``interloper_toolkit.context``."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from interloper.errors import NotFoundError
from interloper_db import engine as engine_module
from interloper_db.models import Backfill, Component, Run
from sqlmodel import Session

from interloper_toolkit import ToolkitContext


def _seed(org_id: UUID) -> tuple[Component, Run, Backfill]:
    job = Component(org_id=org_id, kind="job", key="daily")
    backfill = Backfill(org_id=org_id, component_id=job.id, start_key="2026-07-01", end_key="2026-07-02")
    run = Run(org_id=org_id, component_id=job.id, status="success")
    with Session(engine_module.get_engine()) as session:
        session.add_all([job, backfill, run])
        session.commit()
        for row in (job, backfill, run):
            session.refresh(row)
        session.expunge_all()
    return job, run, backfill


class TestOwnedLookups:
    def test_own_rows_resolve(self, ctx: ToolkitContext):
        job, run, backfill = _seed(ctx.org_id)

        assert ctx.component(str(job.id), kind="job").id == job.id
        assert ctx.run(str(run.id)).id == run.id
        assert ctx.backfill(backfill.id).id == backfill.id

    def test_another_orgs_rows_read_as_missing(self, ctx: ToolkitContext):
        job, run, backfill = _seed(uuid4())

        for lookup, row_id in ((ctx.component, job.id), (ctx.run, run.id), (ctx.backfill, backfill.id)):
            with pytest.raises(NotFoundError, match=f"{row_id} not found"):
                lookup(str(row_id))

    def test_a_missing_row_and_a_foreign_row_share_one_message(self, ctx: ToolkitContext):
        job, _, _ = _seed(uuid4())
        missing = uuid4()

        with pytest.raises(NotFoundError) as foreign_error:
            ctx.component(job.id)
        with pytest.raises(NotFoundError) as missing_error:
            ctx.component(missing)

        assert str(foreign_error.value) == str(job.id).join(str(missing_error.value).split(str(missing)))

    def test_kind_mismatch_is_not_found(self, ctx: ToolkitContext):
        job, _, _ = _seed(ctx.org_id)

        with pytest.raises(NotFoundError):
            ctx.component(job.id, kind="source")
