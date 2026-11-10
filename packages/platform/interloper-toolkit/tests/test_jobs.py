"""Tests for ``interloper_toolkit.jobs``."""

from __future__ import annotations

import dataclasses
from uuid import UUID, uuid4

from interloper_db.store import ComponentQuery, Store

from interloper_toolkit import ToolkitContext, jobs
from interloper_toolkit.models import ToolError


def _source(store: Store, org_id: UUID) -> str:
    connection = store.components.create(org_id, kind="connection", key="demo_connection", config={}, encrypted=False)
    row = store.components.create(
        org_id, kind="source", key="shop_source", relations={"connection": [connection.id], "destinations": []}
    )
    return str(row.id)


class TestCreateJob:
    def test_creates_a_cron_job_over_the_sources(self, ctx: ToolkitContext, store: Store):
        source_id = _source(store, ctx.org_id)

        result = jobs.create_job(ctx, "Daily", "0 6 * * *", [source_id], lookback=2)

        assert result.status == "success"
        assert (result.cron, result.enabled, result.target_count) == ("0 6 * * *", True, 1)
        row = store.components.get(result.job.id, kind="job")
        assert row.config is not None
        assert (row.config["cron"], row.config["lookback"], row.config["offset"]) == ("0 6 * * *", 2, 1)

    def test_needs_at_least_one_target_of_its_own_org(self, ctx: ToolkitContext, store: Store):
        theirs = _source(store, uuid4())

        assert jobs.create_job(ctx, "Daily", "0 6 * * *", []).status == "error"
        assert jobs.create_job(ctx, "Daily", "0 6 * * *", [theirs]).status == "error"
        assert store.components.list(ctx.org_id, ComponentQuery(kind=["job"])).total == 0

    def test_a_viewer_is_refused(self, ctx: ToolkitContext, store: Store):
        source_id = _source(store, ctx.org_id)

        result = jobs.create_job(dataclasses.replace(ctx, role="viewer"), "Daily", "0 6 * * *", [source_id])

        assert isinstance(result, ToolError)
        assert store.components.list(ctx.org_id, ComponentQuery(kind=["job"])).total == 0
