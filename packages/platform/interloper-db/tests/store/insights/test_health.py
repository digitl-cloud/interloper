"""Tests for ``interloper_db.store.insights.health``."""

from __future__ import annotations

from uuid import uuid4

from interloper_db.models import Component
from interloper_db.store.components import ComponentStatus
from interloper_db.store.insights.health import KindInventory


class TestKindInventory:
    def test_each_kind_counts_by_state_and_unlisted_kinds_stay_out(self):
        org = uuid4()
        failing = Component(org_id=org, kind="job", key="cron_job")
        drifted = Component(org_id=org, kind="job", key="cron_job")
        statuses = [
            (Component(org_id=org, kind="job", key="cron_job"), ComponentStatus.OK),
            (failing, ComponentStatus.OK),
            (drifted, ComponentStatus.MISSING),
            (Component(org_id=org, kind="job", key="cron_job", config={"enabled": False}), ComponentStatus.OK),
            (Component(org_id=org, kind="matcher", key="campaigns"), ComponentStatus.OK),
        ]

        inventory = {row.kind: row for row in KindInventory.from_statuses(statuses, {failing.id}, set())}

        assert "matcher" not in inventory
        job = inventory["job"]
        assert (job.total, job.healthy, job.failing, job.attention, job.disabled) == (4, 1, 1, 1, 1)
