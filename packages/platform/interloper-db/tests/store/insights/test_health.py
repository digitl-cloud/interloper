"""Tests for ``interloper_db.store.insights.health``."""

from __future__ import annotations

from uuid import UUID, uuid4

from interloper_db.store.components import ComponentOutline, ComponentStatus
from interloper_db.store.insights.health import Attention, KindInventory


def _outline(
    kind: str, *, parent_id: UUID | None = None, enabled: bool = True, status: ComponentStatus = ComponentStatus.OK
) -> ComponentOutline:
    """Build an outline with the given facts.

    Args:
        kind: The component kind.
        parent_id: The source owning it.
        enabled: Whether it is enabled.
        status: Its status.

    Returns:
        The outline, under a fresh id.
    """
    return ComponentOutline(uuid4(), kind, parent_id, enabled, status, key=kind, source_key=None, name=None, start=None)


class TestAttention:
    def test_a_drifted_source_covers_its_assets_and_a_stray_asset_stands_alone(self):
        gone = _outline("source", status=ComponentStatus.MISSING)
        live = _outline("source")
        covered = _outline("asset", parent_id=gone.id, status=ComponentStatus.MISSING)
        stray = _outline("asset", parent_id=live.id, status=ComponentStatus.DISABLED)
        fine = _outline("asset", parent_id=live.id)

        drifted = Attention.drifted([gone, live, covered, stray, fine])

        assert drifted == {gone.id: ComponentStatus.MISSING, stray.id: ComponentStatus.DISABLED}


class TestKindInventory:
    def test_each_kind_counts_by_state_and_unlisted_kinds_stay_out(self):
        failing = _outline("job")
        outlines = [
            _outline("job"),
            failing,
            _outline("job", status=ComponentStatus.MISSING),
            _outline("job", enabled=False),
            _outline("matcher"),
        ]

        inventory = {row.kind: row for row in KindInventory.from_outlines(outlines, {failing.id}, set())}

        assert "matcher" not in inventory
        job = inventory["job"]
        assert (job.total, job.healthy, job.failing, job.attention, job.disabled) == (4, 1, 1, 1, 1)
