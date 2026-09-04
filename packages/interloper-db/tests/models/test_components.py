"""Tests for the behaviour on ``interloper_db.models.components``.

The row carries two pieces of logic beyond its declarations: resolving an
owned asset's parent key, and merging machine-owned state without touching
the spec.
"""

from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pydantic
import pytest
from sqlalchemy import Engine
from sqlmodel import Session

from interloper_db.models import Component

_ORG = uuid4()


class TestParentKey:
    """A source-owned asset needs its parent's catalog key to resolve."""

    def test_a_parentless_row_has_none(self, component_db: Engine):
        row = Component(org_id=_ORG, kind="source", key="demo_source")

        with Session(component_db) as session:
            assert row.parent_key(session) is None

    def test_an_owned_asset_reports_its_sources_key(self, component_db: Engine):
        with Session(component_db) as session:
            source = Component(org_id=_ORG, kind="source", key="demo_source")
            session.add(source)
            session.commit()
            asset = Component(org_id=_ORG, kind="asset", key="a", parent_id=source.id)
            session.add(asset)
            session.commit()

            assert asset.parent_key(session) == "demo_source"

    def test_a_vanished_parent_reports_none(self, component_db: Engine):
        # Defensive: the foreign key makes this unreachable in practice.
        row = Component(org_id=_ORG, kind="asset", key="a", parent_id=uuid4())

        with Session(component_db) as session:
            assert row.parent_key(session) is None


class TestStampState:
    """State is machine-owned and merges over what is already there."""

    def test_it_merges_rather_than_replaces(self):
        row = Component(org_id=_ORG, kind="job", key="cron_job", state={"next_run_at": "x"})

        row.stamp_state(last_run_at="y")

        assert row.state == {"next_run_at": "x", "last_run_at": "y"}

    def test_a_later_stamp_wins(self):
        row = Component(org_id=_ORG, kind="job", key="cron_job", state={"next_run_at": "x"})

        row.stamp_state(next_run_at="y")

        assert row.state == {"next_run_at": "y"}

    def test_datetimes_are_stored_as_aware_iso_strings(self):
        # Lexicographic comparison in SQL then stays chronological.
        row = Component(org_id=_ORG, kind="job", key="cron_job")
        stamp = dt.datetime(2026, 6, 1, 12, 30, tzinfo=dt.timezone.utc)

        row.stamp_state(next_run_at=stamp)

        assert row.state is not None
        stored = row.state["next_run_at"]
        assert isinstance(stored, str)
        assert dt.datetime.fromisoformat(stored) == stamp

    def test_none_clears_a_field(self):
        row = Component(org_id=_ORG, kind="job", key="cron_job", state={"next_run_at": "x"})

        row.stamp_state(next_run_at=None)

        assert row.state is not None
        assert row.state["next_run_at"] is None

    def test_the_spec_is_untouched(self):
        row = Component(org_id=_ORG, kind="job", key="cron_job", config={"cron": "0 2 * * *"})

        row.stamp_state(next_run_at=None)

        assert row.config == {"cron": "0 2 * * *"}

    def test_a_shape_the_kind_does_not_declare_is_rejected(self):
        row = Component(org_id=_ORG, kind="job", key="cron_job")

        with pytest.raises(pydantic.ValidationError):
            row.stamp_state(next_run_at=object())
