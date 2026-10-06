"""Tests for ``interloper_db.store.insights.base``: the facet's queries over runs, events and executions.

SQLite stands in for Postgres; the executions read model's table definition
doubles as the view's schema.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, NotFoundError
from interloper_assets.demo.source import DemoMonthlySource, DemoSource, demo_asset
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, col

from interloper_db import engine as engine_module
from interloper_db.models import (
    AuthSession,
    Backfill,
    Component,
    ComponentRelation,
    Event,
    Execution,
    Invitation,
    Organisation,
    PersonalAccessToken,
    Profile,
    Quota,
    Run,
    Usage,
    UserOrganisation,
)
from interloper_db.store import PageQuery, Store
from interloper_db.store.insights import ActivityEntry
from interloper_db.store.insights import base as insights_base
from interloper_db.store.runs import partition_keys_overlapping

_ORG = uuid4()
_T0 = dt.datetime(2026, 6, 4, 12, tzinfo=dt.timezone.utc)


@pytest.fixture
def store() -> Iterator[Store]:
    """A store over a fresh in-memory database with every table the facet reads.

    Yields:
        The store, its database disposed once the test finishes.
    """
    engine = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    tenant = (Organisation, Profile, UserOrganisation, Invitation, AuthSession, PersonalAccessToken)
    for model in (*tenant, Component, ComponentRelation, Backfill, Run, Event, Execution, Quota, Usage):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog.from_assets([DemoSource, DemoMonthlySource, demo_asset]))
    finally:
        engine.dispose()
        engine_module._engine = None


def _add(*rows: Any) -> None:
    with Session(engine_module.get_engine()) as session:
        session.add_all(rows)
        session.commit()


def _component(kind: str) -> UUID:
    component_id = uuid4()
    _add(Component(id=component_id, org_id=_ORG, kind=kind, key=kind, name=kind))
    return component_id


def _run(
    target: UUID | None = None,
    *,
    status: str = "failed",
    created: dt.datetime = _T0,
    partition_key: str | None = None,
    org_id: UUID = _ORG,
    root: UUID | None = None,
    attempt: int = 1,
    backfill_id: UUID | None = None,
) -> UUID:
    run_id = uuid4()
    run = Run(
        id=run_id,
        org_id=org_id,
        component_id=target,
        status=status,
        created_at=created,
        partition_key=partition_key,
        attempt=attempt,
        backfill_id=backfill_id,
    )
    if root is not None:
        run.root_run_id = root
    _add(run)
    return run_id


def _error(run_id: UUID, event_type: str, error: str | None, *, second: int = 0, org_id: UUID = _ORG) -> Event:
    return Event(
        org_id=org_id,
        run_id=run_id,
        event_type=event_type,
        component_key="orders",
        error=error,
        timestamp=_T0 + dt.timedelta(seconds=second),
    )


def _execution(run_id: UUID, asset_id: UUID, status: str) -> None:
    """Record an asset's execution in a run, under the run's organisation and stamped with its partition key.

    Args:
        run_id: The run.
        asset_id: The executed asset.
        status: The execution's status.
    """
    with Session(engine_module.get_engine()) as session:
        run = session.get(Run, run_id)
        assert run is not None
        org_id, partition_key = run.org_id, run.partition_key
    _add(
        Execution(
            run_id=run_id,
            component_id=asset_id,
            org_id=org_id,
            component_key="orders",
            partition_key=partition_key,
            status=status,
        )
    )


class TestErrorGroups:
    """Failed attempts collapse per job, run, asset, type and text in the database, then merge by cause."""

    def test_identical_texts_collapse_per_run(self, store: Store) -> None:
        job = _component("job")
        run = _run(job)
        _add(*[_error(run, "operation_retried", "HTTPStatusError: 429", second=s) for s in range(3)])
        _add(_error(run, "operation_failed", "HTTPStatusError: 429", second=9), _error(run, "asset_data_failed", "x"))

        rows = store.insights._error_rows(_ORG, since=None, until=None, job_id=None, backfill_id=None, run_id=None)

        assert [(row.event_type, row.count, row.job_id) for row in rows] == [
            ("operation_retried", 3, job),
            ("operation_failed", 1, job),
        ]
        assert rows[0].first_seen < rows[0].last_seen

    def test_the_window_and_scope_filters_narrow_the_scan(self, store: Store) -> None:
        job, other_job, backfill = uuid4(), uuid4(), uuid4()
        mine, theirs = _run(job), _run(other_job, backfill_id=backfill)
        _add(_error(mine, "operation_failed", "early"), _error(mine, "operation_failed", "late", second=60))
        _add(_error(theirs, "operation_failed", "other job", second=60))
        _add(_error(_run(org_id=uuid4()), "operation_failed", "other org", org_id=uuid4(), second=60))
        middle = _T0 + dt.timedelta(seconds=30)

        def samples(**scope: Any) -> set[str]:
            groups = store.insights.error_groups(_ORG, group_by=("job", "asset"), **scope).groups
            return {group.sample for group in groups}

        assert samples(since=middle) == {"late", "other job"}
        assert samples(until=middle) == {"early"}
        assert samples(job_id=job) == {"late"}
        assert samples(run_id=theirs) == samples(backfill_id=backfill) == {"other job"}

    def test_the_cap_reports_that_it_cut_rows_off(self, store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(insights_base, "MAX_ERROR_ROWS", 2)
        run = _run()
        _add(*[_error(run, "operation_failed", f"error {i}", second=i) for i in range(3)])

        groups = store.insights.error_groups(_ORG)

        assert (groups.rows, groups.truncated) == (2, True)

    def test_an_unknown_grouping_key_is_rejected(self, store: Store) -> None:
        with pytest.raises(ConfigError, match="Unknown group_by"):
            store.insights.error_groups(_ORG, group_by=("job", "colour"))


def _window(first: dt.date, last: dt.date) -> list[Any]:
    return [partition_keys_overlapping(col(Execution.partition_key), first, last)]


class TestCoverageRows:
    """Per asset and time partition, from runs of any target."""

    def test_reads_the_keys_of_every_granularity_overlapping_the_window(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        inside = ["2026-07-02", "2026-07-01T13", "2026-07", "2026"]
        for key in [*inside, "2026-06-30", "2026-06", "2025", "2026-08-01T00", "eu"]:
            _execution(_run(job, partition_key=key), asset, "success")

        rows = store.insights._coverage_rows(_ORG, _window(dt.date(2026, 7, 1), dt.date(2026, 7, 31)))

        assert sorted(row.partition_key for row in rows) == sorted(inside)
        assert all(row.asset_id == asset and row.succeeded for row in rows)

    def test_runs_of_every_target_fold_into_one_row_per_asset_and_partition(self, store: Store) -> None:
        asset = uuid4()
        by_job = _run(uuid4(), partition_key="2026-07-01")
        by_a_deleted_target = _run(None, partition_key="2026-07-01")
        _execution(by_job, asset, "failed")
        _execution(_run(asset, partition_key="2026-07-01"), asset, "success")
        _execution(by_a_deleted_target, asset, "failed")

        [row] = store.insights._coverage_rows(_ORG, _window(dt.date(2026, 7, 1), dt.date(2026, 7, 1)))

        assert (row.asset_id, row.partition_key, row.succeeded, row.failed) == (asset, "2026-07-01", True, True)
        assert row.failed_run_id == max(by_job, by_a_deleted_target)

    def test_only_a_failed_execution_marks_the_row_failed(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        for key, status in (("2026-07-01", "failed"), ("2026-07-02", "running"), ("2026-07-03", "canceled")):
            _execution(_run(job, partition_key=key), asset, status)

        rows = store.insights._coverage_rows(_ORG, _window(dt.date(2026, 7, 1), dt.date(2026, 7, 3)))

        assert {row.partition_key: (row.succeeded, row.failed) for row in rows} == {
            "2026-07-01": (False, True),
            "2026-07-02": (False, False),
            "2026-07-03": (False, False),
        }

    def test_unpartitioned_runs_and_other_orgs_stay_out(self, store: Store) -> None:
        asset = uuid4()
        _execution(_run(uuid4()), asset, "success")
        _execution(_run(uuid4(), partition_key="2026-07-01", org_id=uuid4()), asset, "success")

        assert store.insights._coverage_rows(_ORG, _window(dt.date(2026, 7, 1), dt.date(2026, 7, 1))) == []


class TestAttemptedSpans:
    """Each asset's first to last attempted day, all-time."""

    def test_spans_the_first_day_of_the_first_key_to_the_last_day_of_the_last(self, store: Store) -> None:
        source = store.components.create(_ORG, kind="source", key="demo_source")
        monthly, daily = source.children[0].id, source.children[1].id
        job = uuid4()
        for key in ("2026-03", "2025-11", "2026-01"):
            _execution(_run(job, partition_key=key), monthly, "success")
        for key in ("2026-07-02", "2026-06-30"):
            _execution(_run(job, partition_key=key), daily, "failed")

        spans = store.insights._attempted_spans(_ORG)

        assert spans[monthly] == (dt.date(2025, 11, 1), dt.date(2026, 3, 31))
        assert spans[daily] == (dt.date(2026, 6, 30), dt.date(2026, 7, 2))

    def test_an_asset_never_attempted_or_with_no_time_key_has_none(self, store: Store) -> None:
        source = store.components.create(_ORG, kind="source", key="demo_source")
        asset = source.children[0].id
        _execution(_run(uuid4(), partition_key="eu"), asset, "success")
        _execution(_run(uuid4()), asset, "success")

        assert store.insights._attempted_spans(_ORG) == {}


class TestCoverageByKey:
    """A job's target assets over a key range, any target's runs counting."""

    def test_every_target_asset_reports_its_covered_and_failed_keys(self, store: Store) -> None:
        source = store.components.create(_ORG, kind="source", key="demo_source")
        job = store.components.create(_ORG, kind="job", key="cron_job", relations={"targets": [source.id]})
        assets = {child.key: child.id for child in source.children}
        _execution(_run(job.id, partition_key="2026-07-01"), assets["a"], "failed")
        _execution(_run(assets["a"], partition_key="2026-07-01"), assets["a"], "success")
        _execution(_run(job.id, partition_key="2026-07-02"), assets["a"], "failed")
        _execution(_run(job.id, partition_key="2026-07-03"), assets["a"], "success")

        coverage = store.insights.coverage_by_key(_ORG, job.id, start_key="2026-07-01", end_key="2026-07-02")

        assert coverage.keys == ["2026-07-01", "2026-07-02"]
        by_key = {asset.asset_key: asset for asset in coverage.assets}
        assert set(by_key) == set(assets)
        assert (by_key["a"].covered, by_key["a"].failed) == ({"2026-07-01"}, {"2026-07-02"})

    def test_keys_of_different_granularities_are_rejected(self, store: Store) -> None:
        job = store.components.create(_ORG, kind="job", key="cron_job")

        with pytest.raises(ConfigError, match="different granularities"):
            store.insights.coverage_by_key(_ORG, job.id, start_key="2026-07-01", end_key="2026-07")


class TestHealth:
    """Each job's state and next firing."""

    def test_a_job_whose_stored_config_cannot_window_has_none(self, store: Store) -> None:
        source = store.components.create(_ORG, kind="source", key="demo_source")
        job = store.components.create(_ORG, kind="job", key="cron_job", relations={"targets": [source.id]})
        with Session(engine_module.get_engine()) as session:
            row = session.get(Component, job.id)
            assert row is not None
            row.config = {**(row.config or {}), "offset": -1}
            row.state = {"next_run_at": _T0.isoformat()}
            session.add(row)
            session.commit()

        [health] = store.insights.health(_ORG, now=_T0).jobs

        assert (health.next_run_at, health.window) == (_T0, None)


class TestFeed:
    """The organisation's activity feed, derived from the rows that exist."""

    def test_composes_and_sorts_the_derived_feed(self, store: Store):
        admin = store.profiles.upsert(google_id="g-act", email="act@example.com", name="Act Min")
        org = store.organisations.create(name="Busy", creator_id=admin.id)
        store.members.add(org.id, admin.id, "admin")
        store.invitations.create(org.id, email="new@example.com", role="viewer", invited_by=admin.id)
        _add(
            Component(org_id=org.id, kind="source", key="bing_ads", name="Bing"),
            Run(id=uuid4(), org_id=org.id, status="success", completed_at=_T0),
            Run(id=uuid4(), org_id=org.id, status="success", completed_at=_T0 + dt.timedelta(hours=1)),
            Run(id=uuid4(), org_id=org.id, status="failed"),
        )

        entries = store.insights.feed(org.id, PageQuery()).items

        assert all(isinstance(entry, ActivityEntry) for entry in entries)
        kinds = [entry.kind for entry in entries]
        assert set(kinds) == {"org_created", "member_joined", "invitation_sent", "source_added", "runs_completed"}
        whens = [entry.when for entry in entries]
        assert whens == sorted(whens, reverse=True)
        assert all(when.tzinfo is not None for when in whens)
        joined = next(entry for entry in entries if entry.kind == "member_joined")
        assert joined.subject == "Act Min" and joined.extra == "admin"
        invited = next(entry for entry in entries if entry.kind == "invitation_sent")
        assert invited.subject == "new@example.com" and invited.extra == "Act Min"
        runs = next(entry for entry in entries if entry.kind == "runs_completed")
        assert runs.subject == "2"  # only the successful runs, aggregated per day

    def test_limit_caps_the_feed(self, store: Store):
        admin = store.profiles.upsert(google_id="g-cap", email="cap@example.com", name="Cap")
        org = store.organisations.create(name="Capped", creator_id=admin.id)
        store.members.add(org.id, admin.id, "admin")

        assert len(store.insights.feed(org.id, PageQuery(limit=1)).items) == 1

    def test_the_feed_is_windowed_over_its_whole_length(self, store: Store):
        admin = store.profiles.upsert(google_id="g-page", email="page@example.com", name="Pager")
        org = store.organisations.create(name="Paged", creator_id=admin.id)
        store.invitations.create(org.id, email="a@example.com", role="viewer", invited_by=admin.id)
        store.invitations.create(org.id, email="b@example.com", role="viewer", invited_by=admin.id)
        whole = store.insights.feed(org.id, PageQuery(limit=None))

        second = store.insights.feed(org.id, PageQuery(limit=2, offset=1))

        assert whole.total == 4
        assert second.total == 4
        assert second.items == whole.items[1:3]

    def test_a_deleted_organisation_keeps_its_feed_ending_in_the_deletion(self, store: Store):
        admin = store.profiles.upsert(google_id="g-gone", email="gone@example.com", name="Gone")
        org = store.organisations.create(name="Gone", creator_id=admin.id)
        store.organisations.delete(org.id)

        entries = store.insights.feed(org.id, PageQuery()).items

        assert entries[0].kind == "org_deleted"
        assert entries[0].when.tzinfo is not None

    def test_unknown_org_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.insights.feed(uuid4(), PageQuery())
