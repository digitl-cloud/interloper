"""Tests for ``interloper_api.routes.overview``.

A real store over in-memory SQLite, with a catalog of test component classes
so both live and drifted rows exist, and the semantics (failing, overdue,
drift, renewal errors, state precedence) are exercised against real rows.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from collections.abc import Iterator
from functools import cached_property
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import interloper as il
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import engine as engine_module
from interloper_db.models import (
    Backfill,
    Component,
    ComponentRelation,
    Event,
    Execution,
    Organisation,
    Profile,
    Quota,
    Run,
    Usage,
    UserOrganisation,
)
from interloper_db.store import Store
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_api.dependencies import get_current_user, get_org_id, get_store, require_viewer
from interloper_api.routes import overview as overview_module

NOW = dt.datetime(2026, 8, 13, 9, 14, tzinfo=dt.timezone.utc)


@il.connection(name="Shop API")
class ShopConnection(il.Connection):
    """A test connection the catalog enables."""

    api_key: str = il.SecretField(default="k")

    @cached_property
    def client(self) -> None:
        """No client: the overview never calls out.

        Returns:
            ``None``.
        """
        return None


class Order(il.Schema):
    """One order row."""

    id: int


@il.source
class Shop(il.Source):
    """A test source with one daily-partitioned asset."""

    @il.asset(schema=Order, partitioning=il.TimePartitionConfig(column="date"))
    def orders(self) -> list[dict]:
        return []


@pytest.fixture
def engine() -> Iterator[Engine]:
    """A fresh in-memory database with the tables the overview reads.

    Yields:
        The engine bound to that database, disposed once the test finishes.
    """
    engine = engine_module.init_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _configure(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    models = (Profile, Organisation, UserOrganisation, Component, ComponentRelation, Backfill, Run, Event, Quota, Usage)
    for model in (*models, Execution):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield engine
    finally:
        engine.dispose()
        engine_module._engine = None


@pytest.fixture
def store(engine: Engine) -> Store:
    """A store whose catalog enables the test source and connection.

    Args:
        engine: The database fixture the store binds to.

    Returns:
        The store, with an identity cipher.
    """
    return Store(catalog=il.Catalog.from_assets([Shop, ShopConnection]), encrypt=lambda b: b, decrypt=lambda b: b)


@pytest.fixture
def member(store: Store) -> SimpleNamespace:
    """A member of a fresh organisation.

    Args:
        store: The store the profile and organisation are created in.

    Returns:
        The profile and organisation ids the routes resolve.
    """
    profile = store.auth.upsert_profile(google_id="g-1", email="ada@example.com", name="Ada")
    org = store.organisations.create(name="Acme", creator_id=profile.id)
    return SimpleNamespace(id=profile.id, org_id=org.id, email="ada@example.com", is_super_admin=False)


@pytest.fixture
def client(store: Store, member: SimpleNamespace) -> TestClient:
    """A client over the overview router, authenticated as *member*.

    Args:
        store: The store the routes read.
        member: The authenticated viewer.

    Returns:
        The test client.
    """
    app = FastAPI()
    app.include_router(overview_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: member
    app.dependency_overrides[require_viewer] = lambda: member
    app.dependency_overrides[get_org_id] = lambda: member.org_id
    return TestClient(app)


def _overview(client: TestClient) -> dict[str, Any]:
    """Read the overview at the pinned reference instant.

    Args:
        client: The test client.

    Returns:
        The response body.
    """
    return client.get("/overview", params={"now": NOW.isoformat()}).json()


def _run(
    store: Store,
    org_id: UUID,
    target: Component | None,
    *,
    status: str,
    started: dt.datetime | None,
    completed: dt.datetime | None,
    partition_key: str | None = None,
    backfill_id: UUID | None = None,
    attempt: int = 1,
    root: UUID | None = None,
) -> Run:
    """Insert one run attempt directly, bypassing the queueing path.

    Args:
        store: The store whose engine the row is written through.
        org_id: The organisation.
        target: The run's target, or ``None`` for a deleted one.
        status: The attempt's status.
        started: When it started.
        completed: When it completed.
        partition_key: Its partition.
        backfill_id: Its backfill.
        attempt: Its attempt number.
        root: Its stack's root; defaults to itself.

    Returns:
        The persisted run.
    """
    run = Run(
        org_id=org_id,
        component_id=target.id if target else None,
        status=status,
        started_at=started,
        completed_at=completed,
        created_at=started or NOW,
        partition_key=partition_key,
        backfill_id=backfill_id,
        attempt=attempt,
    )
    with Session(store.engine) as session:
        session.add(run)
        session.commit()
        session.refresh(run)
        run.root_run_id = root or run.id
        session.add(run)
        session.commit()
        session.refresh(run)
    return run


def _execution(store: Store, run: Run, asset: Component, status: str) -> None:
    """Record one asset execution of a run.

    Args:
        store: The store whose engine the row is written through.
        run: The run the execution belongs to.
        asset: The executed asset.
        status: The execution's status.
    """
    with Session(store.engine) as session:
        session.add(
            Execution(
                run_id=run.id,
                component_id=asset.id,
                org_id=run.org_id,
                component_key=asset.key,
                status=status,
                created_at=run.created_at,
            )
        )
        session.commit()


def _run_failed_event(store: Store, run: Run, error: str) -> None:
    """Record a run's failure verdict.

    Args:
        store: The store whose engine the row is written through.
        run: The failed run.
        error: The failure's error text.
    """
    with Session(store.engine) as session:
        session.add(
            Event(
                id=uuid4(),
                org_id=run.org_id,
                run_id=run.id,
                event_type="run_failed",
                error=error,
                timestamp=run.completed_at or NOW,
            )
        )
        session.commit()


def _job(
    store: Store,
    org_id: UUID,
    name: str,
    *,
    enabled: bool = True,
    targets: list[UUID] | None = None,
    cron: str = "0 4 * * *",
) -> Component:
    """Create a cron job.

    Args:
        store: The store to create it in.
        org_id: The organisation.
        name: The job's name.
        enabled: Whether it is enabled.
        targets: The components it targets.
        cron: Its schedule.

    Returns:
        The job row.
    """
    return store.components.create(
        org_id,
        kind="job",
        key="cron_job",
        name=name,
        config={"cron": cron, "enabled": enabled},
        relations={"targets": targets} if targets else None,
    )


class TestHealthStrip:
    """The strip reads the last 24 hours of runs, what is in flight, backfills and jobs."""

    def test_runs_last_24h_bucket_by_completion_hour(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        _run(
            store,
            member.org_id,
            job,
            status="success",
            started=NOW - dt.timedelta(hours=2),
            completed=NOW - dt.timedelta(hours=1, minutes=50),
        )
        _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=2),
            completed=NOW - dt.timedelta(hours=1, minutes=40),
        )
        _run(
            store,
            member.org_id,
            job,
            status="success",
            started=NOW - dt.timedelta(days=2),
            completed=NOW - dt.timedelta(days=2),
        )

        body = _overview(client)

        assert body["runs"]["total"] == 2
        assert body["runs"]["succeeded"] == 1 and body["runs"]["failed"] == 1
        assert len(body["runs"]["hourly"]) == 24
        assert sum(b["succeeded"] + b["failed"] for b in body["runs"]["hourly"]) == 2

    def test_totals_cover_the_same_hours_as_the_bars(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        before_first_bar = NOW - dt.timedelta(hours=23, minutes=44)
        _run(store, member.org_id, job, status="success", started=before_first_bar, completed=before_first_bar)
        _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=1),
            completed=NOW - dt.timedelta(hours=1),
        )

        runs = _overview(client)["runs"]

        assert runs["hourly"][0]["hour"].startswith("2026-08-12T10:00")
        assert runs["total"] == sum(b["succeeded"] + b["failed"] for b in runs["hourly"]) == 1
        assert runs["succeeded"] == 0 and runs["failed"] == 1

    def test_activity_counts_running_and_queued(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        _run(store, member.org_id, job, status="running", started=NOW - dt.timedelta(minutes=5), completed=None)
        _run(store, member.org_id, job, status="queued", started=None, completed=None)
        _run(store, member.org_id, job, status="queued", started=None, completed=None)

        body = _overview(client)

        assert body["activity"] == {"running": 1, "queued": 2, "longest_running_seconds": 300.0}

    def test_backfills_in_progress_roll_up_their_partitions(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        job = _job(store, member.org_id, "j")
        backfill = store.runs.create_backfill(
            member.org_id, component_id=job.id, start_key="2026-07-01", end_key="2026-07-04"
        )
        with Session(store.engine) as session:
            run = session.exec(select(Run).where(Run.backfill_id == backfill.id).limit(1)).one()
            run.status = "success"
            session.add(run)
            session.commit()

        body = _overview(client)

        assert body["backfills"]["active"] == 1
        assert body["backfills"]["partitions_total"] == 4
        assert body["backfills"]["partitions_done"] == 1

    def test_jobs_failing_reads_the_latest_attempt(self, client: TestClient, store: Store, member: SimpleNamespace):
        healthy = _job(store, member.org_id, "healthy")
        failing = _job(store, member.org_id, "failing")
        _job(store, member.org_id, "off", enabled=False)
        first = _run(
            store,
            member.org_id,
            failing,
            status="failed",
            started=NOW - dt.timedelta(hours=3),
            completed=NOW - dt.timedelta(hours=3),
        )
        _run(
            store,
            member.org_id,
            failing,
            status="failed",
            started=NOW - dt.timedelta(hours=2),
            completed=NOW - dt.timedelta(hours=2),
            attempt=2,
            root=first.id,
        )
        _run(
            store,
            member.org_id,
            healthy,
            status="failed",
            started=NOW - dt.timedelta(hours=3),
            completed=NOW - dt.timedelta(hours=3),
        )
        _run(
            store,
            member.org_id,
            healthy,
            status="success",
            started=NOW - dt.timedelta(hours=1),
            completed=NOW - dt.timedelta(hours=1),
        )

        body = _overview(client)

        assert body["jobs"] == {"enabled": 2, "failing": 1}

    def test_an_attempt_that_failed_before_starting_counts(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        job = _job(store, member.org_id, "google-ads-daily")
        first = _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(days=2),
            completed=NOW - dt.timedelta(days=2),
            partition_key="2026-08-11",
        )
        _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=None,
            completed=NOW - dt.timedelta(hours=1),
            partition_key="2026-08-11",
            attempt=2,
            root=first.id,
        )

        body = _overview(client)
        stacks = [i for i in body["attention"] if i["kind"] == "run_stack"]

        assert (body["runs"]["total"], body["runs"]["failed"]) == (1, 1)
        assert [(i["title"], i["target"]) for i in stacks] == [
            ("Still failing after 2 attempts", "google-ads-daily · 2026-08-11")
        ]


class TestAttention:
    """Each kind of problem becomes one item, errors ahead of warnings."""

    def test_error_groups_merge_on_job_and_first_line(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "meta-ads-daily")
        for i in range(3):
            at = NOW - dt.timedelta(hours=1 + i)
            run = _run(store, member.org_id, job, status="failed", started=at, completed=at)
            with Session(store.engine) as session:
                session.add(
                    Event(
                        id=uuid4(),
                        org_id=member.org_id,
                        run_id=run.id,
                        event_type="run_failed",
                        error=f"token expired\ndetail {i}",
                        timestamp=at,
                    )
                )
                session.commit()

        groups = [i for i in _overview(client)["attention"] if i["kind"] == "error_group"]

        assert len(groups) == 1
        assert groups[0]["title"] == "3 runs failed: token expired"
        assert groups[0]["target"] == "meta-ads-daily"
        assert groups[0]["severity"] == "error"
        assert groups[0]["run_id"] is not None
        assert groups[0]["component_kind"] == "job"
        assert groups[0]["since"].startswith("2026-08-13T08:14")

    def test_an_error_group_of_another_target_names_no_job(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        at = NOW - dt.timedelta(hours=1)
        _run_failed_event(
            store, _run(store, member.org_id, source.children[0], status="failed", started=at, completed=at), "boom"
        )
        _run_failed_event(store, _run(store, member.org_id, None, status="failed", started=at, completed=at), "gone")

        groups = [i for i in _overview(client)["attention"] if i["kind"] == "error_group"]

        assert len(groups) == 2
        assert all(group["target"] is None and group["component_kind"] is None for group in groups)

    def test_a_connection_with_a_renewal_error_needs_reauthorisation(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        connection = store.components.create(
            member.org_id, kind="connection", key=ShopConnection.key, name="tiktok-ads", config={"api_key": "k"}
        )
        store.components.stamp_state(connection.id, last_renewal_error="invalid_grant: token revoked")

        item = next(i for i in _overview(client)["attention"] if i["kind"] == "connection")

        assert item["title"] == "tiktok-ads connection needs re-authorisation"
        assert item["target"] == "invalid_grant: token revoked"
        assert item["component_id"] == str(connection.id)

    def test_a_stack_still_failing_after_retries(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "google-ads-daily")
        first = _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=3),
            completed=NOW - dt.timedelta(hours=3),
            partition_key="2026-08-11",
        )
        _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=2),
            completed=NOW - dt.timedelta(hours=2),
            partition_key="2026-08-11",
            attempt=3,
            root=first.id,
        )

        item = next(i for i in _overview(client)["attention"] if i["kind"] == "run_stack")

        assert item["title"] == "Still failing after 3 attempts"
        assert item["target"] == "google-ads-daily · 2026-08-11"

    def test_an_older_failing_stack_stays_listed_after_a_newer_run(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        job = _job(store, member.org_id, "google-ads-daily")
        first = _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=4),
            completed=NOW - dt.timedelta(hours=4),
            partition_key="2026-08-11",
        )
        _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=3),
            completed=NOW - dt.timedelta(hours=3),
            partition_key="2026-08-11",
            attempt=2,
            root=first.id,
        )
        _run(
            store,
            member.org_id,
            job,
            status="success",
            started=NOW - dt.timedelta(hours=1),
            completed=NOW - dt.timedelta(hours=1),
            partition_key="2026-08-12",
        )

        items = [i for i in _overview(client)["attention"] if i["kind"] == "run_stack"]

        assert [i["target"] for i in items] == ["google-ads-daily · 2026-08-11"]

    def test_drift_and_overdue_are_warnings(self, client: TestClient, store: Store, member: SimpleNamespace):
        with Session(store.engine) as session:
            session.add(Component(org_id=member.org_id, kind="source", key="gone", name="Old source"))
            session.commit()
        overdue = _job(store, member.org_id, "bing-ads-daily")
        store.components.stamp_state(overdue.id, next_run_at=NOW - dt.timedelta(hours=5, minutes=12))
        on_time = _job(store, member.org_id, "fine")
        store.components.stamp_state(on_time.id, next_run_at=NOW - dt.timedelta(minutes=5))

        items = _overview(client)["attention"]
        by_kind = {i["kind"]: i for i in items}

        assert by_kind["drift"]["title"] == "Old source is missing from the catalog"
        assert by_kind["drift"]["severity"] == "warning"
        assert by_kind["overdue"]["title"] == "Scheduled run is 5h 12m overdue"
        assert by_kind["overdue"]["target"] == "bing-ads-daily"
        assert [i["kind"] for i in items if i["kind"] == "overdue"] == ["overdue"]

    def test_a_drifted_source_warns_once_for_its_assets(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        with Session(store.engine) as session:
            source = Component(org_id=member.org_id, kind="source", key="gone", name="Old source")
            session.add(source)
            session.flush()
            for key in ("orders", "customers"):
                session.add(Component(org_id=member.org_id, kind="asset", key=key, parent_id=source.id))
            session.commit()

        body = _overview(client)
        drift = [i for i in body["attention"] if i["kind"] == "drift"]
        rows = {r["kind"]: r for r in body["components"]}

        assert [i["title"] for i in drift] == ["Old source is missing from the catalog"]
        assert (rows["source"]["attention"], rows["asset"]["attention"]) == (1, 2)

    def test_errors_sort_before_warnings(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        store.components.stamp_state(job.id, next_run_at=NOW - dt.timedelta(hours=1))
        connection = store.components.create(
            member.org_id, kind="connection", key=ShopConnection.key, name="c", config={"api_key": "k"}
        )
        store.components.stamp_state(connection.id, last_renewal_error="bad")

        items = _overview(client)["attention"]

        assert [i["severity"] for i in items] == ["error", "warning"]


class TestUpcomingAndRecent:
    """Upcoming firings carry their window; recent lists stacks, not attempts."""

    def test_upcoming_lists_enabled_jobs_by_next_firing_with_their_window(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        later = _job(store, member.org_id, "later", targets=[source.id])
        sooner = _job(store, member.org_id, "sooner", targets=[source.id])
        unpartitioned = _job(store, member.org_id, "plain")
        _job(store, member.org_id, "off", enabled=False)
        store.components.stamp_state(later.id, next_run_at=NOW + dt.timedelta(hours=7))
        store.components.stamp_state(sooner.id, next_run_at=NOW + dt.timedelta(hours=4))
        store.components.stamp_state(unpartitioned.id, next_run_at=NOW + dt.timedelta(hours=5))

        upcoming = _overview(client)["upcoming"]

        assert [u["job_name"] for u in upcoming] == ["sooner", "plain", "later"]
        assert upcoming[0]["start_key"] == "2026-08-12" and upcoming[0]["end_key"] == "2026-08-12"
        assert upcoming[1]["start_key"] is None

    def test_recent_lists_the_latest_five_stacks(self, client: TestClient, store: Store, member: SimpleNamespace):
        job = _job(store, member.org_id, "j")
        first = _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=2, minutes=30),
            completed=NOW - dt.timedelta(hours=2, minutes=30),
            partition_key="2026-08-01",
        )
        for i in range(7):
            at = NOW - dt.timedelta(hours=i + 1)
            _run(
                store,
                member.org_id,
                job,
                status="success",
                started=at,
                completed=at,
                partition_key=f"2026-08-{i + 1:02d}",
                attempt=2 if i == 0 else 1,
                root=first.id if i == 0 else None,
            )

        recent = _overview(client)["recent"]

        assert [r["partition_key"] for r in recent] == [
            "2026-08-01",
            "2026-08-02",
            "2026-08-03",
            "2026-08-04",
            "2026-08-05",
        ]
        assert recent[0]["attempt"] == 2 and recent[0]["root_run_id"] == str(first.id)

    def test_queued_runs_do_not_displace_completed_ones(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        job = _job(store, member.org_id, "j")
        done = [
            _run(
                store,
                member.org_id,
                job,
                status="success",
                started=NOW - dt.timedelta(hours=i + 1),
                completed=NOW - dt.timedelta(hours=i + 1),
            )
            for i in range(2)
        ]
        store.runs.create_backfill(member.org_id, component_id=job.id, start_key="2026-07-01", end_key="2026-07-10")

        recent = _overview(client)["recent"]

        assert [r["id"] for r in recent] == [str(run.id) for run in done]


class TestComponents:
    """Each kind counts its components by one state each."""

    def test_each_kind_counts_its_states_with_precedence(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        failing_job = _job(store, member.org_id, "failing")
        _job(store, member.org_id, "off", enabled=False)
        run = _run(
            store,
            member.org_id,
            failing_job,
            status="failed",
            started=NOW - dt.timedelta(hours=1),
            completed=NOW - dt.timedelta(hours=1),
        )
        _execution(store, run, asset, "failed")
        connection = store.components.create(
            member.org_id, kind="connection", key=ShopConnection.key, name="c", config={"api_key": "k"}
        )
        store.components.stamp_state(connection.id, last_renewal_error="bad")
        with Session(store.engine) as session:
            session.add(Component(org_id=member.org_id, kind="destination", key="gone", name="old"))
            session.commit()

        rows = {r["kind"]: r for r in _overview(client)["components"]}

        assert rows["source"] == {
            "kind": "source",
            "total": 1,
            "healthy": 0,
            "failing": 1,
            "attention": 0,
            "disabled": 0,
        }
        assert rows["asset"]["failing"] == 1
        assert rows["job"] == {"kind": "job", "total": 2, "healthy": 0, "failing": 1, "attention": 0, "disabled": 1}
        assert rows["connection"]["attention"] == 1
        assert rows["destination"]["attention"] == 1
        assert rows["hook"]["total"] == 0
        assert [r["kind"] for r in rows.values()] == ["source", "asset", "destination", "connection", "job", "hook"]


class TestAuth:
    """The overview is for members holding at least the viewer role, and reads their organisation alone."""

    def test_a_viewer_is_required(self, store: Store, member: SimpleNamespace):
        app = FastAPI()
        app.include_router(overview_module.router)
        app.dependency_overrides[get_store] = lambda: store
        app.dependency_overrides[get_org_id] = lambda: member.org_id

        assert TestClient(app).get("/overview").status_code in (401, 403)

    def test_another_organisations_data_never_shows(self, client: TestClient, store: Store, member: SimpleNamespace):
        other = store.organisations.create(name="Other", creator_id=member.id).id
        source = store.components.create(other, kind="source", key=Shop.key, name="shop")
        job = _job(store, other, "other-daily", targets=[source.id])
        store.components.stamp_state(job.id, next_run_at=NOW - dt.timedelta(hours=2))
        connection = store.components.create(
            other, kind="connection", key=ShopConnection.key, name="c", config={"api_key": "k"}
        )
        store.components.stamp_state(connection.id, last_renewal_error="bad")
        with Session(store.engine) as session:
            session.add(Component(org_id=other, kind="destination", key="gone", name="old"))
            session.commit()
        first = _run(
            store,
            other,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=3),
            completed=NOW - dt.timedelta(hours=3),
            partition_key="2026-08-12",
        )
        retry = _run(
            store,
            other,
            job,
            status="failed",
            started=NOW - dt.timedelta(hours=2),
            completed=NOW - dt.timedelta(hours=2),
            partition_key="2026-08-12",
            attempt=2,
            root=first.id,
        )
        _execution(store, retry, source.children[0], "failed")
        _run_failed_event(store, retry, "token expired")
        _run(store, other, job, status="running", started=NOW - dt.timedelta(minutes=5), completed=None)
        store.runs.create_backfill(other, component_id=job.id, start_key="2026-07-01", end_key="2026-07-03")

        body = _overview(client)
        coverage = client.get(
            "/overview/coverage", params={"since": "2026-08-01", "until": "2026-08-13", "now": NOW.isoformat()}
        ).json()

        assert body["runs"]["total"] == 0
        assert body["activity"] == {"running": 0, "queued": 0, "longest_running_seconds": None}
        assert body["backfills"] == {"active": 0, "partitions_done": 0, "partitions_total": 0}
        assert body["jobs"] == {"enabled": 0, "failing": 0}
        assert body["attention"] == [] and body["upcoming"] == [] and body["recent"] == []
        assert all(row["total"] == 0 for row in body["components"])
        assert coverage["jobs"] == [] and coverage["days"] == []


class TestCoverage:
    """Each job's asset-partitions roll onto days by the calendar's day rules."""

    def _partition_run(
        self, store: Store, member: SimpleNamespace, job: Component, asset: Component, key: str, status: str
    ) -> Run:
        """Record a run of *job* for one partition, with one execution of *asset*.

        Args:
            store: The store whose engine the rows are written through.
            member: The member whose organisation the run belongs to.
            job: The run's target.
            asset: The executed asset.
            key: The run's partition key.
            status: The run's and the execution's status.

        Returns:
            The persisted run.
        """
        run = _run(
            store,
            member.org_id,
            job,
            status=status,
            started=NOW - dt.timedelta(days=1),
            completed=NOW - dt.timedelta(days=1),
            partition_key=key,
        )
        _execution(store, run, asset, status)
        return run

    def test_days_run_from_the_first_attempt_to_yesterday(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        job = _job(store, member.org_id, "daily", targets=[source.id])
        self._partition_run(store, member, job, asset, "2026-08-10", "success")
        failed = self._partition_run(store, member, job, asset, "2026-08-11", "failed")

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-01", "until": "2026-08-13", "now": NOW.isoformat()}
        ).json()
        days = {d["date"]: d for d in body["days"]}

        assert body["jobs"] == [{"id": str(job.id), "name": "daily"}]
        assert sorted(days) == ["2026-08-10", "2026-08-11", "2026-08-12"]
        assert days["2026-08-10"] == {
            "date": "2026-08-10",
            "job_id": str(job.id),
            "expected": 1,
            "covered": 1,
            "failed": 0,
            "failed_run_id": None,
        }
        assert days["2026-08-11"]["failed"] == 1 and days["2026-08-11"]["failed_run_id"] == str(failed.id)
        assert days["2026-08-12"] == {
            "date": "2026-08-12",
            "job_id": str(job.id),
            "expected": 1,
            "covered": 0,
            "failed": 0,
            "failed_run_id": None,
        }

    def test_today_counts_once_attempted_and_a_disabled_job_stops_at_its_last_partition(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        live = _job(store, member.org_id, "live", targets=[source.id])
        off = _job(store, member.org_id, "off", enabled=False, targets=[source.id])
        self._partition_run(store, member, live, asset, "2026-08-13", "success")
        self._partition_run(store, member, off, asset, "2026-08-05", "success")

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-01", "until": "2026-08-13", "now": NOW.isoformat()}
        ).json()
        by_job = defaultdict(list)
        for day in body["days"]:
            by_job[day["job_id"]].append(day["date"])

        assert by_job[str(live.id)] == ["2026-08-13"]
        assert by_job[str(off.id)] == ["2026-08-05"]

    def test_hourly_and_monthly_keys_roll_onto_days(self, client: TestClient, store: Store, member: SimpleNamespace):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        hourly = _job(store, member.org_id, "hourly", targets=[source.id])
        monthly = _job(store, member.org_id, "monthly", targets=[source.id])
        self._partition_run(store, member, hourly, asset, "2026-08-12T00", "success")
        self._partition_run(store, member, hourly, asset, "2026-08-12T01", "failed")
        self._partition_run(store, member, monthly, asset, "2026-07", "success")

        body = client.get(
            "/overview/coverage", params={"since": "2026-07-30", "until": "2026-08-13", "now": NOW.isoformat()}
        ).json()
        days = {(d["job_id"], d["date"]): d for d in body["days"]}

        hour_day = days[(str(hourly.id), "2026-08-12")]
        assert (hour_day["expected"], hour_day["covered"], hour_day["failed"]) == (24, 1, 1)
        assert days[(str(monthly.id), "2026-07-30")]["covered"] == 1
        assert days[(str(monthly.id), "2026-07-31")]["covered"] == 1
        assert (str(monthly.id), "2026-08-01") not in days

    def test_an_attempt_still_in_flight_is_missing_not_failed(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        job = _job(store, member.org_id, "daily", targets=[source.id])
        self._partition_run(store, member, job, asset, "2026-08-11", "running")
        self._partition_run(store, member, job, asset, "2026-08-12", "canceled")

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-11", "until": "2026-08-12", "now": NOW.isoformat()}
        ).json()

        assert [(d["date"], d["expected"], d["covered"], d["failed"]) for d in body["days"]] == [
            ("2026-08-11", 1, 0, 0),
            ("2026-08-12", 1, 0, 0),
        ]

    def test_runs_of_other_targets_are_left_out(self, client: TestClient, store: Store, member: SimpleNamespace):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        run = _run(
            store,
            member.org_id,
            asset,
            status="success",
            started=NOW - dt.timedelta(days=1),
            completed=NOW - dt.timedelta(days=1),
            partition_key="2026-08-12",
        )
        _execution(store, run, asset, "success")

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-01", "until": "2026-08-13", "now": NOW.isoformat()}
        ).json()

        assert body["jobs"] == [] and body["days"] == []

    def test_an_hourly_job_owes_only_the_hours_elapsed_today(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        job = _job(store, member.org_id, "hourly", targets=[source.id])
        self._partition_run(store, member, job, asset, "2026-08-13T00", "success")
        self._partition_run(store, member, job, asset, "2026-08-13T01", "success")
        self._partition_run(store, member, job, asset, "2026-08-13T08", "failed")

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-13", "until": "2026-08-13", "now": NOW.isoformat()}
        ).json()

        [today] = body["days"]
        assert (today["expected"], today["covered"], today["failed"]) == (9, 2, 1)

    def test_a_partition_spanning_past_today_stops_at_today(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        job = _job(store, member.org_id, "monthly", targets=[source.id])
        self._partition_run(store, member, job, asset, "2026-08", "success")

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-01", "until": "2026-08-31", "now": NOW.isoformat()}
        ).json()

        assert [d["date"] for d in body["days"]] == [f"2026-08-{day:02d}" for day in range(1, 14)]

    def test_a_yearly_key_covers_its_days_and_the_open_year_is_not_expected(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        job = _job(store, member.org_id, "yearly", targets=[source.id])
        self._partition_run(store, member, job, asset, "2025", "success")

        body = client.get(
            "/overview/coverage", params={"since": "2025-12-30", "until": "2026-01-02", "now": NOW.isoformat()}
        ).json()

        assert [(d["date"], d["covered"]) for d in body["days"]] == [("2025-12-30", 1), ("2025-12-31", 1)]

    def test_a_day_counts_each_asset(self, client: TestClient, store: Store, member: SimpleNamespace):
        first = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        second = store.components.create(
            member.org_id, kind="source", key=Shop.key, name="shop-eu", config={"dataset": "eu"}
        )
        job = _job(store, member.org_id, "daily", targets=[first.id, second.id])
        run = _run(
            store,
            member.org_id,
            job,
            status="failed",
            started=NOW - dt.timedelta(days=1),
            completed=NOW - dt.timedelta(days=1),
            partition_key="2026-08-12",
        )
        _execution(store, run, first.children[0], "success")
        _execution(store, run, second.children[0], "failed")

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-12", "until": "2026-08-12", "now": NOW.isoformat()}
        ).json()

        [day] = body["days"]
        assert (day["expected"], day["covered"], day["failed"]) == (2, 1, 1)
        assert day["failed_run_id"] == str(run.id)

    def test_the_failed_run_kept_for_a_day_does_not_depend_on_row_order(
        self, client: TestClient, store: Store, member: SimpleNamespace
    ):
        source = store.components.create(member.org_id, kind="source", key=Shop.key, name="shop")
        asset = source.children[0]
        job = _job(store, member.org_id, "hourly", targets=[source.id])
        runs = [self._partition_run(store, member, job, asset, f"2026-08-12T{hour:02d}", "failed") for hour in range(4)]

        body = client.get(
            "/overview/coverage", params={"since": "2026-08-12", "until": "2026-08-12", "now": NOW.isoformat()}
        ).json()

        assert body["days"][0]["failed_run_id"] == str(max(run.id for run in runs))

    def test_a_twelve_month_window_as_the_client_sends_it_is_accepted(self, client: TestClient):
        response = client.get("/overview/coverage", params={"since": "2025-09-01", "until": "2026-09-30"})

        assert response.status_code == 200

    def test_the_window_bound_is_inclusive(self, client: TestClient):
        since = dt.date(2025, 9, 1)
        span = overview_module.MAX_COVERAGE_SPAN_DAYS

        at_bound = client.get(
            "/overview/coverage",
            params={"since": since.isoformat(), "until": (since + dt.timedelta(days=span - 1)).isoformat()},
        )
        over_bound = client.get(
            "/overview/coverage",
            params={"since": since.isoformat(), "until": (since + dt.timedelta(days=span)).isoformat()},
        )

        assert at_bound.status_code == 200
        assert over_bound.status_code == 422

    def test_an_empty_window_is_rejected(self, client: TestClient):
        response = client.get("/overview/coverage", params={"since": "2026-08-13", "until": "2026-08-12"})

        assert response.status_code == 422
