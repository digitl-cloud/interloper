"""Tests for the run lifecycle methods in ``RunStore`` (``store/runs.py``).

These run against an in-memory SQLite database (only the runs/backfills
tables) so status transitions are exercised against real SQL. The stack's
concurrency guarantees rest on Postgres row locks, which SQLite does not have:
those tests read a server DSN from ``INTERLOPER_TEST_POSTGRES_DSN``, provision
a throwaway database, and skip without the variable.
"""

from __future__ import annotations

import datetime as dt
import os
import threading
from collections.abc import Callable, Iterator
from typing import Any, ClassVar
from urllib.parse import urlparse, urlunparse
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import ConfigError, ConflictError, NotFoundError
from interloper.partitioning.time import TimeGranularity
from pydantic import ValidationError
from sqlalchemy import Engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, select

from interloper_db import engine as engine_module
from interloper_db import provision
from interloper_db.models import Backfill, Component, Event, Quota, Run, Usage
from interloper_db.store import RunQuery, RunStore, Store
from interloper_db.store.runs import partition_key_range

_ORG_ID = uuid4()


class FakePlumbing(il.Operation):
    """Test-only kind whose operation is platform plumbing (non-billable)."""

    kind: ClassVar[str] = "fake_plumbing"
    billable: ClassVar[bool] = False

    async def execute(self, context: il.OperationContext) -> il.OperationResult:
        """Do nothing.

        Args:
            context: The platform-provided execution context, unused.

        Returns:
            An effectless success.
        """
        return il.OperationResult()


il.KINDS.register(FakePlumbing.kind, FakePlumbing.anchor())


@pytest.fixture
def store() -> Iterator[Store]:
    """A store wired to a fresh in-memory SQLite database.

    Yields:
        The store bound to that database, disposed once the test finishes.
    """
    engine = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _sqlite_uuid(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.create_function("gen_random_uuid", 0, lambda: uuid4().hex)

    for model in (Backfill, Component, Run, Event, Quota, Usage):
        model.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}), engine=engine)
    finally:
        engine.dispose()
        engine_module._engine = None


def _backfill(store: Store, *, days: int = 4, concurrency: int = 2) -> Backfill:
    """Start a daily backfill over a fresh job, from 2026-01-01 on.

    Args:
        store: The store under test.
        days: How many daily partitions it spans.
        concurrency: How many of its runs are queued at once.

    Returns:
        The created backfill.
    """
    return store.backfills.create(
        _ORG_ID,
        component_id=_component(store, kind="job"),
        start_key="2026-01-01",
        end_key=f"2026-01-{days:02d}",
        concurrency=concurrency,
    )


def _component(store: Store, kind: str, key: str | None = None, name: str | None = None) -> UUID:
    with Session(store.engine) as session:
        row = Component(id=uuid4(), org_id=_ORG_ID, kind=kind, key=key or kind, name=name or kind)
        session.add(row)
        session.commit()
        assert row.id is not None
        return row.id


def _job_with_retry(store: Store, **policy: Any) -> UUID:
    """A job component whose config declares a retry policy.

    Returns:
        The component id.
    """
    with Session(store.engine) as session:
        row = Component(
            id=uuid4(),
            org_id=_ORG_ID,
            kind="job",
            key="job",
            name="job",
            config={"retry": policy} if policy else {},
        )
        session.add(row)
        session.commit()
        assert row.id is not None
        return row.id


class TestRunTargetOperations:
    """Run creation validates the target's operation and records billability."""

    def test_kind_without_operation_is_rejected(self, store: Store):
        target = _component(store, kind="destination")
        with pytest.raises(ConfigError, match="cannot be run"):
            store.runs.create(_ORG_ID, component_id=target)

    def test_missing_component_is_rejected(self, store: Store):
        with pytest.raises(NotFoundError):
            store.runs.create(_ORG_ID, component_id=uuid4())

    def test_billable_recorded_from_the_operation(self, store: Store):
        target = _component(store, kind="fake_plumbing")
        run = store.runs.create(_ORG_ID, component_id=target)
        assert run.billable is False


class TestTargetResolution:
    """Runs carry their target component, eagerly joined and deletion-aware."""

    def test_target_is_loaded_with_the_run(self, store: Store):
        target = _component(store, kind="job")
        created = store.runs.create(_ORG_ID, component_id=target)

        # Every access below happens after the store call returned (its
        # session is closed), so it only works if each path loaded the
        # relationship — create by touching it, get/list by eager join.
        assert created.target is not None and created.target.key == "job"

        run = store.runs.get(created.id)
        assert run.target is not None
        assert (run.target.kind, run.target.key, run.target.name) == ("job", "job", "job")

        listed = store.runs.list(_ORG_ID, RunQuery()).items
        assert [r.target.key for r in listed if r.target] == ["job"]

    def test_deleted_target_resolves_to_none(self, store: Store):
        target = _component(store, kind="job")
        created = store.runs.create(_ORG_ID, component_id=target)

        # Mirror the FK's ON DELETE SET NULL by hand — SQLite does not
        # enforce it without the foreign_keys pragma.
        with Session(store.engine) as session:
            run_row = session.get(Run, created.id)
            component = session.get(Component, target)
            assert run_row is not None and component is not None
            run_row.component_id = None
            session.add(run_row)
            session.delete(component)
            session.commit()

        run = store.runs.get(created.id)
        assert run.component_id is None
        assert run.target is None

    def test_retry_copies_the_record(self, store: Store):
        target = _component(store, kind="fake_plumbing")
        run = store.runs.create(_ORG_ID, component_id=target)
        store.runs.complete(run.id, success=False)

        retry = store.runs.retry(run.id)

        assert retry.billable is False


class TestStackIdentity:
    """Every run belongs to a stack; a first attempt is its own root."""

    def test_a_new_run_is_its_own_stack_root(self, store: Store) -> None:
        run = store.runs.create(_ORG_ID)

        assert run.root_run_id == run.id
        assert run.scheduled_for is None
        assert run.attempt == 1

    def test_a_manual_retry_joins_its_predecessors_stack(self, store: Store) -> None:
        run = store.runs.create(_ORG_ID)
        store.runs.complete(run.id, success=False)

        retry = store.runs.retry(run.id)

        assert retry.root_run_id == run.root_run_id
        assert retry.id != run.id
        assert retry.attempt == 2


class TestAutomaticRetry:
    """A failed run queues its own next attempt when its target allows one."""

    def test_a_failed_run_queues_its_next_attempt(self, store: Store) -> None:
        target = _job_with_retry(store, max_attempts=2, delay=60)
        run = store.runs.create(_ORG_ID, component_id=target)

        store.runs.complete(run.id, success=False)

        with Session(store.engine) as session:
            successor = session.exec(select(Run).where(Run.retry_of == run.id)).one()
        assert successor.root_run_id == run.root_run_id
        assert successor.attempt == 2
        assert successor.retry_scope == "failed"
        assert successor.status == "queued"
        assert successor.scheduled_for is not None
        assert successor.billable == run.billable

    def test_an_exhausted_budget_queues_nothing(self, store: Store) -> None:
        target = _job_with_retry(store, max_attempts=1)
        run = store.runs.create(_ORG_ID, component_id=target)

        store.runs.complete(run.id, success=False)

        with Session(store.engine) as session:
            assert session.exec(select(Run).where(Run.retry_of == run.id)).all() == []

    def test_a_successful_run_queues_nothing(self, store: Store) -> None:
        target = _job_with_retry(store, max_attempts=3)
        run = store.runs.create(_ORG_ID, component_id=target)

        store.runs.complete(run.id, success=True)

        with Session(store.engine) as session:
            assert session.exec(select(Run).where(Run.retry_of == run.id)).all() == []

    def test_a_target_declaring_no_policy_queues_nothing(self, store: Store) -> None:
        target = _component(store, kind="job")
        run = store.runs.create(_ORG_ID, component_id=target)

        store.runs.complete(run.id, success=False)

        with Session(store.engine) as session:
            assert session.exec(select(Run).where(Run.retry_of == run.id)).all() == []

    def test_a_source_policy_is_not_read_at_the_run_level(self, store: Store) -> None:
        # A source's `retry` is an operation budget; reading it here would
        # apply an operation's attempts to whole runs.
        with Session(store.engine) as session:
            row = Component(
                id=uuid4(),
                org_id=_ORG_ID,
                kind="source",
                key="shop",
                name="shop",
                config={"retry": {"max_attempts": 5}},
            )
            session.add(row)
            session.commit()
            target = row.id
        run = store.runs.create(_ORG_ID, component_id=target)

        store.runs.complete(run.id, success=False)

        with Session(store.engine) as session:
            assert session.exec(select(Run).where(Run.retry_of == run.id)).all() == []

    def test_a_run_whose_target_is_gone_queues_nothing(self, store: Store) -> None:
        run = store.runs.create(_ORG_ID)

        store.runs.complete(run.id, success=False)

        with Session(store.engine) as session:
            assert session.exec(select(Run).where(Run.retry_of == run.id)).all() == []

    def test_the_successor_stays_in_its_backfill(self, store: Store) -> None:
        target = _job_with_retry(store, max_attempts=2, delay=0)
        backfill = store.backfills.create(_ORG_ID, component_id=target, start_key="2026-01-01", end_key="2026-01-01")
        with Session(store.engine) as session:
            run = session.exec(select(Run).where(Run.backfill_id == backfill.id)).one()

        store.runs.complete(run.id, success=False)

        with Session(store.engine) as session:
            successor = session.exec(select(Run).where(Run.retry_of == run.id)).one()
        assert successor.backfill_id == backfill.id


class TestStackNativeListing:
    """A listing shows one row per stack: its latest attempt."""

    def _failed_then(self, store: Store, *, success: bool) -> tuple[Run, Run]:
        """A two-attempt stack whose second attempt ends as asked.

        Returns:
            The first attempt and its successor.
        """
        target = _job_with_retry(store, max_attempts=2, delay=0)
        first = store.runs.create(_ORG_ID, component_id=target)
        store.runs.complete(first.id, success=False)
        with Session(store.engine) as session:
            successor = session.exec(select(Run).where(Run.retry_of == first.id)).one()
        store.runs.complete(successor.id, success=success)
        return first, successor

    def test_a_stack_is_one_row_at_its_latest_attempt(self, store: Store) -> None:
        first, successor = self._failed_then(store, success=True)

        runs = store.runs.list(_ORG_ID, RunQuery()).items

        assert [run.id for run in runs] == [successor.id]
        assert runs[0].attempt == 2
        assert first.id not in {run.id for run in runs}

    def test_the_total_matches_the_listing(self, store: Store) -> None:
        self._failed_then(store, success=True)

        page = store.runs.list(_ORG_ID, RunQuery())

        assert len(page.items) == 1
        assert page.total == 1

    def test_a_status_filter_reads_the_stacks_verdict(self, store: Store) -> None:
        # The first attempt failed, so a run-level filter would surface it; the
        # stack succeeded, and that is what a reader means by "failed runs".
        self._failed_then(store, success=True)

        assert store.runs.list(_ORG_ID, RunQuery(status="failed")).items == []
        assert len(store.runs.list(_ORG_ID, RunQuery(status="success")).items) == 1

    def test_an_exhausted_stack_still_reads_as_failed(self, store: Store) -> None:
        self._failed_then(store, success=False)

        assert len(store.runs.list(_ORG_ID, RunQuery(status="failed")).items) == 1

    def test_a_stack_lists_its_attempts_newest_first(self, store: Store) -> None:
        first, successor = self._failed_then(store, success=True)

        attempts = store.runs.list(_ORG_ID, RunQuery(root_run_id=first.root_run_id))

        assert [run.id for run in attempts.items] == [successor.id, first.id]
        assert attempts.total == 2

    def test_unretried_runs_are_unaffected(self, store: Store) -> None:
        first = store.runs.create(_ORG_ID)
        second = store.runs.create(_ORG_ID)

        runs = store.runs.list(_ORG_ID, RunQuery()).items

        assert {run.id for run in runs} == {first.id, second.id}


def _H(hours: int) -> dt.timedelta:
    return dt.timedelta(hours=hours)


def _timed_run(
    store: Store,
    *,
    started_at: dt.datetime | None,
    completed_at: dt.datetime | None,
    org_id: UUID = _ORG_ID,
    status: str | None = None,
    created_at: dt.datetime | None = None,
) -> UUID:
    """Insert a run occupying a known interval.

    Args:
        store: The store whose database the run is written to.
        started_at: When the run started; ``None`` for one that never did.
        completed_at: When the run completed; ``None`` for one still going.
        org_id: The organisation the run belongs to.
        status: The run's status; ``None`` derives ``success`` from a set
            *completed_at* and ``running`` otherwise.
        created_at: The run's creation instant; ``None`` keeps the default.

    Returns:
        The id of the inserted run.
    """
    with Session(store.engine) as session:
        run = Run(
            id=uuid4(),
            org_id=org_id,
            status=status or ("success" if completed_at else "running"),
            started_at=started_at,
            completed_at=completed_at,
        )
        if created_at is not None:
            run.created_at = created_at
        session.add(run)
        session.commit()
        return run.id


class TestListRunsWindow:
    """`after`/`before` select runs whose execution overlaps the window."""

    def test_overlapping_runs_only(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        before_window = _timed_run(store, started_at=base - _H(3), completed_at=base - _H(2))
        straddling_start = _timed_run(store, started_at=base - _H(2), completed_at=base + _H(1))
        inside = _timed_run(store, started_at=base + _H(2), completed_at=base + _H(3))
        after_window = _timed_run(store, started_at=base + _H(6), completed_at=base + _H(7))

        found = store.runs.list(_ORG_ID, RunQuery(after=base, before=base + _H(4), limit=100)).items

        assert {r.id for r in found} == {straddling_start, inside}
        assert before_window not in {r.id for r in found}
        assert after_window not in {r.id for r in found}

    def test_running_run_is_open_ended(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        running = _timed_run(store, started_at=base - _H(5), completed_at=None)

        found = store.runs.list(_ORG_ID, RunQuery(after=base, before=base + _H(1), limit=100)).items

        assert [r.id for r in found] == [running]

    def test_never_started_runs_are_excluded(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=None, completed_at=None)

        assert store.runs.list(_ORG_ID, RunQuery(after=base, before=base + _H(1), limit=100)).items == []
        after_only = store.runs.list(_ORG_ID, RunQuery(after=base, limit=100))
        assert after_only.items == []
        assert after_only.total == 0

    def test_the_total_matches_the_same_window(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=base + _H(1), completed_at=base + _H(2))
        _timed_run(store, started_at=base + _H(9), completed_at=base + _H(10))

        page = store.runs.list(_ORG_ID, RunQuery(after=base, before=base + _H(4)))

        assert len(page.items) == 1
        assert page.total == 1

    def test_unbounded_listing_keeps_every_run(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        _timed_run(store, started_at=base, completed_at=base + _H(1))
        _timed_run(store, started_at=None, completed_at=None)

        assert len(store.runs.list(_ORG_ID, RunQuery(limit=100)).items) == 2


class TestListRunsSort:
    """`sort` orders the listing on a whitelisted field, the id breaking ties."""

    def test_a_backfill_pages_by_partition_without_overlap(self, store: Store):
        backfill = _backfill(store, days=5, concurrency=1)

        pages = [
            store.runs.list(_ORG_ID, RunQuery(backfill_id=backfill.id, sort="partition_key", limit=2, offset=offset))
            for offset in (0, 2, 4)
        ]

        assert {page.total for page in pages} == {5}
        assert [[run.partition_key for run in page.items] for page in pages] == [
            ["2026-01-01", "2026-01-02"],
            ["2026-01-03", "2026-01-04"],
            ["2026-01-05"],
        ]

    def test_a_dash_prefix_sorts_descending(self, store: Store):
        backfill = _backfill(store, days=3, concurrency=1)

        runs = store.runs.list(_ORG_ID, RunQuery(backfill_id=backfill.id, sort="-partition_key")).items

        assert [run.partition_key for run in runs] == ["2026-01-03", "2026-01-02", "2026-01-01"]

    def test_runs_never_started_sort_last_either_way(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        earlier = _timed_run(store, started_at=base, completed_at=base + _H(1))
        later = _timed_run(store, started_at=base + _H(2), completed_at=base + _H(3))
        never = _timed_run(store, started_at=None, completed_at=None)

        ascending = store.runs.list(_ORG_ID, RunQuery(sort="started_at")).items
        descending = store.runs.list(_ORG_ID, RunQuery(sort="-started_at")).items

        assert [run.id for run in ascending] == [earlier, later, never]
        assert [run.id for run in descending] == [later, earlier, never]

    def test_an_unknown_field_is_rejected(self):
        # The query string binds straight to the model, so validation is the gate.
        with pytest.raises(ValidationError, match="sort"):
            RunQuery.model_validate({"sort": "-org_id"})


class TestListRunsCompleted:
    """`completed_after`/`completed_before` select runs by the instant they completed."""

    def test_a_run_that_failed_before_starting_is_kept(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        never_started = _timed_run(store, started_at=None, completed_at=base + _H(1), status="failed")

        by_completion = store.runs.list(_ORG_ID, RunQuery(completed_after=base, completed_before=base + _H(4)))
        by_overlap = store.runs.list(_ORG_ID, RunQuery(after=base, before=base + _H(4)))

        assert [r.id for r in by_completion.items] == [never_started]
        assert by_completion.total == 1
        assert by_overlap.items == []

    def test_runs_not_completed_or_completed_outside_are_left_out(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        at_start = _timed_run(store, started_at=base - _H(1), completed_at=base)
        at_end = _timed_run(store, started_at=base, completed_at=base + _H(4))
        _timed_run(store, started_at=base - _H(3), completed_at=base - _H(2))
        _timed_run(store, started_at=base + _H(1), completed_at=base + _H(5))
        _timed_run(store, started_at=base, completed_at=None)
        _timed_run(store, started_at=None, completed_at=None, status="queued")

        found = store.runs.list(_ORG_ID, RunQuery(completed_after=base, completed_before=base + _H(4))).items

        assert {r.id for r in found} == {at_start, at_end}
        assert len(store.runs.list(_ORG_ID, RunQuery(completed_before=base + _H(4))).items) == 3

    def test_completed_order_lists_the_most_recently_completed_first(self, store: Store):
        base = dt.datetime(2026, 2, 4, 12, 0, tzinfo=dt.timezone.utc)
        long_run = _timed_run(store, started_at=base, completed_at=base + _H(5), created_at=base)
        short_run = _timed_run(store, started_at=base + _H(1), completed_at=base + _H(2), created_at=base + _H(1))

        by_created = store.runs.list(_ORG_ID, RunQuery()).items
        by_completed = store.runs.list(_ORG_ID, RunQuery(sort="-completed_at")).items

        assert [r.id for r in by_created] == [short_run, long_run]
        assert [r.id for r in by_completed] == [long_run, short_run]


class TestGetAndComplete:
    """Id-addressed reads and the terminal transition."""

    def test_get_returns_the_run(self, store: Store):
        run = store.runs.create(_ORG_ID)

        assert store.runs.get(run.id).id == run.id

    def test_get_missing_run_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Run {missing} not found"):
            store.runs.get(missing)

    def test_another_orgs_run_reads_as_missing(self, store: Store):
        run = store.runs.create(_ORG_ID)

        assert store.runs.get(run.id, org_id=_ORG_ID).id == run.id
        with pytest.raises(NotFoundError, match=f"Run {run.id} not found"):
            store.runs.get(run.id, org_id=uuid4())

    def test_complete_refuses_a_terminal_run(self, store: Store):
        # The reaper failing a run whose pod finally started must not overwrite
        # the executor's verdict, nor queue a retry of work that succeeded.
        run = store.runs.create(_ORG_ID)
        store.runs.complete(run.id, success=True)

        with pytest.raises(ConflictError, match=f"Run {run.id} is already success"):
            store.runs.complete(run.id, success=False)
        assert store.runs.get(run.id).status == "success"

    def test_complete_records_success(self, store: Store):
        run = store.runs.create(_ORG_ID)

        store.runs.complete(run.id, success=True)

        completed = store.runs.get(run.id)
        assert completed.status == "success"
        assert completed.completed_at is not None

    def test_complete_records_failure(self, store: Store):
        run = store.runs.create(_ORG_ID)

        store.runs.complete(run.id, success=False)

        assert store.runs.get(run.id).status == "failed"

    def test_complete_missing_run_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Run {missing} not found"):
            store.runs.complete(missing, success=True)


class TestCompletionStamping:
    """Completing a run stamps its target's ``last_run_at`` and ``last_run_status``."""

    def _job_run(self, store: Store) -> tuple[UUID, UUID]:
        """Insert a job and a running run targeting it, in an organisation of their own.

        Args:
            store: The store under test.

        Returns:
            The job's id and the run's id.
        """
        org = uuid4()
        with Session(store.engine) as session:
            job = Component(org_id=org, kind="job", key="cron_job", name="J")
            session.add(job)
            session.flush()
            run = Run(id=uuid4(), org_id=org, component_id=job.id, status="running")
            session.add(run)
            session.commit()
            return job.id, run.id

    def test_a_success_stamps_the_jobs_last_run_at_and_status(self, store: Store) -> None:
        component_id, run_id = self._job_run(store)

        completed = store.runs.complete(run_id, success=True)
        assert completed.status == "success"
        assert completed.completed_at is not None

        with Session(store.engine) as session:
            stamped = session.get(Component, component_id)
            assert stamped is not None and stamped.state is not None
            # SQLite round-trips the column naive; the stamped ISO string is aware UTC.
            stamped_at = dt.datetime.fromisoformat(stamped.state["last_run_at"])
            assert stamped_at == completed.completed_at.replace(tzinfo=dt.timezone.utc)
            assert stamped.state["last_run_status"] == "success"

    def test_a_failure_stamps_a_failed_status(self, store: Store) -> None:
        component_id, run_id = self._job_run(store)

        store.runs.complete(run_id, success=False)

        with Session(store.engine) as session:
            stamped = session.get(Component, component_id)
            assert stamped is not None and stamped.state is not None
            assert stamped.state["last_run_status"] == "failed"


class TestPartitionKeyValidation:
    """A run's partition key must be a shape the framework recognises."""

    def test_a_well_formed_key_is_accepted(self, store: Store):
        run = store.runs.create(_ORG_ID, partition_key="2026-01-01")

        assert run.partition_key == "2026-01-01"

    def test_an_unrecognised_shape_is_rejected(self, store: Store):
        with pytest.raises(ConfigError):
            store.runs.create(_ORG_ID, partition_key="not-a-key")

    @pytest.mark.parametrize(
        ("key", "granularity"),
        [
            ("2026-08-21", TimeGranularity.DAY),
            ("2026-08", TimeGranularity.MONTH),
            ("2026", TimeGranularity.YEAR),
            ("2026-08-21T13", TimeGranularity.HOUR),
        ],
    )
    def test_parse_partition_reads_the_granularity_off_the_shape(self, key: str, granularity: TimeGranularity):
        assert RunStore.parse_partition(key).granularity is granularity

    def test_parse_partition_refuses_an_unknown_shape_as_a_config_error(self):
        with pytest.raises(ConfigError):
            RunStore.parse_partition("not-a-key")


class TestRetryValidation:
    """Only a failed run can be retried, and only with a known scope."""

    def test_an_unknown_scope_is_rejected(self, store: Store):
        run = store.runs.create(_ORG_ID)

        with pytest.raises(ConfigError, match="Invalid retry scope: 'sideways'"):
            store.runs.retry(run.id, scope="sideways")

    def test_a_missing_run_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Run {missing} not found"):
            store.runs.retry(missing)

    def test_a_run_that_did_not_fail_is_rejected(self, store: Store):
        run = store.runs.create(_ORG_ID)

        with pytest.raises(ConflictError, match="is not failed"):
            store.runs.retry(run.id)

    def test_an_earlier_attempt_retries_the_stack_from_its_head(self, store: Store):
        first = store.runs.create(_ORG_ID)
        store.runs.complete(first.id, success=False)
        second = store.runs.retry(first.id)
        store.runs.complete(second.id, success=False)

        third = store.runs.retry(first.id)

        assert (third.attempt, third.retry_of, third.root_run_id) == (3, second.id, first.id)

    def test_a_stack_with_an_attempt_in_flight_is_not_retried_again(self, store: Store):
        target = _job_with_retry(store, max_attempts=2, delay=60)
        run = store.runs.create(_ORG_ID, component_id=target)
        store.runs.complete(run.id, success=False)

        with pytest.raises(ConflictError, match="latest attempt 2 is 'queued'"):
            store.runs.retry(run.id)

    def test_a_stack_healed_by_a_later_attempt_is_not_retried(self, store: Store):
        first = store.runs.create(_ORG_ID)
        store.runs.complete(first.id, success=False)
        second = store.runs.retry(first.id)
        store.runs.complete(second.id, success=True)

        with pytest.raises(ConflictError, match="latest attempt 2 is 'success'"):
            store.runs.retry(first.id)

    def test_a_head_superseded_while_its_lock_was_awaited_is_refused(self, store: Store):
        # SQLite has no row locks, so the rival retry is written right after
        # the locked read of the head, where a concurrent commit would land.
        first = store.runs.create(_ORG_ID)
        store.runs.complete(first.id, success=False)

        rivals: list[Run] = []

        def commit_a_rival_retry(orm_execute_state: Any) -> Any:
            if rivals or not orm_execute_state.execution_options.get("populate_existing"):
                return None
            locked_read = orm_execute_state.invoke_statement().freeze()
            rivals.append(Run(org_id=_ORG_ID, status="queued", retry_of=first.id, root_run_id=first.id, attempt=2))
            orm_execute_state.session.add(rivals[0])
            return locked_read()

        event.listen(Session, "do_orm_execute", commit_a_rival_retry)
        try:
            with pytest.raises(ConflictError, match="stack was retried concurrently"):
                store.runs.retry(first.id)
        finally:
            event.remove(Session, "do_orm_execute", commit_a_rival_retry)
        assert rivals

    def test_the_failed_scope_is_accepted(self, store: Store):
        run = store.runs.create(_ORG_ID)
        store.runs.complete(run.id, success=False)

        retried = store.runs.retry(run.id, scope="failed")

        assert retried.retry_of == run.id
        assert retried.retry_scope == "failed"


class TestAllAttemptsAndPartitionRange:
    """``all_attempts`` keeps every attempt; a partition range bounds by key and granularity."""

    def _add(self, store: Store, **fields: Any) -> UUID:
        run = Run(org_id=_ORG_ID, **{"status": "success", **fields})
        with Session(store.engine) as session:
            session.add(run)
            session.commit()
            assert run.id is not None
            return run.id

    def test_all_attempts_lists_and_counts_every_attempt(self, store: Store):
        first = self._add(store, status="failed")
        second = self._add(store, retry_of=first, root_run_id=first, attempt=2)

        latest = store.runs.list(_ORG_ID, RunQuery())
        every_attempt = store.runs.list(_ORG_ID, RunQuery(all_attempts=True))

        assert [run.id for run in latest.items] == [second]
        assert latest.total == 1
        assert {run.id for run in every_attempt.items} == {first, second}
        assert every_attempt.total == 2

    def test_partition_range_excludes_other_granularities(self, store: Store):
        inside = self._add(store, partition_key="2026-07-02")
        self._add(store, partition_key="2026-07-02T13")
        self._add(store, partition_key="2026-08-01")

        with Session(store.engine) as session:
            matched = session.exec(select(Run.id).where(*partition_key_range("2026-07-01", "2026-07-31"))).all()

        assert matched == [inside]

    def test_partition_key_range_bounds_by_value_and_granularity(self):
        assert len(partition_key_range("2026-07-01", "2026-07-31")) == 3


class TestRunFilters:
    """``list`` narrows on component, backfill, status and the target's identity, and its total follows."""

    def test_the_component_filter_narrows_the_listing(self, store: Store):
        target = _component(store, kind="source")
        store.runs.create(_ORG_ID, component_id=target)
        store.runs.create(_ORG_ID)

        page = store.runs.list(_ORG_ID, RunQuery(component_id=target))

        assert len(page.items) == 1
        assert page.total == 1

    def test_the_backfill_filter_narrows_the_listing(self, store: Store):
        backfill = _backfill(store, days=2)
        store.runs.create(_ORG_ID)

        page = store.runs.list(_ORG_ID, RunQuery(backfill_id=backfill.id))

        assert len(page.items) == 2
        assert page.total == 2

    def test_the_status_filter_narrows_the_listing(self, store: Store):
        succeeded = store.runs.create(_ORG_ID)
        store.runs.complete(succeeded.id, success=True)
        store.runs.create(_ORG_ID)

        page = store.runs.list(_ORG_ID, RunQuery(status="success"))

        assert [row.id for row in page.items] == [succeeded.id]
        assert page.total == 1

    def test_another_orgs_runs_are_never_listed(self, store: Store):
        store.runs.create(_ORG_ID)

        page = store.runs.list(uuid4(), RunQuery())

        assert page.items == []
        assert page.total == 0

    def test_the_kind_filter_narrows_to_the_targets_kind(self, store: Store):
        job = store.runs.create(_ORG_ID, component_id=_component(store, kind="job"))
        store.runs.create(_ORG_ID, component_id=_component(store, kind="source"))

        page = store.runs.list(_ORG_ID, RunQuery(component_kind="job"))

        assert [row.id for row in page.items] == [job.id]
        assert page.total == 1

    def test_the_key_filter_narrows_to_the_targets_type(self, store: Store):
        facebook = store.runs.create(_ORG_ID, component_id=_component(store, kind="source", key="facebook_ads"))
        store.runs.create(_ORG_ID, component_id=_component(store, kind="source", key="google_ads"))

        page = store.runs.list(_ORG_ID, RunQuery(component_key="facebook_ads"))

        assert [row.id for row in page.items] == [facebook.id]
        assert page.total == 1

    def test_the_search_matches_the_targets_name_or_key_case_insensitively(self, store: Store):
        named = _component(store, kind="source", key="s1", name="Swarovski FB")
        keyed = _component(store, kind="job", key="swarovski_daily")
        by_name = store.runs.create(_ORG_ID, component_id=named)
        by_key = store.runs.create(_ORG_ID, component_id=keyed)
        store.runs.create(_ORG_ID, component_id=_component(store, kind="source", key="s2", name="Other"))
        store.runs.create(_ORG_ID)

        page = store.runs.list(_ORG_ID, RunQuery(q="SWARO"))

        assert {row.id for row in page.items} == {by_name.id, by_key.id}
        assert page.total == 2


class TestLatestByTarget:
    """One run per target: its most recently created attempt, whatever its stack."""

    @staticmethod
    def _run(
        store: Store,
        component_id: UUID | None,
        *,
        status: str,
        created: dt.datetime,
        root: UUID | None = None,
        attempt: int = 1,
        org_id: UUID = _ORG_ID,
        partition_key: str | None = None,
    ) -> Run:
        """Insert a run with a known creation time, as the first attempt of a stack or a later one.

        Args:
            store: The store whose database the run is written to.
            component_id: The run's target; ``None`` for a run whose target was deleted.
            status: The run's status.
            created: The run's creation instant.
            root: The stack's root run; ``None`` starts a new stack.
            attempt: The run's attempt number within its stack.
            org_id: The organisation the run belongs to.
            partition_key: The run's partition; ``None`` for an unpartitioned run.

        Returns:
            The inserted run.
        """
        run = Run(
            org_id=org_id,
            component_id=component_id,
            status=status,
            attempt=attempt,
            created_at=created,
            partition_key=partition_key,
        )
        if root is not None:
            run.root_run_id = root
        with Session(store.engine) as session:
            session.add(run)
            session.commit()
            session.refresh(run)
        return run

    def test_the_most_recently_created_attempt_wins(self, store: Store) -> None:
        job = _component(store, kind="job", key="cron_job")
        t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        self._run(store, job, status="success", created=t0)
        first = self._run(store, job, status="failed", created=t0 + _H(1))
        retry = self._run(store, job, status="success", created=t0 + _H(2), root=first.id, attempt=2)

        rows = store.runs.latest_by_target(_ORG_ID)

        assert [(row.id, row.status) for row in rows] == [(retry.id, "success")]

    def test_an_interleaved_retry_is_the_most_recent_attempt(self, store: Store) -> None:
        job = _component(store, kind="job", key="cron_job")
        t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        first = self._run(store, job, status="failed", created=t0)
        self._run(store, job, status="success", created=t0 + _H(1))
        retry = self._run(store, job, status="failed", created=t0 + _H(2), root=first.id, attempt=2)

        rows = store.runs.latest_by_target(_ORG_ID)

        assert [(row.id, row.status) for row in rows] == [(retry.id, "failed")]

    def test_kind_filter_and_org_scoping(self, store: Store) -> None:
        job = _component(store, kind="job", key="cron_job")
        source = _component(store, kind="source", key="demo")
        t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        self._run(store, job, status="failed", created=t0)
        self._run(store, source, status="success", created=t0)
        self._run(store, job, status="failed", created=t0 + _H(1), org_id=uuid4())

        jobs_only = store.runs.latest_by_target(_ORG_ID, component_kind="job")

        assert [(row.component_id, row.status) for row in jobs_only] == [(job, "failed")]
        assert {row.org_id for row in store.runs.latest_by_target(_ORG_ID)} == {_ORG_ID}
        assert store.runs.latest_by_target(uuid4()) == []

    def test_a_creation_tie_goes_to_the_later_partition(self, store: Store) -> None:
        job = _component(store, kind="job", key="cron_job")
        created = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        later = self._run(store, job, status="failed", created=created, partition_key="2026-01-02")
        self._run(store, job, status="success", created=created, partition_key="2026-01-01")
        self._run(store, job, status="success", created=created)

        rows = store.runs.latest_by_target(_ORG_ID)

        assert [row.id for row in rows] == [later.id]

    def test_a_deleted_target_is_left_out(self, store: Store) -> None:
        self._run(store, None, status="failed", created=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc))

        assert store.runs.latest_by_target(_ORG_ID) == []


@pytest.fixture(scope="module")
def postgres_db() -> Iterator[Engine]:
    """A throwaway Postgres database migrated to head.

    Yields:
        The engine bound to that database, dropped once the module finishes.
    """
    server_dsn = os.getenv("INTERLOPER_TEST_POSTGRES_DSN")
    if not server_dsn:
        pytest.skip("INTERLOPER_TEST_POSTGRES_DSN not set")
    dsn = urlunparse(urlparse(server_dsn)._replace(path=f"/interloper_test_{uuid4().hex[:8]}"))
    provision.ensure_database(dsn)
    engine = engine_module.init_engine(dsn)
    try:
        provision.create_all(engine)
        yield engine
    finally:
        engine.dispose()
        engine_module._engine = None
        provision.drop_database(dsn)


@pytest.fixture
def postgres_store(postgres_db: Engine) -> Store:
    """A store over the throwaway Postgres database.

    Returns:
        A store with an empty catalog, reading and writing that database.
    """
    return Store(catalog=il.Catalog(components={}), engine=postgres_db)


def _contend(store: Store, first: Callable[[], object], second: Callable[[], object]) -> BaseException | None:
    """Race *second* against *first* while *first*'s transaction is still open.

    *first* runs inside a transaction held open until *second* has had time
    to block on whatever *first* locked; the transaction then commits and
    *second* is let through.

    Args:
        store: The store both calls write through.
        first: The call that wins the race.
        second: The call that contends with it, on its own connection.

    Returns:
        What *second* raised, or ``None`` when it returned.

    Raises:
        AssertionError: If *second* finished before *first* committed, which
            means nothing made it wait.
    """
    first_holds = threading.Event()
    release = threading.Event()
    outcome: dict[str, BaseException | None] = {}

    def hold_first() -> None:
        with store.transaction():
            first()
            first_holds.set()
            release.wait(10)

    def run_second() -> None:
        try:
            second()
            outcome["error"] = None
        except BaseException as error:  # noqa: BLE001
            outcome["error"] = error

    holder = threading.Thread(target=hold_first)
    holder.start()
    assert first_holds.wait(10)
    contender = threading.Thread(target=run_second)
    contender.start()
    contender.join(0.5)
    blocked = contender.is_alive()
    release.set()
    holder.join(10)
    contender.join(10)
    if not blocked:
        raise AssertionError("the contending call did not wait for the first to commit")
    return outcome["error"]


def _attempts(store: Store, root_run_id: UUID) -> list[int]:
    with Session(store.engine) as session:
        runs = session.exec(select(Run).where(Run.root_run_id == root_run_id)).all()
    return sorted(run.attempt for run in runs)


@pytest.mark.integration
class TestConcurrentStackWrites:
    """Two writers racing on one stack leave it a linear chain of attempts."""

    def test_a_concurrent_retry_of_the_same_head_is_refused(self, postgres_store: Store) -> None:
        store = postgres_store
        first = store.runs.create(_ORG_ID)
        store.runs.complete(first.id, success=False)

        error = _contend(store, lambda: store.runs.retry(first.id), lambda: store.runs.retry(first.id))

        assert isinstance(error, ValueError)
        assert "stack was retried concurrently" in str(error)
        assert _attempts(store, first.root_run_id) == [1, 2]

    def test_a_concurrent_completion_queues_one_successor(self, postgres_store: Store) -> None:
        store = postgres_store
        target = _job_with_retry(store, max_attempts=3, delay=60)
        run = store.runs.create(_ORG_ID, component_id=target)

        error = _contend(
            store,
            lambda: store.runs.complete(run.id, success=False),
            lambda: store.runs.complete(run.id, success=False),
        )

        assert isinstance(error, ValueError)
        assert f"Run {run.id} is already failed" in str(error)
        assert _attempts(store, run.root_run_id) == [1, 2]
