"""Tests for migration ``004_run_attempts``: the attempt-aware ``executions`` view.

The view is Postgres SQL, so nothing but a live server exercises its ranking:
the SQLite suites stand the read model up as a table and test the mapping, not
the query. This module reads a server DSN from ``INTERLOPER_TEST_POSTGRES_DSN``,
provisions a throwaway database migrated to head, and drops it afterwards;
without the variable it skips.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Iterator
from urllib.parse import urlparse, urlunparse
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db import engine as engine_module
from interloper_db import provision
from interloper_db.models import Event, Execution, Run

pytestmark = pytest.mark.integration

_ORG_ID = uuid4()


@pytest.fixture(scope="module")
def postgres_db() -> Iterator[Engine]:
    """A throwaway Postgres database migrated to head.

    Yields:
        The global engine bound to that database, dropped once the module finishes.
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


def _emit(session: Session, run_id: UUID, component_id: UUID, event_type: str, attempt: int, second: int) -> None:
    """Append one operation-lifecycle event for a given attempt."""
    session.add(
        Event(
            id=uuid4(),
            org_id=_ORG_ID,
            run_id=run_id,
            event_type=event_type,
            component_id=component_id,
            component_key="orders",
            data={"attempt": attempt},
            timestamp=dt.datetime(2026, 1, 1, 12, 0, second, tzinfo=dt.timezone.utc),
        )
    )


def _execution(engine: Engine, run_id: UUID) -> Execution:
    with Session(engine) as session:
        return session.exec(select(Execution).where(Execution.run_id == run_id)).one()


def _run(session: Session) -> UUID:
    run = Run(org_id=_ORG_ID, status="running")
    session.add(run)
    session.flush()
    return run.id


class TestVerdictReadsTheLatestAttempt:
    def test_an_operation_that_healed_reads_as_a_success(self, postgres_db: Engine) -> None:
        component_id = uuid4()
        with Session(postgres_db) as session:
            run_id = _run(session)
            _emit(session, run_id, component_id, "operation_started", 1, 0)
            _emit(session, run_id, component_id, "operation_retried", 1, 1)
            _emit(session, run_id, component_id, "operation_started", 2, 2)
            _emit(session, run_id, component_id, "operation_completed", 2, 3)
            session.commit()

        execution = _execution(postgres_db, run_id)

        assert execution.status == "success"
        assert execution.attempts == 2

    def test_an_operation_that_exhausted_its_budget_reads_as_a_failure(self, postgres_db: Engine) -> None:
        component_id = uuid4()
        with Session(postgres_db) as session:
            run_id = _run(session)
            _emit(session, run_id, component_id, "operation_started", 1, 0)
            _emit(session, run_id, component_id, "operation_retried", 1, 1)
            _emit(session, run_id, component_id, "operation_started", 2, 2)
            _emit(session, run_id, component_id, "operation_failed", 2, 3)
            session.commit()

        execution = _execution(postgres_db, run_id)

        assert execution.status == "failed"
        assert execution.attempts == 2

    def test_a_single_attempt_is_unchanged(self, postgres_db: Engine) -> None:
        component_id = uuid4()
        with Session(postgres_db) as session:
            run_id = _run(session)
            _emit(session, run_id, component_id, "operation_queued", 1, 0)
            _emit(session, run_id, component_id, "operation_started", 1, 1)
            _emit(session, run_id, component_id, "operation_completed", 1, 2)
            session.commit()

        execution = _execution(postgres_db, run_id)

        assert (execution.status, execution.attempts) == ("success", 1)

    def test_events_written_before_attempts_existed_count_as_one(self, postgres_db: Engine) -> None:
        # Historical rows carry no `attempt` in their data; COALESCE reads them
        # as the first attempt rather than dropping them from the ranking.
        component_id = uuid4()
        with Session(postgres_db) as session:
            run_id = _run(session)
            session.add(
                Event(
                    id=uuid4(),
                    org_id=_ORG_ID,
                    run_id=run_id,
                    event_type="operation_completed",
                    component_id=component_id,
                    component_key="orders",
                    timestamp=dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc),
                )
            )
            session.commit()

        execution = _execution(postgres_db, run_id)

        assert (execution.status, execution.attempts) == ("success", 1)

    def test_a_retried_attempt_never_wins_the_verdict(self, postgres_db: Engine) -> None:
        # The newest event of the latest attempt is the retry itself, which
        # must read as still running rather than as the operation's outcome.
        component_id = uuid4()
        with Session(postgres_db) as session:
            run_id = _run(session)
            _emit(session, run_id, component_id, "operation_started", 1, 0)
            _emit(session, run_id, component_id, "operation_retried", 1, 1)
            session.commit()

        execution = _execution(postgres_db, run_id)

        assert execution.status == "running"
        assert execution.attempts == 1

    def test_the_timestamps_span_every_attempt(self, postgres_db: Engine) -> None:
        component_id = uuid4()
        with Session(postgres_db) as session:
            run_id = _run(session)
            _emit(session, run_id, component_id, "operation_started", 1, 0)
            _emit(session, run_id, component_id, "operation_retried", 1, 1)
            _emit(session, run_id, component_id, "operation_started", 2, 5)
            _emit(session, run_id, component_id, "operation_completed", 2, 9)
            session.commit()

        execution = _execution(postgres_db, run_id)

        assert execution.started_at == dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        assert execution.completed_at == dt.datetime(2026, 1, 1, 12, 0, 9, tzinfo=dt.timezone.utc)
