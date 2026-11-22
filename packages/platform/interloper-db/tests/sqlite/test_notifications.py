"""Tests for ``interloper_db.sqlite.notifications``: which changes a SQLite database notifies, and with what.

Each test changes rows as the platform does and reads what a listener was
handed as each transaction committed.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, delete, update
from sqlmodel import Session, col

from interloper_db import engine as engine_module
from interloper_db.models import Component, ComponentRelation, Event, Execution, Run
from interloper_db.provision import bootstrap
from interloper_db.sqlite import SQLiteListener, SQLiteNotifications

_ORG = uuid4()
_T0 = datetime(2026, 11, 22, 10, tzinfo=UTC)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Engine]:
    """A SQLite database brought up as ``interloper dev`` brings one up.

    Args:
        tmp_path: Pytest's temporary directory, which holds the file.

    Yields:
        The engine bound to it, disposed once the test finishes.
    """
    engine = bootstrap(f"sqlite:///{tmp_path}/interloper.db")
    try:
        yield engine
    finally:
        engine.dispose()
        engine_module._engine = None


@pytest.fixture
def notified(database: Engine) -> list[dict[str, Any]]:
    """What a listener on the database is handed, in order.

    Args:
        database: The database listened to.

    Returns:
        The changes, appended as their transactions commit.
    """
    received: list[dict[str, Any]] = []
    SQLiteListener(database, received.append)
    return received


def _add(engine: Engine, *rows: Any) -> None:
    with Session(engine) as session:
        session.add_all(rows)
        session.commit()


def _source(engine: Engine) -> UUID:
    component_id = uuid4()
    _add(engine, Component(id=component_id, org_id=_ORG, kind="source", key="demo", name="Demo"))
    return component_id


def _run(engine: Engine, component_id: UUID | None = None) -> UUID:
    run_id = uuid4()
    _add(engine, Run(id=run_id, org_id=_ORG, component_id=component_id, root_run_id=run_id))
    return run_id


class TestTriggers:
    """One trigger per notifying table and operation."""

    def test_every_notifying_trigger_is_created(self, database: Engine) -> None:
        with database.connect() as connection:
            triggers = set(
                connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type = 'trigger'").scalars()
            )

        assert set(SQLiteNotifications().triggers()) <= triggers

    def test_each_notification_is_one_trigger_per_operation(self) -> None:
        notifications = SQLiteNotifications()

        expected = {
            f"trg_{table}_notify_{operation.lower()}"
            for table, notification in notifications.notifications().items()
            for operation in notification.operations
        }

        assert set(notifications.triggers()) == expected


class TestNotifications:
    """What each change notifies, as 0002 notifies it on Postgres."""

    def test_a_new_run_is_notified_with_its_component(self, database: Engine, notified: list[dict[str, Any]]) -> None:
        run_id = _run(database, _source(database))

        (change,) = [change for change in notified if change["table"] == "runs"]

        assert (change["op"], UUID(hex=change["org_id"])) == ("INSERT", _ORG)
        assert UUID(hex=change["record"]["id"]) == run_id
        assert (change["record"]["component_kind"], change["record"]["component_key"]) == ("source", "demo")
        assert change["record"]["component_name"] == "Demo"

    def test_a_run_beat_alone_is_not_notified(self, database: Engine, notified: list[dict[str, Any]]) -> None:
        run_id = _run(database)
        before = len(notified)
        with Session(database) as session:
            session.connection().execute(update(Run).where(col(Run.id) == run_id).values(heartbeat_at=_T0))
            session.commit()

        assert len(notified) == before

    def test_a_status_change_is_notified_even_with_a_beat(
        self, database: Engine, notified: list[dict[str, Any]]
    ) -> None:
        run_id = _run(database)
        with Session(database) as session:
            session.connection().execute(
                update(Run).where(col(Run.id) == run_id).values(status="running", heartbeat_at=_T0)
            )
            session.commit()

        assert notified[-1]["op"] == "UPDATE"
        assert notified[-1]["record"]["status"] == "running"

    def test_an_execution_is_notified_as_made_and_as_changed(
        self, database: Engine, notified: list[dict[str, Any]]
    ) -> None:
        run_id = _run(database)
        component_id = uuid4()
        for minute, event_type in enumerate(("operation_started", "operation_completed")):
            _add(
                database,
                Event(
                    org_id=_ORG,
                    run_id=run_id,
                    component_id=component_id,
                    event_type=event_type,
                    timestamp=_T0 + timedelta(minutes=minute),
                ),
            )

        operations = [change["op"] for change in notified if change["table"] == "executions"]

        assert operations == ["INSERT", "UPDATE"]

    def test_an_execution_update_that_changes_nothing_is_not_notified(
        self, database: Engine, notified: list[dict[str, Any]]
    ) -> None:
        run_id = _run(database)
        component_id = uuid4()
        _add(
            database,
            Event(org_id=_ORG, run_id=run_id, component_id=component_id, event_type="operation_started", timestamp=_T0),
        )
        before = len(notified)
        with Session(database) as session:
            session.connection().execute(
                update(Execution).where(col(Execution.run_id) == run_id).values(status="running")
            )
            session.commit()

        assert len(notified) == before

    def test_an_events_record_nests_its_data(self, database: Engine, notified: list[dict[str, Any]]) -> None:
        _add(database, Event(org_id=_ORG, event_type="run_started", data={"attempt": 2}, timestamp=_T0))

        (change,) = [change for change in notified if change["table"] == "events"]

        assert change["record"]["data"] == {"attempt": 2}

    def test_a_component_carries_only_its_id_kind_and_parent(
        self, database: Engine, notified: list[dict[str, Any]]
    ) -> None:
        _source(database)

        (change,) = [change for change in notified if change["table"] == "components"]

        assert set(change["record"]) == {"id", "kind", "parent_id"}

    def test_a_deletion_is_notified_from_the_row_it_removed(
        self, database: Engine, notified: list[dict[str, Any]]
    ) -> None:
        source, other = _source(database), _source(database)
        _add(
            database,
            ComponentRelation(
                src_id=source, name="upstream", dst_id=other, org_id=_ORG, src_kind="source", dst_kind="source"
            ),
        )
        with Session(database) as session:
            session.connection().execute(delete(ComponentRelation))
            session.commit()

        change = notified[-1]

        assert (change["table"], change["op"]) == ("component_relations", "DELETE")
        assert UUID(hex=change["record"]["src_id"]) == source

    def test_a_rolled_back_change_is_not_notified(self, database: Engine, notified: list[dict[str, Any]]) -> None:
        with Session(database) as session:
            run_id = uuid4()
            session.add(Run(id=run_id, org_id=_ORG, root_run_id=run_id))
            session.flush()
            session.rollback()

        assert notified == []
