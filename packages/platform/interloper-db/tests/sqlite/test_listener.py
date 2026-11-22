"""Tests for ``interloper_db.sqlite.listener``: a SQLite database's changes, handed out as Postgres sends them."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db.models import Component, Event, Run
from interloper_db.provision import bootstrap
from interloper_db.sqlite import SQLiteListener

_ORG = UUID("651f5df0-28a9-4570-9391-7764ea9e739f")
_T0 = datetime(2026, 11, 22, 10, 0, 1, 250000, tzinfo=UTC)


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
def received(database: Engine) -> list[dict[str, Any]]:
    """What a listener on the database is handed, in order.

    Args:
        database: The database listened to.

    Returns:
        The changes, appended as their transactions commit.
    """
    changes: list[dict[str, Any]] = []
    SQLiteListener(database, changes.append)
    return changes


def _run(engine: Engine, run_id: UUID | None = None, *, scheduled_for: datetime | None = None) -> UUID:
    run_id = run_id or uuid4()
    with Session(engine) as session:
        session.add(Run(id=run_id, org_id=_ORG, root_run_id=run_id, scheduled_for=scheduled_for))
        session.commit()
    return run_id


class TestCommit:
    """A transaction's changes are handed out as it commits, never before, never when it rolls back."""

    def test_changes_are_handed_out_as_their_transaction_commits_in_order(
        self, database: Engine, received: list[dict[str, Any]]
    ) -> None:
        first, second = uuid4(), uuid4()
        with Session(database) as session:
            session.add(Run(id=first, org_id=_ORG, root_run_id=first))
            session.flush()
            session.add(Run(id=second, org_id=_ORG, root_run_id=second))
            session.flush()

            assert received == []

            session.commit()

        assert [change["record"]["id"] for change in received] == [str(first), str(second)]

    def test_a_rolled_back_change_is_never_handed_out(self, database: Engine, received: list[dict[str, Any]]) -> None:
        with Session(database) as session:
            rolled_back = uuid4()
            session.add(Run(id=rolled_back, org_id=_ORG, root_run_id=rolled_back))
            session.flush()
            session.rollback()
        kept = _run(database)

        assert [change["record"]["id"] for change in received] == [str(kept)]

    def test_a_failing_callback_is_logged_and_the_change_still_commits(
        self, database: Engine, caplog: pytest.LogCaptureFixture
    ) -> None:
        def fail(change: dict[str, Any]) -> None:
            raise RuntimeError("socket gone")

        SQLiteListener(database, fail)

        with caplog.at_level("ERROR", logger="interloper_db.sqlite.listener"):
            run_id = _run(database)

        with Session(database) as session:
            assert session.get(Run, run_id) is not None
        assert "could not be handed out" in caplog.text


class TestDecode:
    """Values come back as Postgres renders them in its notification's JSON."""

    def test_ids_and_the_organisation_are_dashed(self, database: Engine, received: list[dict[str, Any]]) -> None:
        run_id = _run(database, UUID("29901a97-aa5c-49af-831f-fbf647cc3ce2"))

        (change,) = received

        assert change["org_id"] == "651f5df0-28a9-4570-9391-7764ea9e739f"
        assert change["record"]["id"] == change["record"]["root_run_id"] == str(run_id)

    def test_timestamps_are_iso_with_their_offset(self, database: Engine, received: list[dict[str, Any]]) -> None:
        _run(database, scheduled_for=_T0)

        (change,) = received

        assert change["record"]["scheduled_for"] == "2026-11-22T10:00:01.250000+00:00"
        assert datetime.fromisoformat(change["record"]["created_at"]).tzinfo is UTC

    def test_booleans_are_booleans_and_nulls_stay_null(self, database: Engine, received: list[dict[str, Any]]) -> None:
        _run(database)

        (change,) = received

        assert (change["record"]["retried"], change["record"]["billable"]) == (False, True)
        assert change["record"]["started_at"] is None

    def test_a_json_column_stays_nested(self, database: Engine, received: list[dict[str, Any]]) -> None:
        with Session(database) as session:
            session.add(Event(org_id=_ORG, event_type="run_started", data={"attempt": 2, "tags": ["a"]}, timestamp=_T0))
            session.commit()

        (change,) = received

        assert change["record"]["data"] == {"attempt": 2, "tags": ["a"]}

    def test_what_is_not_a_column_passes_through(self, database: Engine, received: list[dict[str, Any]]) -> None:
        component_id = uuid4()
        with Session(database) as session:
            session.add(Component(id=component_id, org_id=_ORG, kind="source", key="demo", name="Demo"))
            session.commit()
        received.clear()
        run_id = uuid4()
        with Session(database) as session:
            session.add(Run(id=run_id, org_id=_ORG, component_id=component_id, root_run_id=run_id))
            session.commit()

        (change,) = received

        assert change["record"]["component_id"] == str(component_id)
        assert change["record"]["component_key"] == "demo"
