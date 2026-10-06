"""Tests for migration ``012_executions_table``: executions folded from events by a trigger.

Postgres-only, like the other migration tests: this module reads a server DSN
from ``INTERLOPER_TEST_POSTGRES_DSN``, provisions a throwaway database
migrated to head, and drops it afterwards; without the variable it skips.

The oracle is the view the table replaces, migration 008's ``executions``
definition, run over the same events: the fold must agree with it whatever
order the events arrive in and however often one is delivered.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import os
import random
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Connection, Engine, text

import interloper_db.migrations
from interloper_db import engine as engine_module
from interloper_db import provision

pytestmark = pytest.mark.integration

_MIGRATION = Path(interloper_db.migrations.__path__[0]) / "versions" / "012_executions_table.py"
_spec = importlib.util.spec_from_file_location("migration_012", _MIGRATION)
assert _spec is not None and _spec.loader is not None
migration_012 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration_012)

_VIEW_QUERY = migration_012._VIEW.removeprefix("CREATE VIEW executions AS")
_COLUMNS = "run_id, component_id, org_id, component_key, status, attempts, started_at, completed_at, created_at"
_T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


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


def _run(connection: Connection, org_id: UUID) -> UUID:
    """Insert a run for the events to belong to.

    Args:
        connection: The connection the run is inserted on.
        org_id: The organisation owning the run.

    Returns:
        The run's id.
    """
    run_id = uuid4()
    connection.execute(
        text(
            "INSERT INTO runs (id, org_id, status, attempt, root_run_id, billable) "
            "VALUES (:id, :org_id, 'running', 1, :id, true)"
        ),
        {"id": run_id, "org_id": org_id},
    )
    return run_id


def _lifecycle(rng: random.Random) -> list[tuple[str, int]]:
    """One operation's events as a runner emits them: ``(event_type, attempt)`` in emission order.

    Args:
        rng: The source of the lifecycle's shape.

    Returns:
        The events, from the queued one to the last, which may be no verdict at all.
    """
    events = [("operation_queued", 1)]
    if rng.random() < 0.1:
        return [*events, (rng.choice(["operation_canceled", "operation_skipped"]), 1)]
    attempts = rng.choice([1, 1, 1, 2, 3])
    for attempt in range(1, attempts + 1):
        events.append(("operation_started", attempt))
        if attempt < attempts:
            events.append(("operation_retried", attempt))
    ending = rng.choice(["operation_completed", "operation_failed", "operation_canceled", None])
    return [*events, (ending, attempts)] if ending else events


def _mix(rng: random.Random) -> list[tuple[str, int]]:
    """Any mix of operation events over a few attempts, in no particular order.

    Every rule of the fold gets exercised, including the ones a runner's own
    sequences never reach (two verdicts in one attempt, a verdict before its
    start). The one combination left out is ``operation_retried`` with
    ``operation_skipped`` in the same attempt, which no runner produces and
    where the table, which keeps the status and not the event, reads
    ``running`` as ``operation_started``.

    Args:
        rng: The source of the mix.

    Returns:
        The events as ``(event_type, attempt)`` pairs.
    """
    kinds = [
        "operation_queued",
        "operation_started",
        "operation_retried",
        "operation_skipped",
        "operation_completed",
        "operation_failed",
        "operation_canceled",
    ]
    events: list[tuple[str, int]] = []
    for attempt in range(1, rng.randint(1, 3) + 1):
        chosen = rng.sample(kinds, rng.randint(1, 4))
        if "operation_retried" in chosen and "operation_skipped" in chosen:
            chosen.remove("operation_skipped")
        events += [(kind, attempt) for kind in chosen for _ in range(rng.choice([1, 1, 2]))]
    return events


def _insert(connection: Connection, rows: list[dict[str, Any]]) -> None:
    """Insert events one statement each, as ``EventStore.save`` does, skipping a repeated id.

    Args:
        connection: The connection the events are inserted on.
        rows: The events' column values.
    """
    for row in rows:
        connection.execute(
            text(
                "INSERT INTO events (id, org_id, run_id, event_type, component_id, component_key, data, timestamp) "
                "VALUES (:id, :org_id, :run_id, :event_type, :component_id, :component_key, "
                "CAST(:data AS jsonb), :timestamp) ON CONFLICT (id) DO NOTHING"
            ),
            row,
        )


def _table(connection: Connection, run_ids: list[UUID]) -> set[tuple[Any, ...]]:
    """The executions the trigger folded for these runs.

    Args:
        connection: The connection to read on.
        run_ids: The runs to read.

    Returns:
        One tuple per execution.
    """
    statement = text(f"SELECT {_COLUMNS} FROM executions WHERE run_id = ANY(:run_ids)")
    return {tuple(row) for row in connection.execute(statement, {"run_ids": run_ids})}


def _view(connection: Connection, run_ids: list[UUID]) -> set[tuple[Any, ...]]:
    """The executions migration 008's view computes from the same events.

    Args:
        connection: The connection to read on.
        run_ids: The runs to read.

    Returns:
        One tuple per execution.
    """
    statement = text(f"SELECT {_COLUMNS} FROM ({_VIEW_QUERY}) AS view WHERE run_id = ANY(:run_ids)")
    return {tuple(row) for row in connection.execute(statement, {"run_ids": run_ids})}


class TestFold:
    @pytest.mark.parametrize(("seed", "events"), [(seed, events) for seed in range(5) for events in (_lifecycle, _mix)])
    def test_shuffled_and_repeated_events_fold_to_the_views_rows(
        self, postgres_db: Engine, seed: int, events: Any
    ) -> None:
        rng = random.Random(seed)
        org_id = uuid4()
        with postgres_db.begin() as connection:
            run_ids = [_run(connection, org_id) for _ in range(20)]
            rows: list[dict[str, Any]] = []
            for run_id in run_ids:
                for component in range(rng.randint(1, 4)):
                    component_id = uuid4()
                    for step, (event_type, attempt) in enumerate(events(rng)):
                        rows.append(
                            {
                                "id": uuid4(),
                                "org_id": org_id,
                                "run_id": run_id,
                                "event_type": event_type,
                                "component_id": component_id,
                                "component_key": f"asset_{component}",
                                "data": f'{{"attempt": {attempt}}}',
                                "timestamp": _T0 + dt.timedelta(seconds=step * rng.choice([0, 1, 1, 2])),
                            }
                        )
            delivered = rows + rng.sample(rows, len(rows) // 5)
            rng.shuffle(delivered)
            _insert(connection, delivered)

        with postgres_db.connect() as connection:
            folded, expected = _table(connection, run_ids), _view(connection, run_ids)

        assert folded == expected
        assert len(folded) == len({(row["run_id"], row["component_id"]) for row in rows})

    def test_events_without_a_run_or_a_component_fold_nothing(self, postgres_db: Engine) -> None:
        org_id = uuid4()
        with postgres_db.begin() as connection:
            run_id = _run(connection, org_id)
            base = {"org_id": org_id, "component_key": "orders", "data": "{}", "timestamp": _T0}
            _insert(
                connection,
                [
                    {**base, "id": uuid4(), "run_id": run_id, "event_type": "operation_started", "component_id": None},
                    {**base, "id": uuid4(), "run_id": None, "event_type": "operation_started", "component_id": uuid4()},
                    {**base, "id": uuid4(), "run_id": run_id, "event_type": "run_started", "component_id": uuid4()},
                ],
            )

        with postgres_db.connect() as connection:
            count = connection.execute(text("SELECT count(*) FROM executions WHERE org_id = :o"), {"o": org_id})

        assert count.scalar_one() == 0


class TestUpgrade:
    def test_the_backfill_carries_the_views_rows_over(self, postgres_db: Engine) -> None:
        rng = random.Random(42)
        org_id = uuid4()
        provision.downgrade(postgres_db, revision="011")
        try:
            with postgres_db.begin() as connection:
                run_ids = [_run(connection, org_id) for _ in range(10)]
                rows = [
                    {
                        "id": uuid4(),
                        "org_id": org_id,
                        "run_id": run_id,
                        "event_type": event_type,
                        "component_id": component_id,
                        "component_key": "orders",
                        "data": f'{{"attempt": {attempt}}}',
                        "timestamp": _T0 + dt.timedelta(seconds=step),
                    }
                    for run_id in run_ids
                    for component_id in [uuid4()]
                    for step, (event_type, attempt) in enumerate(_lifecycle(rng))
                ]
                _insert(connection, rows)
            with postgres_db.connect() as connection:
                before = {
                    tuple(row)
                    for row in connection.execute(
                        text(f"SELECT {_COLUMNS} FROM executions WHERE run_id = ANY(:run_ids)"), {"run_ids": run_ids}
                    )
                }
        finally:
            provision.upgrade(postgres_db)

        with postgres_db.connect() as connection:
            assert _table(connection, run_ids) == before
            index = connection.execute(text("SELECT to_regclass('ix_events_executions')")).scalar()
        assert index is None


class TestDowngrade:
    def test_it_restores_the_view_and_its_index(self, postgres_db: Engine) -> None:
        provision.downgrade(postgres_db, revision="011")
        try:
            with postgres_db.connect() as connection:
                kind = connection.execute(text("SELECT relkind FROM pg_class WHERE relname = 'executions'")).scalar()
                index = connection.execute(text("SELECT to_regclass('ix_events_executions')")).scalar()
                trigger = connection.execute(
                    text("SELECT count(*) FROM pg_trigger WHERE tgname = 'trg_events_fold_execution'")
                ).scalar()
        finally:
            provision.upgrade(postgres_db)

        assert (kind, index is not None, trigger) == ("v", True, 0)
