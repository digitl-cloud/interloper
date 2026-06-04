"""Tests for event pagination in ``RunMixin`` (``list_events`` / ``count_events``).

These run against an in-memory SQLite database so the offset/limit/ordering
behaviour is exercised end-to-end against real SQL, not a stubbed query.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db.models import Event
from interloper_db.store.runs import RunMixin

_RUN_ID = UUID("99c018d6-98fe-4de5-a867-1f1a9a545a38")
_OTHER_RUN_ID = uuid4()
_ORG_ID = uuid4()
_BASE_TS = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def store() -> Iterator[RunMixin]:
    """A RunMixin wired to a fresh in-memory SQLite database."""
    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    # Only the events table is exercised here; creating the full schema would
    # pull in Postgres-only column types (e.g. ARRAY) that SQLite can't render.
    Event.__table__.create(eng)  # type: ignore[attr-defined]
    try:
        yield RunMixin()
    finally:
        eng.dispose()
        engine_module._engine = None


def _seed(events: list[Event]) -> None:
    with Session(engine_module.get_engine()) as session:
        session.add_all(events)
        session.commit()


def _make_events(n: int, *, run_id: UUID = _RUN_ID, start: int = 0) -> list[Event]:
    """Build ``n`` events for a run, one second apart, oldest first."""
    return [
        Event(
            id=uuid4(),
            org_id=_ORG_ID,
            run_id=run_id,
            event_type="asset_materializing" if i < n - 1 else "asset_completed",
            timestamp=_BASE_TS + timedelta(seconds=start + i),
        )
        for i in range(n)
    ]


def test_list_events_defaults_to_oldest_first(store: RunMixin) -> None:
    _seed(_make_events(3))
    events = store.list_events(run_id=_RUN_ID)
    timestamps = [e.timestamp for e in events]
    assert timestamps == sorted(timestamps)


def test_offset_and_limit_page_without_gaps_or_repeats(store: RunMixin) -> None:
    _seed(_make_events(250))

    page1 = store.list_events(run_id=_RUN_ID, limit=100, offset=0)
    page2 = store.list_events(run_id=_RUN_ID, limit=100, offset=100)
    page3 = store.list_events(run_id=_RUN_ID, limit=100, offset=200)

    assert [len(page1), len(page2), len(page3)] == [100, 100, 50]

    ids = [e.id for p in (page1, page2, page3) for e in p]
    assert len(ids) == 250
    assert len(set(ids)) == 250  # no row repeated across pages


def test_terminal_event_is_reachable_via_offset(store: RunMixin) -> None:
    # The outcome event sorts last; the default first page hides it, but
    # paging to the tail must surface it.
    _seed(_make_events(150))

    first_page = store.list_events(run_id=_RUN_ID, limit=100, offset=0)
    assert all(e.event_type != "asset_completed" for e in first_page)

    last_page = store.list_events(run_id=_RUN_ID, limit=100, offset=100)
    assert last_page[-1].event_type == "asset_completed"


def test_ordering_is_stable_for_equal_timestamps(store: RunMixin) -> None:
    # All events share a timestamp; paging must still be deterministic
    # (tie-broken by id) so no row is skipped or repeated.
    shared = [
        Event(
            id=uuid4(),
            org_id=_ORG_ID,
            run_id=_RUN_ID,
            event_type="asset_materializing",
            timestamp=_BASE_TS,
        )
        for _ in range(20)
    ]
    _seed(shared)

    page1 = store.list_events(run_id=_RUN_ID, limit=10, offset=0)
    page2 = store.list_events(run_id=_RUN_ID, limit=10, offset=10)
    ids = [e.id for e in page1 + page2]
    assert len(set(ids)) == 20


def test_count_events_ignores_limit_and_offset(store: RunMixin) -> None:
    _seed(_make_events(777))
    assert store.count_events(run_id=_RUN_ID) == 777
    # A capped page does not change the reported total.
    assert len(store.list_events(run_id=_RUN_ID, limit=100)) == 100


def test_filters_isolate_runs(store: RunMixin) -> None:
    _seed(_make_events(5, run_id=_RUN_ID))
    _seed(_make_events(3, run_id=_OTHER_RUN_ID))
    assert store.count_events(run_id=_RUN_ID) == 5
    assert store.count_events(run_id=_OTHER_RUN_ID) == 3
    assert len(store.list_events(run_id=_RUN_ID)) == 5
