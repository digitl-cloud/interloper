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
from interloper_db.models import AssetExecution, Event
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
    Event.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        mixin = RunMixin()
        mixin._engine = eng
        yield mixin
    finally:
        eng.dispose()
        engine_module._engine = None


def _seed(events: list[Event]) -> None:
    with Session(engine_module.get_engine()) as session:
        session.add_all(events)
        session.commit()


def _make_events(n: int, *, run_id: UUID = _RUN_ID, start: int = 0, asset_id: UUID | None = None) -> list[Event]:
    """Build ``n`` events for a run, one second apart, oldest first."""
    return [
        Event(
            id=uuid4(),
            org_id=_ORG_ID,
            run_id=run_id,
            asset_id=asset_id,
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


def test_asset_filter_lists_and_counts_only_that_asset(store: RunMixin) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(150, asset_id=asset_a))
    _seed(_make_events(30, start=150, asset_id=asset_b))

    assert store.count_events(run_id=_RUN_ID, asset_ids=[asset_a]) == 150
    assert store.count_events(run_id=_RUN_ID, asset_ids=[asset_b]) == 30

    # Paging honours the filter: asset_a events past the first unfiltered
    # page are reachable through the filtered offsets.
    page2 = store.list_events(run_id=_RUN_ID, asset_ids=[asset_a], limit=100, offset=100)
    assert len(page2) == 50
    assert all(e.asset_id == asset_a for e in page2)

    # asset_b's events all live beyond the first 150 rows of the run, yet its
    # filtered first page surfaces them.
    page_b = store.list_events(run_id=_RUN_ID, asset_ids=[asset_b], limit=100, offset=0)
    assert len(page_b) == 30
    assert all(e.asset_id == asset_b for e in page_b)


def test_event_type_filter_lists_and_counts_only_those_types(store: RunMixin) -> None:
    # _make_events emits n-1 "asset_materializing" then one "asset_completed".
    _seed(_make_events(5))

    assert store.count_events(run_id=_RUN_ID, event_types=["asset_completed"]) == 1
    assert store.count_events(run_id=_RUN_ID, event_types=["asset_materializing"]) == 4
    # A set of types is the union of each.
    assert store.count_events(run_id=_RUN_ID, event_types=["asset_completed", "asset_materializing"]) == 5

    completed = store.list_events(run_id=_RUN_ID, event_types=["asset_completed"])
    assert len(completed) == 1
    assert all(e.event_type == "asset_completed" for e in completed)


def test_asset_and_event_type_filters_compose(store: RunMixin) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(5, asset_id=asset_a))
    _seed(_make_events(5, start=5, asset_id=asset_b))

    # Each asset has exactly one "asset_completed"; narrowing to asset_a's set
    # of one type yields just that asset's completion.
    assert store.count_events(run_id=_RUN_ID, asset_ids=[asset_a], event_types=["asset_completed"]) == 1
    page = store.list_events(run_id=_RUN_ID, asset_ids=[asset_a], event_types=["asset_completed"])
    assert len(page) == 1
    assert page[0].asset_id == asset_a
    assert page[0].event_type == "asset_completed"


def test_asset_filter_accepts_multiple_assets(store: RunMixin) -> None:
    asset_a, asset_b, asset_c = uuid4(), uuid4(), uuid4()
    _seed(_make_events(10, asset_id=asset_a))
    _seed(_make_events(5, start=10, asset_id=asset_b))
    _seed(_make_events(7, start=15, asset_id=asset_c))

    # A set of asset ids (e.g. every asset of one status) is the union of each.
    assert store.count_events(run_id=_RUN_ID, asset_ids=[asset_a, asset_b]) == 15
    page = store.list_events(run_id=_RUN_ID, asset_ids=[asset_a, asset_b], limit=100, offset=0)
    assert len(page) == 15
    assert all(e.asset_id in {asset_a, asset_b} for e in page)


# -- complete_run job stamping -------------------------------------------------


@pytest.fixture
def run_store() -> Iterator[RunMixin]:
    """A RunMixin over a database with runs and components tables."""
    from interloper_db.models import Component, Run

    eng = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Component.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    Run.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    try:
        mixin = RunMixin()
        mixin._engine = eng
        yield mixin
    finally:
        eng.dispose()
        engine_module._engine = None


def test_complete_run_stamps_the_jobs_last_run_at(run_store: RunMixin) -> None:
    from interloper_db.models import Component, Run

    org = uuid4()
    with Session(engine_module.get_engine()) as session:
        job = Component(org_id=org, kind="job", key="cron_job", name="J")
        session.add(job)
        session.flush()
        run = Run(id=uuid4(), org_id=org, component_id=job.id, status="running")
        session.add(run)
        session.commit()
        component_id, run_id = job.id, run.id

    completed = run_store.complete_run(run_id, success=True)
    assert completed.status == "success"
    assert completed.completed_at is not None

    with Session(engine_module.get_engine()) as session:
        stamped = session.get(Component, component_id)
        assert stamped is not None and stamped.state is not None
        # SQLite round-trips the column naive; the stamped ISO string is aware UTC.
        stamped_at = datetime.fromisoformat(stamped.state["last_run_at"])
        assert stamped_at == completed.completed_at.replace(tzinfo=timezone.utc)


def test_asset_executions_read_model_maps_the_view(store: RunMixin) -> None:
    """The typed read model round-trips rows shaped like the view's output.

    SQLite stands in: the model's table definition doubles as the view's
    schema, so creating it as a table exercises the exact mapping the view
    serves in production.
    """
    eng = engine_module.get_engine()
    AssetExecution.__table__.create(eng)  # ty: ignore[unresolved-attribute]
    run_id, asset_id, org = uuid4(), uuid4(), uuid4()
    with Session(eng) as session:
        session.add(
            AssetExecution(
                run_id=run_id,
                asset_id=asset_id,
                org_id=org,
                asset_key="a",
                status="success",
                completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        )
        session.commit()

    rows = store.list_asset_executions(run_id)
    assert [(row.asset_key, row.status) for row in rows] == [("a", "success")]
    assert store.list_asset_executions(uuid4()) == []
