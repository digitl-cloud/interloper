"""Tests for ``interloper_db.store.events``."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import NotFoundError
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db.models import Event
from interloper_db.store import EventQuery, Page, Store

_RUN_ID = UUID("99c018d6-98fe-4de5-a867-1f1a9a545a38")
_OTHER_RUN_ID = uuid4()
_ORG_ID = uuid4()
_BASE_TS = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)


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
    # Only the events table is exercised here; creating the full schema would
    # pull in Postgres-only column types (e.g. ARRAY) that SQLite can't render.
    Event.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    try:
        yield Store(catalog=il.Catalog(components={}), engine=engine)
    finally:
        engine.dispose()
        engine_module._engine = None


def _seed(events: list[Event]) -> None:
    with Session(engine_module.get_engine()) as session:
        session.add_all(events)
        session.commit()


def _make_events(
    n: int,
    *,
    run_id: UUID = _RUN_ID,
    start: int = 0,
    component_id: UUID | None = None,
    org_id: UUID = _ORG_ID,
) -> list[Event]:
    """Build ``n`` events for a run, one second apart, oldest first.

    Returns:
        The events in chronological order, the last one an ``asset_completed``.
    """
    return [
        Event(
            id=uuid4(),
            org_id=org_id,
            run_id=run_id,
            component_id=component_id,
            event_type="asset_materializing" if i < n - 1 else "asset_completed",
            timestamp=_BASE_TS + timedelta(seconds=start + i),
        )
        for i in range(n)
    ]


def test_list_defaults_to_oldest_first(store: Store) -> None:
    _seed(_make_events(3))
    events = store.events.list(_ORG_ID, EventQuery(limit=100), run_id=_RUN_ID).items
    timestamps = [e.timestamp for e in events]
    assert timestamps == sorted(timestamps)


def test_offset_and_limit_page_without_gaps_or_repeats(store: Store) -> None:
    _seed(_make_events(250))

    pages = [
        store.events.list(_ORG_ID, EventQuery(limit=100, offset=offset), run_id=_RUN_ID) for offset in (0, 100, 200)
    ]

    assert [len(page.items) for page in pages] == [100, 100, 50]
    assert {page.total for page in pages} == {250}

    ids = [e.id for page in pages for e in page.items]
    assert len(ids) == 250
    assert len(set(ids)) == 250  # no row repeated across pages


def test_terminal_event_is_reachable_via_offset(store: Store) -> None:
    # The outcome event sorts last; the default first page hides it, but
    # paging to the tail must surface it.
    _seed(_make_events(150))

    first_page = store.events.list(_ORG_ID, EventQuery(limit=100, offset=0), run_id=_RUN_ID).items
    assert all(e.event_type != "asset_completed" for e in first_page)

    last_page = store.events.list(_ORG_ID, EventQuery(limit=100, offset=100), run_id=_RUN_ID).items
    assert last_page[-1].event_type == "asset_completed"


def test_ordering_is_stable_for_equal_timestamps(store: Store) -> None:
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

    page1 = store.events.list(_ORG_ID, EventQuery(limit=10, offset=0), run_id=_RUN_ID).items
    page2 = store.events.list(_ORG_ID, EventQuery(limit=10, offset=10), run_id=_RUN_ID).items
    ids = [e.id for e in page1 + page2]
    assert len(set(ids)) == 20


def test_the_total_ignores_limit_and_offset(store: Store) -> None:
    _seed(_make_events(777))

    page = store.events.list(_ORG_ID, EventQuery(limit=100, offset=50), run_id=_RUN_ID)

    # A capped page does not change the reported total.
    assert len(page.items) == 100
    assert page.total == 777


def test_the_run_scope_isolates_runs(store: Store) -> None:
    _seed(_make_events(5, run_id=_RUN_ID))
    _seed(_make_events(3, run_id=_OTHER_RUN_ID))

    mine = store.events.list(_ORG_ID, EventQuery(limit=100), run_id=_RUN_ID)
    theirs = store.events.list(_ORG_ID, EventQuery(limit=100), run_id=_OTHER_RUN_ID)

    assert len(mine.items) == 5
    assert mine.total == 5
    assert theirs.total == 3
    assert {e.run_id for e in theirs.items} == {_OTHER_RUN_ID}


def test_asset_filter_lists_and_counts_only_that_asset(store: Store) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(150, component_id=asset_a))
    _seed(_make_events(30, start=150, component_id=asset_b))

    # Paging honours the filter: asset_a events past the first unfiltered
    # page are reachable through the filtered offsets.
    page_a = store.events.list(_ORG_ID, EventQuery(component_id=[asset_a], limit=100, offset=100), run_id=_RUN_ID)
    assert page_a.total == 150
    assert len(page_a.items) == 50
    assert all(e.component_id == asset_a for e in page_a.items)

    # asset_b's events all live beyond the first 150 rows of the run, yet its
    # filtered first page surfaces them.
    page_b = store.events.list(_ORG_ID, EventQuery(component_id=[asset_b], limit=100, offset=0), run_id=_RUN_ID)
    assert page_b.total == 30
    assert len(page_b.items) == 30
    assert all(e.component_id == asset_b for e in page_b.items)


def test_event_type_filter_lists_and_counts_only_those_types(store: Store) -> None:
    # _make_events emits n-1 "asset_materializing" then one "asset_completed".
    _seed(_make_events(5))

    def page(*event_types: str) -> Page[Event]:
        return store.events.list(_ORG_ID, EventQuery(event_type=list(event_types), limit=100), run_id=_RUN_ID)

    completed = page("asset_completed")
    assert completed.total == 1
    assert [e.event_type for e in completed.items] == ["asset_completed"]
    assert page("asset_materializing").total == 4
    # A set of types is the union of each.
    assert page("asset_completed", "asset_materializing").total == 5


def test_asset_and_event_type_filters_compose(store: Store) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(5, component_id=asset_a))
    _seed(_make_events(5, start=5, component_id=asset_b))

    # Each asset has exactly one "asset_completed"; narrowing to asset_a's set
    # of one type yields just that asset's completion.
    query = EventQuery(component_id=[asset_a], event_type=["asset_completed"], limit=100)
    page = store.events.list(_ORG_ID, query, run_id=_RUN_ID)

    assert page.total == 1
    assert len(page.items) == 1
    assert page.items[0].component_id == asset_a
    assert page.items[0].event_type == "asset_completed"


def test_asset_filter_accepts_multiple_assets(store: Store) -> None:
    asset_a, asset_b, asset_c = uuid4(), uuid4(), uuid4()
    _seed(_make_events(10, component_id=asset_a))
    _seed(_make_events(5, start=10, component_id=asset_b))
    _seed(_make_events(7, start=15, component_id=asset_c))

    # A set of asset ids (e.g. every asset of one status) is the union of each.
    page = store.events.list(_ORG_ID, EventQuery(component_id=[asset_a, asset_b], limit=100, offset=0), run_id=_RUN_ID)

    assert page.total == 15
    assert len(page.items) == 15
    assert all(e.component_id in {asset_a, asset_b} for e in page.items)


class TestFilters:
    """The organisation scope, the run scope and the query filters, each narrowing the listing and its total."""

    def test_the_org_scope_narrows_the_listing(self, store: Store) -> None:
        _seed(_make_events(2))
        _seed(_make_events(1, run_id=_OTHER_RUN_ID, org_id=uuid4()))

        page = store.events.list(_ORG_ID, EventQuery(limit=100))

        assert len(page.items) == 2
        assert page.total == 2
        assert {e.org_id for e in page.items} == {_ORG_ID}

    def test_another_orgs_run_reads_as_empty(self, store: Store) -> None:
        _seed(_make_events(3, run_id=_OTHER_RUN_ID, org_id=uuid4()))

        page = store.events.list(_ORG_ID, EventQuery(limit=100), run_id=_OTHER_RUN_ID)

        assert page.items == []
        assert page.total == 0

    def test_the_component_filter_narrows_the_listing(self, store: Store) -> None:
        mine = uuid4()
        _seed(_make_events(2, component_id=mine))
        _seed(_make_events(3, component_id=uuid4(), start=100))

        page = store.events.list(_ORG_ID, EventQuery(component_id=[mine], limit=100))

        assert len(page.items) == 2
        assert page.total == 2

    def test_several_component_ids_union(self, store: Store) -> None:
        first, second = uuid4(), uuid4()
        _seed(_make_events(2, component_id=first))
        _seed(_make_events(3, component_id=second, start=100))

        assert store.events.list(_ORG_ID, EventQuery(component_id=[first, second], limit=100)).total == 5

    def test_the_type_filter_narrows_the_listing(self, store: Store) -> None:
        _seed(_make_events(3))

        page = store.events.list(_ORG_ID, EventQuery(event_type=["asset_completed"], limit=100), run_id=_RUN_ID)

        assert [e.event_type for e in page.items] == ["asset_completed"]
        assert page.total == 1

    def test_the_filters_compose_with_the_run_scope(self, store: Store) -> None:
        asset = uuid4()
        _seed(_make_events(3, component_id=asset))
        _seed(_make_events(3, run_id=_OTHER_RUN_ID, component_id=asset, start=100))
        _seed([_error(_RUN_ID, "operation_failed", "boom", second=200)])

        query = EventQuery(component_id=[asset], event_type=["asset_completed"], limit=100)
        page = store.events.list(_ORG_ID, query, run_id=_OTHER_RUN_ID)

        assert [(e.run_id, e.component_id, e.event_type) for e in page.items] == [
            (_OTHER_RUN_ID, asset, "asset_completed")
        ]
        assert page.total == 1
        assert store.events.list(_ORG_ID, EventQuery(has_error=True, limit=100), run_id=_OTHER_RUN_ID).items == []

    def test_no_filters_counts_everything(self, store: Store) -> None:
        _seed(_make_events(2))
        _seed(_make_events(3, run_id=_OTHER_RUN_ID, start=100))

        assert store.events.list(_ORG_ID, EventQuery(limit=100)).total == 5


def _error(run_id: UUID, event_type: str, error: str | None, *, second: int = 0, org_id: UUID = _ORG_ID) -> Event:
    return Event(
        id=uuid4(),
        org_id=org_id,
        run_id=run_id,
        event_type=event_type,
        component_key="orders",
        error=error,
        timestamp=_BASE_TS + timedelta(seconds=second),
    )


class TestGet:
    def test_an_event_loads_by_id(self, store: Store) -> None:
        event = _make_events(1)[0]
        event_id = event.id
        _seed([event])

        assert store.events.get(event_id).event_type == "asset_completed"

    def test_a_missing_event_is_not_found(self, store: Store) -> None:
        with pytest.raises(NotFoundError):
            store.events.get(uuid4())

    def test_another_orgs_event_reads_as_missing(self, store: Store) -> None:
        event = _make_events(1)[0]
        event_id = event.id
        _seed([event])

        assert store.events.get(event_id, org_id=_ORG_ID).id == event_id
        with pytest.raises(NotFoundError):
            store.events.get(event_id, org_id=uuid4())


class TestHasError:
    def test_only_events_carrying_an_error_list_and_count(self, store: Store) -> None:
        _seed([_error(_RUN_ID, "operation_failed", "boom"), _error(_RUN_ID, "operation_started", None, second=1)])

        page = store.events.list(_ORG_ID, EventQuery(has_error=True, limit=100), run_id=_RUN_ID)

        assert [e.error for e in page.items] == ["boom"]
        assert page.total == 1
