"""Tests for ``interloper_db.store.events``."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import interloper as il
import pytest
from interloper.errors import NotFoundError
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db.models import Event, Run
from interloper_db.store import EventQuery, Page, Store
from interloper_db.store.events import EventStore

_RUN_ID = UUID("99c018d6-98fe-4de5-a867-1f1a9a545a38")
_OTHER_RUN_ID = uuid4()
_ORG_ID = uuid4()
_BASE_TS = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)


def test_sanitize_strips_nul_bytes() -> None:
    """NUL bytes (which Postgres text rejects) are removed."""
    assert EventStore._sanitize_text("a\x00b\x00c") == "abc"


def test_sanitize_passes_through_none() -> None:
    """``None`` stays ``None``."""
    assert EventStore._sanitize_text(None) is None


def test_sanitize_keeps_normal_text() -> None:
    """Ordinary text is returned unchanged."""
    assert EventStore._sanitize_text("hello world") == "hello world"


def test_sanitize_truncates_oversized() -> None:
    """Oversized values are capped and marked as truncated."""
    out = EventStore._sanitize_text("x" * 100, max_len=10)
    assert out is not None
    assert out.startswith("x" * 10)
    assert out.endswith("[truncated]")
    assert len(out) < 100


# -- _sanitize_data --------------------------------------------------------------


def test_sanitize_data_passes_json_through() -> None:
    assert EventStore._sanitize_data({"a": 1, "b": ["x", None]}) == {"a": 1, "b": ["x", None]}


def test_sanitize_data_empty_becomes_none() -> None:
    assert EventStore._sanitize_data({}) is None


def test_sanitize_data_coerces_non_json_values() -> None:
    """Non-JSON values go through ``str`` rather than failing the write."""
    out = EventStore._sanitize_data({"when": dt.date(2026, 8, 5)})
    assert out == {"when": "2026-08-05"}


def test_sanitize_data_strips_nul_escapes() -> None:
    """Postgres jsonb rejects NUL escapes just like text rejects NUL bytes."""
    assert EventStore._sanitize_data({"k": "a\x00b"}) == {"k": "ab"}


def test_sanitize_data_replaces_oversized_payloads() -> None:
    assert EventStore._sanitize_data({"blob": "x" * 100_000}) == {"truncated": True}


def test_sanitize_data_drops_unencodable_payloads() -> None:
    assert EventStore._sanitize_data({"nan": float("nan")}) is None


# -- _event_values ---------------------------------------------------------------


def _framework_event(metadata: dict[str, object]) -> il.Event:
    return il.Event(
        type=il.EventType.OPERATION_COMPLETED,
        timestamp=dt.datetime(2026, 8, 5, tzinfo=dt.timezone.utc),
        metadata=metadata,
    )


def test_event_values_maps_component_metadata_onto_columns() -> None:
    """The ``component_*`` identity keys core emitters stamp land on their columns."""
    component_id = uuid4()
    values = EventStore._event_values(
        _framework_event(
            {"component_id": str(component_id), "component_kind": "asset", "component_key": "ads", "message": "done"}
        ),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["component_id"] == component_id
    assert values["component_kind"] == "asset"
    assert values["component_key"] == "ads"
    assert values["message"] == "done"


def test_event_values_accepts_explicit_component_metadata() -> None:
    hook_id = uuid4()
    values = EventStore._event_values(
        _framework_event({"component_id": str(hook_id), "component_kind": "hook", "component_key": "slack"}),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["component_id"] == hook_id
    assert values["component_kind"] == "hook"
    assert values["component_key"] == "slack"


def test_event_values_spills_unpromoted_metadata_into_data() -> None:
    """Metadata without a structured column persists losslessly in ``data``."""
    values = EventStore._event_values(
        _framework_event(
            {
                "component_id": str(uuid4()),
                "component_key": "ads",
                "asset_qualified_key": "facebook.ads",
                "parent_id": "src-1",
                "error": "boom",
            }
        ),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["data"] == {"asset_qualified_key": "facebook.ads", "parent_id": "src-1"}
    assert values["error"] == "boom"


def test_event_values_spills_demoted_scope_keys_into_data() -> None:
    """backfill_id / partition_or_window have no column since 006.

    They ride in ``data``, and the None values producers emit unconditionally don't.
    """
    values = EventStore._event_values(
        _framework_event(
            {
                "backfill_id": "b0e0a72f-7e2f-49a8-bb3e-9adfa22a1eb3",
                "partition_or_window": "2026-08-04",
                "target_kind": None,
            }
        ),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["data"] == {
        "backfill_id": "b0e0a72f-7e2f-49a8-bb3e-9adfa22a1eb3",
        "partition_or_window": "2026-08-04",
    }
    assert "backfill_id" not in values and "partition_or_window" not in values


def test_event_values_without_component_or_extras() -> None:
    run_id = uuid4()
    values = EventStore._event_values(_framework_event({"message": "run done"}), org_id=uuid4(), run_id=run_id)
    assert values["run_id"] == run_id
    assert values["component_id"] is None
    assert values["component_kind"] is None
    assert values["data"] is None


def test_event_values_preserves_producer_assigned_id() -> None:
    event = _framework_event({})
    values = EventStore._event_values(event, org_id=uuid4(), run_id=None)
    assert values["id"] == UUID(event.id)


# -- Listing -------------------------------------------------------------------


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


class TestEventValues:
    """The row values derived from a framework event."""

    def test_a_non_uuid_event_id_gets_a_fresh_one(self) -> None:
        # Producer ids are normally uuid5-derived; anything else still persists
        # rather than failing the write and dropping the event.
        event = il.Event(type=il.EventType.RUN_STARTED, metadata={})
        event.id = "not-a-uuid"

        values = EventStore._event_values(event, _ORG_ID, _RUN_ID)

        assert isinstance(values["id"], UUID)

    def test_a_uuid_event_id_is_preserved(self) -> None:
        # Identity survives end to end so the upsert dedups re-delivery.
        event = il.Event(type=il.EventType.RUN_STARTED, metadata={})

        values = EventStore._event_values(event, _ORG_ID, _RUN_ID)

        assert str(values["id"]) == event.id


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


@pytest.fixture
def run_tables(store: Store) -> None:
    """Add the runs table to the events-only database."""
    Run.__table__.create(engine_module.get_engine())  # ty: ignore[unresolved-attribute]


def _run(*, org_id: UUID = _ORG_ID, job_id: UUID | None = None, partition_key: str | None = None) -> UUID:
    run_id = uuid4()
    with Session(engine_module.get_engine()) as session:
        session.add(Run(id=run_id, org_id=org_id, component_id=job_id, partition_key=partition_key, status="failed"))
        session.commit()
    return run_id


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


@pytest.mark.usefixtures("run_tables")
class TestErrorGroups:
    """Error events collapse per job, run, component, type and text, loudest first."""

    def test_identical_texts_collapse_per_run(self, store: Store) -> None:
        job = uuid4()
        run = _run(job_id=job)
        _seed([_error(run, "operation_retried", "HTTPStatusError: 429", second=s) for s in range(3)])
        _seed([_error(run, "operation_failed", "HTTPStatusError: 429", second=9)])

        groups, truncated = store.events.error_groups(_ORG_ID, event_types=["operation_retried", "operation_failed"])

        assert not truncated
        assert [(g.event_type, g.count, g.job_id) for g in groups] == [
            ("operation_retried", 3, job),
            ("operation_failed", 1, job),
        ]
        assert groups[0].first_seen < groups[0].last_seen

    def test_only_the_requested_types_with_an_error_are_read(self, store: Store) -> None:
        run = _run()
        _seed(
            [
                _error(run, "asset_data_failed", "boom"),
                _error(run, "operation_failed", "boom", second=1),
                _error(run, "operation_started", None, second=2),
            ]
        )

        groups, _ = store.events.error_groups(_ORG_ID, event_types=["operation_failed"])

        assert [g.event_type for g in groups] == ["operation_failed"]

    def test_the_window_and_scope_filters_narrow_the_scan(self, store: Store) -> None:
        job, other_job = uuid4(), uuid4()
        mine, theirs = _run(job_id=job), _run(job_id=other_job)
        _seed([_error(mine, "operation_failed", "early"), _error(mine, "operation_failed", "late", second=60)])
        _seed([_error(theirs, "operation_failed", "other job", second=60)])
        _seed([_error(_run(org_id=uuid4()), "operation_failed", "other org", org_id=uuid4(), second=60)])
        types = ["operation_failed"]

        windowed, _ = store.events.error_groups(_ORG_ID, event_types=types, since=_BASE_TS + timedelta(seconds=30))
        by_job, _ = store.events.error_groups(_ORG_ID, event_types=types, job_id=job)
        by_run, _ = store.events.error_groups(_ORG_ID, event_types=types, run_id=theirs)
        until, _ = store.events.error_groups(_ORG_ID, event_types=types, until=_BASE_TS + timedelta(seconds=30))

        assert {g.error for g in windowed} == {"late", "other job"}
        assert {g.error for g in by_job} == {"early", "late"}
        assert {g.error for g in by_run} == {"other job"}
        assert {g.error for g in until} == {"early"}

    def test_the_backfill_filter_reads_its_runs(self, store: Store) -> None:
        backfill = uuid4()
        run = uuid4()
        with Session(engine_module.get_engine()) as session:
            session.add(Run(id=run, org_id=_ORG_ID, backfill_id=backfill, status="failed"))
            session.commit()
        _seed([_error(run, "operation_failed", "in backfill"), _error(_run(), "operation_failed", "outside")])

        groups, _ = store.events.error_groups(_ORG_ID, event_types=["operation_failed"], backfill_id=backfill)

        assert [g.error for g in groups] == ["in backfill"]

    def test_the_cap_reports_that_it_cut_groups_off(self, store: Store) -> None:
        run = _run()
        _seed([_error(run, "operation_failed", f"error {i}", second=i) for i in range(3)])

        groups, truncated = store.events.error_groups(_ORG_ID, event_types=["operation_failed"], max_rows=2)

        assert len(groups) == 2
        assert truncated
