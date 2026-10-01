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
from interloper_db.models import Event, Execution, Run
from interloper_db.store import Store
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


# -- Pagination ----------------------------------------------------------------


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


def _make_events(n: int, *, run_id: UUID = _RUN_ID, start: int = 0, component_id: UUID | None = None) -> list[Event]:
    """Build ``n`` events for a run, one second apart, oldest first.

    Returns:
        The events in chronological order, the last one an ``asset_completed``.
    """
    return [
        Event(
            id=uuid4(),
            org_id=_ORG_ID,
            run_id=run_id,
            component_id=component_id,
            event_type="asset_materializing" if i < n - 1 else "asset_completed",
            timestamp=_BASE_TS + timedelta(seconds=start + i),
        )
        for i in range(n)
    ]


def test_list_defaults_to_oldest_first(store: Store) -> None:
    _seed(_make_events(3))
    events = store.events.list_all(run_id=_RUN_ID)
    timestamps = [e.timestamp for e in events]
    assert timestamps == sorted(timestamps)


def test_offset_and_limit_page_without_gaps_or_repeats(store: Store) -> None:
    _seed(_make_events(250))

    page1 = store.events.list_all(run_id=_RUN_ID, limit=100, offset=0)
    page2 = store.events.list_all(run_id=_RUN_ID, limit=100, offset=100)
    page3 = store.events.list_all(run_id=_RUN_ID, limit=100, offset=200)

    assert [len(page1), len(page2), len(page3)] == [100, 100, 50]

    ids = [e.id for p in (page1, page2, page3) for e in p]
    assert len(ids) == 250
    assert len(set(ids)) == 250  # no row repeated across pages


def test_terminal_event_is_reachable_via_offset(store: Store) -> None:
    # The outcome event sorts last; the default first page hides it, but
    # paging to the tail must surface it.
    _seed(_make_events(150))

    first_page = store.events.list_all(run_id=_RUN_ID, limit=100, offset=0)
    assert all(e.event_type != "asset_completed" for e in first_page)

    last_page = store.events.list_all(run_id=_RUN_ID, limit=100, offset=100)
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

    page1 = store.events.list_all(run_id=_RUN_ID, limit=10, offset=0)
    page2 = store.events.list_all(run_id=_RUN_ID, limit=10, offset=10)
    ids = [e.id for e in page1 + page2]
    assert len(set(ids)) == 20


def test_count_ignores_limit_and_offset(store: Store) -> None:
    _seed(_make_events(777))
    assert store.events.count(run_id=_RUN_ID) == 777
    # A capped page does not change the reported total.
    assert len(store.events.list_all(run_id=_RUN_ID, limit=100)) == 100


def test_filters_isolate_runs(store: Store) -> None:
    _seed(_make_events(5, run_id=_RUN_ID))
    _seed(_make_events(3, run_id=_OTHER_RUN_ID))
    assert store.events.count(run_id=_RUN_ID) == 5
    assert store.events.count(run_id=_OTHER_RUN_ID) == 3
    assert len(store.events.list_all(run_id=_RUN_ID)) == 5


def test_asset_filter_lists_and_counts_only_that_asset(store: Store) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(150, component_id=asset_a))
    _seed(_make_events(30, start=150, component_id=asset_b))

    assert store.events.count(run_id=_RUN_ID, component_ids=[asset_a]) == 150
    assert store.events.count(run_id=_RUN_ID, component_ids=[asset_b]) == 30

    # Paging honours the filter: asset_a events past the first unfiltered
    # page are reachable through the filtered offsets.
    page2 = store.events.list_all(run_id=_RUN_ID, component_ids=[asset_a], limit=100, offset=100)
    assert len(page2) == 50
    assert all(e.component_id == asset_a for e in page2)

    # asset_b's events all live beyond the first 150 rows of the run, yet its
    # filtered first page surfaces them.
    page_b = store.events.list_all(run_id=_RUN_ID, component_ids=[asset_b], limit=100, offset=0)
    assert len(page_b) == 30
    assert all(e.component_id == asset_b for e in page_b)


def test_event_type_filter_lists_and_counts_only_those_types(store: Store) -> None:
    # _make_events emits n-1 "asset_materializing" then one "asset_completed".
    _seed(_make_events(5))

    assert store.events.count(run_id=_RUN_ID, event_types=["asset_completed"]) == 1
    assert store.events.count(run_id=_RUN_ID, event_types=["asset_materializing"]) == 4
    # A set of types is the union of each.
    assert store.events.count(run_id=_RUN_ID, event_types=["asset_completed", "asset_materializing"]) == 5

    completed = store.events.list_all(run_id=_RUN_ID, event_types=["asset_completed"])
    assert len(completed) == 1
    assert all(e.event_type == "asset_completed" for e in completed)


def test_asset_and_event_type_filters_compose(store: Store) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(5, component_id=asset_a))
    _seed(_make_events(5, start=5, component_id=asset_b))

    # Each asset has exactly one "asset_completed"; narrowing to asset_a's set
    # of one type yields just that asset's completion.
    assert store.events.count(run_id=_RUN_ID, component_ids=[asset_a], event_types=["asset_completed"]) == 1
    page = store.events.list_all(run_id=_RUN_ID, component_ids=[asset_a], event_types=["asset_completed"])
    assert len(page) == 1
    assert page[0].component_id == asset_a
    assert page[0].event_type == "asset_completed"


def test_asset_filter_accepts_multiple_assets(store: Store) -> None:
    asset_a, asset_b, asset_c = uuid4(), uuid4(), uuid4()
    _seed(_make_events(10, component_id=asset_a))
    _seed(_make_events(5, start=10, component_id=asset_b))
    _seed(_make_events(7, start=15, component_id=asset_c))

    # A set of asset ids (e.g. every asset of one status) is the union of each.
    assert store.events.count(run_id=_RUN_ID, component_ids=[asset_a, asset_b]) == 15
    page = store.events.list_all(run_id=_RUN_ID, component_ids=[asset_a, asset_b], limit=100, offset=0)
    assert len(page) == 15
    assert all(e.component_id in {asset_a, asset_b} for e in page)


# -- complete_run job stamping -------------------------------------------------


@pytest.fixture
def run_store() -> Iterator[Store]:
    """A store over a database with runs, components, and usage tables.

    Yields:
        The store bound to that database, disposed once the test finishes.
    """
    from interloper_db.models import Component, Run, Usage

    engine = engine_module.init_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Component.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    Run.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    Usage.__table__.create(engine)  # ty: ignore[unresolved-attribute]  (completion settles usage)
    try:
        yield Store(catalog=il.Catalog(components={}), engine=engine)
    finally:
        engine.dispose()
        engine_module._engine = None


def test_complete_run_stamps_the_jobs_last_run_at_and_status(run_store: Store) -> None:
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

    completed = run_store.runs.complete(run_id, success=True)
    assert completed.status == "success"
    assert completed.completed_at is not None

    with Session(engine_module.get_engine()) as session:
        stamped = session.get(Component, component_id)
        assert stamped is not None and stamped.state is not None
        # SQLite round-trips the column naive; the stamped ISO string is aware UTC.
        stamped_at = datetime.fromisoformat(stamped.state["last_run_at"])
        assert stamped_at == completed.completed_at.replace(tzinfo=timezone.utc)
        assert stamped.state["last_run_status"] == "success"


def test_complete_run_stamps_a_failed_status(run_store: Store) -> None:
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

    run_store.runs.complete(run_id, success=False)

    with Session(engine_module.get_engine()) as session:
        stamped = session.get(Component, component_id)
        assert stamped is not None and stamped.state is not None
        assert stamped.state["last_run_status"] == "failed"


def test_executions_read_model_maps_the_view(store: Store) -> None:
    """The typed read model round-trips rows shaped like the view's output.

    SQLite stands in: the model's table definition doubles as the view's
    schema, so creating it as a table exercises the exact mapping the view
    serves in production.
    """
    engine = engine_module.get_engine()
    Execution.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    run_id, asset_id, org = uuid4(), uuid4(), uuid4()
    with Session(engine) as session:
        session.add(
            Execution(
                run_id=run_id,
                component_id=asset_id,
                org_id=org,
                component_key="a",
                status="success",
                completed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        )
        session.commit()

    rows = store.events.list_executions(run_id)
    assert [(row.component_key, row.status) for row in rows] == [("a", "success")]
    assert store.events.list_executions(uuid4()) == []


def test_count_executions_groups_each_run_by_status(store: Store) -> None:
    """One count per run and status, for the requested runs only; a run with nothing yet is absent."""
    engine = engine_module.get_engine()
    Execution.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    org = uuid4()
    first, second, unrequested, idle = uuid4(), uuid4(), uuid4(), uuid4()
    with Session(engine) as session:
        rows = [
            (first, "a", "success"),
            (first, "b", "success"),
            (first, "c", "failed"),
            (second, "a", "running"),
            (unrequested, "a", "success"),
        ]
        session.add_all(
            Execution(run_id=run, component_id=uuid4(), org_id=org, component_key=key, status=status)
            for run, key, status in rows
        )
        session.commit()

    counts = store.events.count_executions([first, second, idle])

    assert counts == {first: {"success": 2, "failed": 1}, second: {"running": 1}}
    assert store.events.count_executions([]) == {}


def test_latest_executions_keeps_the_newest_per_asset(store: Store) -> None:
    """One row per asset of the org: its most recent execution, older runs and other orgs dropped."""
    engine = engine_module.get_engine()
    Execution.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    org, other_org = uuid4(), uuid4()
    asset_a, asset_b, foreign = uuid4(), uuid4(), uuid4()
    old_run, new_run = uuid4(), uuid4()
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with Session(engine) as session:
        rows = [
            (old_run, asset_a, org, "a", "failed", t0),
            (new_run, asset_a, org, "a", "success", t0 + timedelta(hours=1)),
            (old_run, asset_b, org, "b", "running", t0),
            (old_run, foreign, other_org, "x", "success", t0),
        ]
        session.add_all(
            Execution(run_id=run, component_id=asset, org_id=owner, component_key=key, status=status, created_at=at)
            for run, asset, owner, key, status, at in rows
        )
        session.commit()

    rows = store.events.latest_executions(org)

    assert {(row.component_id, row.run_id, row.status) for row in rows} == {
        (asset_a, new_run, "success"),
        (asset_b, old_run, "running"),
    }
    assert store.events.latest_executions(uuid4()) == []


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
    """``list_all`` and ``count`` share one filter set."""

    def test_the_org_filter_narrows_the_listing(self, store: Store) -> None:
        _seed(_make_events(2))
        _seed([
            Event(
                id=uuid4(),
                org_id=uuid4(),
                run_id=_OTHER_RUN_ID,
                event_type="asset_completed",
                timestamp=_BASE_TS,
            )
        ])

        assert len(store.events.list_all(org_id=_ORG_ID)) == 2
        assert store.events.count(org_id=_ORG_ID) == 2

    def test_the_component_filter_narrows_the_listing(self, store: Store) -> None:
        mine = uuid4()
        _seed(_make_events(2, component_id=mine))
        _seed(_make_events(3, component_id=uuid4(), start=100))

        assert len(store.events.list_all(component_ids=[mine])) == 2
        assert store.events.count(component_ids=[mine]) == 2

    def test_several_component_ids_union(self, store: Store) -> None:
        first, second = uuid4(), uuid4()
        _seed(_make_events(2, component_id=first))
        _seed(_make_events(3, component_id=second, start=100))

        assert store.events.count(component_ids=[first, second]) == 5

    def test_the_type_filter_narrows_the_listing(self, store: Store) -> None:
        _seed(_make_events(3))

        assert store.events.count(run_id=_RUN_ID, event_types=["asset_completed"]) == 1

    def test_no_filters_counts_everything(self, store: Store) -> None:
        _seed(_make_events(2))
        _seed(_make_events(3, run_id=_OTHER_RUN_ID, start=100))

        assert store.events.count() == 5


@pytest.fixture
def run_tables(store: Store) -> None:
    """Add the runs table and the executions read model to the events-only database."""
    engine = engine_module.get_engine()
    Run.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    Execution.__table__.create(engine)  # ty: ignore[unresolved-attribute]


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

        assert [e.error for e in store.events.list_all(run_id=_RUN_ID, has_error=True)] == ["boom"]
        assert store.events.count(run_id=_RUN_ID, has_error=True) == 1


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
        _seed([
            _error(run, "asset_data_failed", "boom"),
            _error(run, "operation_failed", "boom", second=1),
            _error(run, "operation_started", None, second=2),
        ])

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


@pytest.mark.usefixtures("run_tables")
class TestPartitionCoverage:
    """Per partition of a job, whether each asset ever succeeded."""

    def _execution(self, run_id: UUID, asset_id: UUID, status: str) -> None:
        with Session(engine_module.get_engine()) as session:
            session.add(
                Execution(run_id=run_id, component_id=asset_id, org_id=_ORG_ID, component_key="orders", status=status)
            )
            session.commit()

    def test_any_successful_execution_covers_the_partition(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        failed_first = _run(job_id=job, partition_key="2026-07-01")
        healed_later = _run(job_id=job, partition_key="2026-07-01")
        never_healed = _run(job_id=job, partition_key="2026-07-02")
        self._execution(failed_first, asset, "failed")
        self._execution(healed_later, asset, "success")
        self._execution(never_healed, asset, "failed")

        rows = store.events.partition_coverage(_ORG_ID, job, "2026-07-01", "2026-07-02")

        assert sorted((r.partition_key, r.succeeded) for r in rows) == [("2026-07-01", True), ("2026-07-02", False)]

    def test_other_granularities_jobs_and_orgs_stay_out(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        self._execution(_run(job_id=job, partition_key="2026-07-01T13"), asset, "success")
        self._execution(_run(job_id=uuid4(), partition_key="2026-07-01"), asset, "success")
        self._execution(_run(job_id=job, partition_key="2026-07-03"), asset, "success")

        assert store.events.partition_coverage(_ORG_ID, job, "2026-07-01", "2026-07-02") == []
        assert store.events.partition_coverage(uuid4(), job, "2026-07-01", "2026-07-03") == []

    def test_a_job_without_runs_has_no_coverage(self, store: Store) -> None:
        assert store.events.partition_coverage(_ORG_ID, uuid4(), "2026-07-01", "2026-07-02") == []


@pytest.mark.usefixtures("run_tables")
class TestCoverageRows:
    """Org-wide coverage: every job's partitions overlapping a day window, at any granularity."""

    def _execution(self, run_id: UUID, asset_id: UUID, status: str) -> None:
        with Session(engine_module.get_engine()) as session:
            session.add(
                Execution(run_id=run_id, component_id=asset_id, org_id=_ORG_ID, component_key="orders", status=status)
            )
            session.commit()

    def test_every_granularity_overlapping_the_window_is_read(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        day = _run(job_id=job, partition_key="2026-07-02")
        hour = _run(job_id=job, partition_key="2026-07-01T13")
        month = _run(job_id=job, partition_key="2026-06")
        year = _run(job_id=job, partition_key="2026")
        outside_day = _run(job_id=job, partition_key="2026-07-03")
        outside_month = _run(job_id=job, partition_key="2026-05")
        for run in (day, hour, month, year, outside_day, outside_month):
            self._execution(run, asset, "success")

        rows = store.events.coverage_rows(_ORG_ID, dt.date(2026, 6, 30), dt.date(2026, 7, 2))

        assert sorted(r.partition_key for r in rows) == ["2026", "2026-06", "2026-07-01T13", "2026-07-02"]
        assert all(r.job_id == job and r.asset_id == asset and r.succeeded for r in rows)

    def test_a_failed_run_is_reported_until_an_execution_succeeds(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        failed = _run(job_id=job, partition_key="2026-07-01")
        healed = _run(job_id=job, partition_key="2026-07-01")
        still_failed = _run(job_id=job, partition_key="2026-07-02")
        self._execution(failed, asset, "failed")
        self._execution(healed, asset, "success")
        self._execution(still_failed, asset, "failed")

        rows = {
            r.partition_key: r for r in store.events.coverage_rows(_ORG_ID, dt.date(2026, 7, 1), dt.date(2026, 7, 2))
        }

        assert rows["2026-07-01"].succeeded and rows["2026-07-01"].failed_run_id == failed
        assert not rows["2026-07-02"].succeeded and rows["2026-07-02"].failed_run_id == still_failed

    def test_only_a_failed_execution_marks_the_row_failed(self, store: Store) -> None:
        job, asset = uuid4(), uuid4()
        self._execution(_run(job_id=job, partition_key="2026-07-01"), asset, "failed")
        self._execution(_run(job_id=job, partition_key="2026-07-02"), asset, "running")
        self._execution(_run(job_id=job, partition_key="2026-07-03"), asset, "canceled")

        rows = {
            r.partition_key: r for r in store.events.coverage_rows(_ORG_ID, dt.date(2026, 7, 1), dt.date(2026, 7, 3))
        }

        assert {key: (row.succeeded, row.failed) for key, row in rows.items()} == {
            "2026-07-01": (False, True),
            "2026-07-02": (False, False),
            "2026-07-03": (False, False),
        }

    def test_unpartitioned_runs_deleted_jobs_and_other_orgs_stay_out(self, store: Store) -> None:
        asset = uuid4()
        self._execution(_run(job_id=uuid4(), partition_key=None), asset, "success")
        self._execution(_run(job_id=None, partition_key="2026-07-01"), asset, "success")
        self._execution(_run(org_id=uuid4(), job_id=uuid4(), partition_key="2026-07-01"), asset, "success")

        assert store.events.coverage_rows(_ORG_ID, dt.date(2026, 7, 1), dt.date(2026, 7, 2)) == []


class TestLatestByComponent:
    """One event per component: its newest of the given types, other types and orgs dropped."""

    def test_the_newest_event_of_the_types_is_kept(self, store: Store) -> None:
        hook_a, hook_b, hook_c = uuid4(), uuid4(), uuid4()
        low_id, high_id = UUID(int=1), UUID(int=2)
        _seed(
            [
                Event(id=uuid4(), org_id=_ORG_ID, component_id=None, event_type="hook_fired", timestamp=_BASE_TS),
                Event(id=low_id, org_id=_ORG_ID, component_id=hook_c, event_type="hook_fired", timestamp=_BASE_TS),
                Event(id=high_id, org_id=_ORG_ID, component_id=hook_c, event_type="hook_failed", timestamp=_BASE_TS),
                Event(id=uuid4(), org_id=_ORG_ID, component_id=hook_a, event_type="hook_failed", timestamp=_BASE_TS),
                Event(
                    id=uuid4(),
                    org_id=_ORG_ID,
                    component_id=hook_a,
                    event_type="hook_fired",
                    timestamp=_BASE_TS + timedelta(minutes=1),
                ),
                Event(
                    id=uuid4(),
                    org_id=_ORG_ID,
                    component_id=hook_a,
                    event_type="log",
                    timestamp=_BASE_TS + timedelta(minutes=2),
                ),
                Event(id=uuid4(), org_id=_ORG_ID, component_id=hook_b, event_type="hook_failed", timestamp=_BASE_TS),
                Event(id=uuid4(), org_id=uuid4(), component_id=uuid4(), event_type="hook_failed", timestamp=_BASE_TS),
            ]
        )

        rows = store.events.latest_by_component(_ORG_ID, event_types=["hook_fired", "hook_failed"])

        assert {(e.component_id, e.event_type) for e in rows} == {
            (hook_a, "hook_fired"),
            (hook_b, "hook_failed"),
            (hook_c, "hook_failed"),
        }
        assert {e.id for e in rows if e.component_id == hook_c} == {high_id}
        assert store.events.latest_by_component(uuid4(), event_types=["hook_fired"]) == []

    def test_since_bounds_the_read(self, store: Store) -> None:
        recent_hook, stale_hook = uuid4(), uuid4()
        _seed(
            [
                Event(
                    id=uuid4(),
                    org_id=_ORG_ID,
                    component_id=recent_hook,
                    event_type="hook_fired",
                    timestamp=_BASE_TS + timedelta(days=1),
                ),
                Event(
                    id=uuid4(), org_id=_ORG_ID, component_id=stale_hook, event_type="hook_failed", timestamp=_BASE_TS
                ),
            ]
        )

        rows = store.events.latest_by_component(
            _ORG_ID, event_types=["hook_fired", "hook_failed"], since=_BASE_TS + timedelta(hours=1)
        )

        assert [e.component_id for e in rows] == [recent_hook]
