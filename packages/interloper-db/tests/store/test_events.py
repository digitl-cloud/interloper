"""Tests for ``interloper_db.store.events``."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import interloper as il
import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session

from interloper_db import engine as engine_module
from interloper_db.models import AssetExecution, Event
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
        type=il.EventType.ASSET_COMPLETED,
        timestamp=dt.datetime(2026, 8, 5, tzinfo=dt.timezone.utc),
        metadata=metadata,
    )


def test_event_values_maps_asset_metadata_onto_component_columns() -> None:
    """The ``asset_id``/``asset_key`` keys core emitters use land on the component columns.

    They land with kind ``asset`` — core needs no schema knowledge.
    """
    asset_id = uuid4()
    values = EventStore._event_values(
        _framework_event({"asset_id": str(asset_id), "asset_key": "ads", "message": "done"}),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["component_id"] == asset_id
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
                "asset_id": str(uuid4()),
                "asset_key": "ads",
                "asset_qualified_key": "facebook.ads",
                "source_id": "src-1",
                "error": "boom",
            }
        ),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["data"] == {"asset_qualified_key": "facebook.ads", "source_id": "src-1"}
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


def test_list_events_defaults_to_oldest_first(store: Store) -> None:
    _seed(_make_events(3))
    events = store.events.list_events(run_id=_RUN_ID)
    timestamps = [e.timestamp for e in events]
    assert timestamps == sorted(timestamps)


def test_offset_and_limit_page_without_gaps_or_repeats(store: Store) -> None:
    _seed(_make_events(250))

    page1 = store.events.list_events(run_id=_RUN_ID, limit=100, offset=0)
    page2 = store.events.list_events(run_id=_RUN_ID, limit=100, offset=100)
    page3 = store.events.list_events(run_id=_RUN_ID, limit=100, offset=200)

    assert [len(page1), len(page2), len(page3)] == [100, 100, 50]

    ids = [e.id for p in (page1, page2, page3) for e in p]
    assert len(ids) == 250
    assert len(set(ids)) == 250  # no row repeated across pages


def test_terminal_event_is_reachable_via_offset(store: Store) -> None:
    # The outcome event sorts last; the default first page hides it, but
    # paging to the tail must surface it.
    _seed(_make_events(150))

    first_page = store.events.list_events(run_id=_RUN_ID, limit=100, offset=0)
    assert all(e.event_type != "asset_completed" for e in first_page)

    last_page = store.events.list_events(run_id=_RUN_ID, limit=100, offset=100)
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

    page1 = store.events.list_events(run_id=_RUN_ID, limit=10, offset=0)
    page2 = store.events.list_events(run_id=_RUN_ID, limit=10, offset=10)
    ids = [e.id for e in page1 + page2]
    assert len(set(ids)) == 20


def test_count_events_ignores_limit_and_offset(store: Store) -> None:
    _seed(_make_events(777))
    assert store.events.count_events(run_id=_RUN_ID) == 777
    # A capped page does not change the reported total.
    assert len(store.events.list_events(run_id=_RUN_ID, limit=100)) == 100


def test_filters_isolate_runs(store: Store) -> None:
    _seed(_make_events(5, run_id=_RUN_ID))
    _seed(_make_events(3, run_id=_OTHER_RUN_ID))
    assert store.events.count_events(run_id=_RUN_ID) == 5
    assert store.events.count_events(run_id=_OTHER_RUN_ID) == 3
    assert len(store.events.list_events(run_id=_RUN_ID)) == 5


def test_asset_filter_lists_and_counts_only_that_asset(store: Store) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(150, component_id=asset_a))
    _seed(_make_events(30, start=150, component_id=asset_b))

    assert store.events.count_events(run_id=_RUN_ID, component_ids=[asset_a]) == 150
    assert store.events.count_events(run_id=_RUN_ID, component_ids=[asset_b]) == 30

    # Paging honours the filter: asset_a events past the first unfiltered
    # page are reachable through the filtered offsets.
    page2 = store.events.list_events(run_id=_RUN_ID, component_ids=[asset_a], limit=100, offset=100)
    assert len(page2) == 50
    assert all(e.component_id == asset_a for e in page2)

    # asset_b's events all live beyond the first 150 rows of the run, yet its
    # filtered first page surfaces them.
    page_b = store.events.list_events(run_id=_RUN_ID, component_ids=[asset_b], limit=100, offset=0)
    assert len(page_b) == 30
    assert all(e.component_id == asset_b for e in page_b)


def test_event_type_filter_lists_and_counts_only_those_types(store: Store) -> None:
    # _make_events emits n-1 "asset_materializing" then one "asset_completed".
    _seed(_make_events(5))

    assert store.events.count_events(run_id=_RUN_ID, event_types=["asset_completed"]) == 1
    assert store.events.count_events(run_id=_RUN_ID, event_types=["asset_materializing"]) == 4
    # A set of types is the union of each.
    assert store.events.count_events(run_id=_RUN_ID, event_types=["asset_completed", "asset_materializing"]) == 5

    completed = store.events.list_events(run_id=_RUN_ID, event_types=["asset_completed"])
    assert len(completed) == 1
    assert all(e.event_type == "asset_completed" for e in completed)


def test_asset_and_event_type_filters_compose(store: Store) -> None:
    asset_a, asset_b = uuid4(), uuid4()
    _seed(_make_events(5, component_id=asset_a))
    _seed(_make_events(5, start=5, component_id=asset_b))

    # Each asset has exactly one "asset_completed"; narrowing to asset_a's set
    # of one type yields just that asset's completion.
    assert store.events.count_events(run_id=_RUN_ID, component_ids=[asset_a], event_types=["asset_completed"]) == 1
    page = store.events.list_events(run_id=_RUN_ID, component_ids=[asset_a], event_types=["asset_completed"])
    assert len(page) == 1
    assert page[0].component_id == asset_a
    assert page[0].event_type == "asset_completed"


def test_asset_filter_accepts_multiple_assets(store: Store) -> None:
    asset_a, asset_b, asset_c = uuid4(), uuid4(), uuid4()
    _seed(_make_events(10, component_id=asset_a))
    _seed(_make_events(5, start=10, component_id=asset_b))
    _seed(_make_events(7, start=15, component_id=asset_c))

    # A set of asset ids (e.g. every asset of one status) is the union of each.
    assert store.events.count_events(run_id=_RUN_ID, component_ids=[asset_a, asset_b]) == 15
    page = store.events.list_events(run_id=_RUN_ID, component_ids=[asset_a, asset_b], limit=100, offset=0)
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


def test_complete_run_stamps_the_jobs_last_run_at(run_store: Store) -> None:
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


def test_asset_executions_read_model_maps_the_view(store: Store) -> None:
    """The typed read model round-trips rows shaped like the view's output.

    SQLite stands in: the model's table definition doubles as the view's
    schema, so creating it as a table exercises the exact mapping the view
    serves in production.
    """
    engine = engine_module.get_engine()
    AssetExecution.__table__.create(engine)  # ty: ignore[unresolved-attribute]
    run_id, asset_id, org = uuid4(), uuid4(), uuid4()
    with Session(engine) as session:
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

    rows = store.events.list_asset_executions(run_id)
    assert [(row.asset_key, row.status) for row in rows] == [("a", "success")]
    assert store.events.list_asset_executions(uuid4()) == []
