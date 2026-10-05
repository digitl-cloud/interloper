"""Tests for the run models (``interloper_db.models.runs``)."""

from __future__ import annotations

import datetime as dt
from uuid import UUID, uuid4

import interloper as il
import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from interloper_db.models import Component, Event, Run


def test_event_metadata_carries_target_context() -> None:
    org = uuid4()
    target = Component(org_id=org, kind="job", key="nightly", name="Nightly sync")
    run = Run(id=uuid4(), org_id=org, component_id=target.id, backfill_id=uuid4())

    metadata = run.event_metadata(target)

    assert metadata == {
        "run_id": str(run.id),
        "backfill_id": str(run.backfill_id),
        "org_id": str(org),
        "target_id": str(target.id),
        "target_kind": "job",
        "target_key": "nightly",
        "target_name": "Nightly sync",
    }


def test_event_metadata_without_target() -> None:
    run = Run(id=uuid4(), org_id=uuid4())

    metadata = run.event_metadata(None)

    assert metadata == {"run_id": str(run.id), "backfill_id": None, "org_id": str(run.org_id)}


def test_a_stack_holds_one_run_per_attempt(auth_db: Engine) -> None:
    org = uuid4()
    root = Run(id=uuid4(), org_id=org)
    root.root_run_id = root.id
    with Session(auth_db) as session:
        session.add(root)
        session.commit()
        session.add(Run(id=uuid4(), org_id=org, root_run_id=root.id, retry_of=root.id, attempt=2))
        session.add(Run(id=uuid4(), org_id=org, root_run_id=root.id, retry_of=root.id, attempt=2))

        with pytest.raises(IntegrityError):
            session.commit()


_ORG_ID = uuid4()
_RUN_ID = UUID("99c018d6-98fe-4de5-a867-1f1a9a545a38")


@pytest.mark.parametrize("status", ["queued", "success", "failed", "canceled"])
def test_a_run_neither_dispatched_nor_running_is_never_overdue(status: str) -> None:
    long_ago = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    run = Run(org_id=uuid4(), status=status, heartbeat_at=long_ago, started_at=long_ago)

    overdue = run.overdue(long_ago + dt.timedelta(days=30), startup_timeout=1, heartbeat_timeout=1, run_timeout=1)

    assert overdue is None


def _event_values(event: il.Event, org_id: UUID, run_id: UUID | None) -> dict[str, object]:
    return Event.from_event(event, org_id, run_id).model_dump()


def test_sanitize_strips_nul_bytes() -> None:
    """NUL bytes (which Postgres text rejects) are removed."""
    assert Event._sanitize_text("a\x00b\x00c") == "abc"


def test_sanitize_passes_through_none() -> None:
    """``None`` stays ``None``."""
    assert Event._sanitize_text(None) is None


def test_sanitize_keeps_normal_text() -> None:
    """Ordinary text is returned unchanged."""
    assert Event._sanitize_text("hello world") == "hello world"


def test_sanitize_truncates_oversized() -> None:
    """Oversized values are capped and marked as truncated."""
    out = Event._sanitize_text("x" * 100, max_len=10)
    assert out is not None
    assert out.startswith("x" * 10)
    assert out.endswith("[truncated]")
    assert len(out) < 100


# -- Event payload sanitising ------------------------------------------------------


def test_sanitize_data_passes_json_through() -> None:
    assert Event._sanitize_data({"a": 1, "b": ["x", None]}) == {"a": 1, "b": ["x", None]}


def test_sanitize_data_empty_becomes_none() -> None:
    assert Event._sanitize_data({}) is None


def test_sanitize_data_coerces_non_json_values() -> None:
    """Non-JSON values go through ``str`` rather than failing the write."""
    out = Event._sanitize_data({"when": dt.date(2026, 8, 5)})
    assert out == {"when": "2026-08-05"}


def test_sanitize_data_strips_nul_escapes() -> None:
    """Postgres jsonb rejects NUL escapes just like text rejects NUL bytes."""
    assert Event._sanitize_data({"k": "a\x00b"}) == {"k": "ab"}


def test_sanitize_data_replaces_oversized_payloads() -> None:
    assert Event._sanitize_data({"blob": "x" * 100_000}) == {"truncated": True}


def test_sanitize_data_drops_unencodable_payloads() -> None:
    assert Event._sanitize_data({"nan": float("nan")}) is None


# -- Event.from_event ----------------------------------------------------------------


def _framework_event(metadata: dict[str, object]) -> il.Event:
    return il.Event(
        type=il.EventType.OPERATION_COMPLETED,
        timestamp=dt.datetime(2026, 8, 5, tzinfo=dt.timezone.utc),
        metadata=metadata,
    )


def test_event_values_maps_component_metadata_onto_columns() -> None:
    """The ``component_*`` identity keys core emitters stamp land on their columns."""
    component_id = uuid4()
    values = _event_values(
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
    values = _event_values(
        _framework_event({"component_id": str(hook_id), "component_kind": "hook", "component_key": "slack"}),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["component_id"] == hook_id
    assert values["component_kind"] == "hook"
    assert values["component_key"] == "slack"


def test_event_values_spills_unpromoted_metadata_into_data() -> None:
    """Metadata without a structured column persists losslessly in ``data``."""
    values = _event_values(
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
    values = _event_values(
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
    values = _event_values(_framework_event({"message": "run done"}), org_id=uuid4(), run_id=run_id)
    assert values["run_id"] == run_id
    assert values["component_id"] is None
    assert values["component_kind"] is None
    assert values["data"] is None


def test_event_values_preserves_producer_assigned_id() -> None:
    event = _framework_event({})
    values = _event_values(event, org_id=uuid4(), run_id=None)
    assert values["id"] == UUID(event.id)


# -- Listing -------------------------------------------------------------------


class TestEventValues:
    """The row values derived from a framework event."""

    def test_a_non_uuid_event_id_gets_a_fresh_one(self) -> None:
        # Producer ids are normally uuid5-derived; anything else still persists
        # rather than failing the write and dropping the event.
        event = il.Event(type=il.EventType.RUN_STARTED, metadata={})
        event.id = "not-a-uuid"

        values = _event_values(event, _ORG_ID, _RUN_ID)

        assert isinstance(values["id"], UUID)

    def test_a_uuid_event_id_is_preserved(self) -> None:
        # Identity survives end to end so the upsert dedups re-delivery.
        event = il.Event(type=il.EventType.RUN_STARTED, metadata={})

        values = _event_values(event, _ORG_ID, _RUN_ID)

        assert str(values["id"]) == event.id
