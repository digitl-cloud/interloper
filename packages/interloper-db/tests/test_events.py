"""Tests for event persistence helpers."""

from __future__ import annotations

import datetime as dt
from uuid import UUID, uuid4

import interloper as il

from interloper_db.store.runs import RunMixin


def test_sanitize_strips_nul_bytes() -> None:
    """NUL bytes (which Postgres text rejects) are removed."""
    assert RunMixin._sanitize_text("a\x00b\x00c") == "abc"


def test_sanitize_passes_through_none() -> None:
    """``None`` stays ``None``."""
    assert RunMixin._sanitize_text(None) is None


def test_sanitize_keeps_normal_text() -> None:
    """Ordinary text is returned unchanged."""
    assert RunMixin._sanitize_text("hello world") == "hello world"


def test_sanitize_truncates_oversized() -> None:
    """Oversized values are capped and marked as truncated."""
    out = RunMixin._sanitize_text("x" * 100, max_len=10)
    assert out is not None
    assert out.startswith("x" * 10)
    assert out.endswith("[truncated]")
    assert len(out) < 100


# -- _sanitize_data --------------------------------------------------------------


def test_sanitize_data_passes_json_through() -> None:
    assert RunMixin._sanitize_data({"a": 1, "b": ["x", None]}) == {"a": 1, "b": ["x", None]}


def test_sanitize_data_empty_becomes_none() -> None:
    assert RunMixin._sanitize_data({}) is None


def test_sanitize_data_coerces_non_json_values() -> None:
    """Non-JSON values go through ``str`` rather than failing the write."""
    out = RunMixin._sanitize_data({"when": dt.date(2026, 8, 5)})
    assert out == {"when": "2026-08-05"}


def test_sanitize_data_strips_nul_escapes() -> None:
    """Postgres jsonb rejects NUL escapes just like text rejects NUL bytes."""
    assert RunMixin._sanitize_data({"k": "a\x00b"}) == {"k": "ab"}


def test_sanitize_data_replaces_oversized_payloads() -> None:
    assert RunMixin._sanitize_data({"blob": "x" * 100_000}) == {"truncated": True}


def test_sanitize_data_drops_unencodable_payloads() -> None:
    assert RunMixin._sanitize_data({"nan": float("nan")}) is None


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
    values = RunMixin._event_values(
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
    values = RunMixin._event_values(
        _framework_event({"component_id": str(hook_id), "component_kind": "hook", "component_key": "slack"}),
        org_id=uuid4(),
        run_id=None,
    )
    assert values["component_id"] == hook_id
    assert values["component_kind"] == "hook"
    assert values["component_key"] == "slack"


def test_event_values_spills_unpromoted_metadata_into_data() -> None:
    """Metadata without a structured column persists losslessly in ``data``."""
    values = RunMixin._event_values(
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
    values = RunMixin._event_values(
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
    values = RunMixin._event_values(_framework_event({"message": "run done"}), org_id=uuid4(), run_id=run_id)
    assert values["run_id"] == run_id
    assert values["component_id"] is None
    assert values["component_kind"] is None
    assert values["data"] is None


def test_event_values_preserves_producer_assigned_id() -> None:
    event = _framework_event({})
    values = RunMixin._event_values(event, org_id=uuid4(), run_id=None)
    assert values["id"] == UUID(event.id)
