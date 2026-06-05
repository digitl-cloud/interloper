"""Tests for deterministic asset-event ids in :mod:`interloper.runner.state`.

The id of an asset-lifecycle event is derived from ``(run_id, asset_id,
event_type)`` so the *same* logical event collapses to one row when produced by
more than one party — most importantly a cross-process runner's child container
emitting its own ``asset_failed`` and the host authoring a fallback one. These
tests pin that property at the source (the ``RunState`` emit path) without a DB.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import interloper as il
from interloper.events import Event, EventBus, EventType
from interloper.runner.state import RunState, _asset_event_id


class _Asset(il.Asset):
    """Plain asset fixture (never materialized in these tests)."""


@contextmanager
def _capture() -> Iterator[list[Event]]:
    """Collect every event delivered on the global bus within the block.

    Yields:
        The growing list of captured events.
    """
    captured: list[Event] = []
    EventBus.subscribe(captured.append)
    try:
        yield captured
        EventBus.flush(timeout=5.0)
    finally:
        EventBus.unsubscribe(captured.append)


def _state(run_id: str, asset_id: str) -> tuple[RunState, il.Asset]:
    asset = _Asset(id=asset_id)
    return RunState(il.DAG(asset), metadata={"run_id": run_id}), asset


# ---------------------------------------------------------------------------
# The pure derivation
# ---------------------------------------------------------------------------


def test_asset_event_id_is_deterministic() -> None:
    """Same triple → same id; any component differing → different id."""
    base = _asset_event_id("run-1", "asset-1", EventType.ASSET_FAILED)

    assert base == _asset_event_id("run-1", "asset-1", EventType.ASSET_FAILED)
    assert base != _asset_event_id("run-2", "asset-1", EventType.ASSET_FAILED)
    assert base != _asset_event_id("run-1", "asset-2", EventType.ASSET_FAILED)
    assert base != _asset_event_id("run-1", "asset-1", EventType.ASSET_COMPLETED)


# ---------------------------------------------------------------------------
# RunState stamps the deterministic id on what it emits
# ---------------------------------------------------------------------------


def test_mark_asset_terminal_stamps_deterministic_id() -> None:
    """``mark_asset_failed`` / ``mark_asset_completed`` carry the derived id."""
    state, asset = _state("run-abc", "asset-xyz")

    with _capture() as events:
        state.mark_asset_failed(asset, "boom", tb="trace")

    failed = next(e for e in events if e.type == EventType.ASSET_FAILED)
    assert failed.id == _asset_event_id("run-abc", "asset-xyz", EventType.ASSET_FAILED)
    assert failed.metadata["error"] == "boom"

    state2, asset2 = _state("run-abc", "asset-2")
    with _capture() as events:
        state2.mark_asset_completed(asset2)

    completed = next(e for e in events if e.type == EventType.ASSET_COMPLETED)
    assert completed.id == _asset_event_id("run-abc", "asset-2", EventType.ASSET_COMPLETED)


def test_host_fallback_and_child_terminal_share_one_id() -> None:
    """Two producers (host + child) emitting the same logical terminal collide.

    Simulates the cross-process topology: the child container and the host run
    the same ``RunState`` code with the same ``run_id`` and ``asset_id``. Both
    author ``asset_failed``; because the ids are equal, the idempotent
    ``save_event`` upsert keeps a single row instead of orphaning or doubling.
    """
    child_state, child_asset = _state("run-shared", "asset-shared")
    host_state, host_asset = _state("run-shared", "asset-shared")

    with _capture() as events:
        child_state.mark_asset_failed(child_asset, "child error")  # child's own terminal
        host_state.mark_asset_failed(host_asset, "Job foo failed")  # host fallback

    failed_ids = {e.id for e in events if e.type == EventType.ASSET_FAILED}
    assert len(failed_ids) == 1


def test_duplicate_asset_queued_collapses_to_one_id() -> None:
    """The host bulk ``asset_queued`` and a child re-emit share an id."""
    host_state, _ = _state("run-q", "asset-q")
    child_state, _ = _state("run-q", "asset-q")

    with _capture() as events:
        host_state.start_run(None)
        child_state.start_run(None)

    queued_ids = {e.id for e in events if e.type == EventType.ASSET_QUEUED}
    assert queued_ids == {_asset_event_id("run-q", "asset-q", EventType.ASSET_QUEUED)}
