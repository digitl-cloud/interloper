"""Unit tests for ``RunExecutor`` retry skip logic.

These avoid a live database by faking the store's asset-execution lookups and
the lineage-walk session, so they stay pure unit tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

from interloper_scheduler import executor as executor_module
from interloper_scheduler.executor import RunExecutor


class _FakeStore:
    """Returns canned asset_executions per run_id."""

    def __init__(self, executions: dict[UUID, list[dict[str, str]]]) -> None:
        self._executions = executions

    def list_asset_executions(self, run_id: UUID) -> list[SimpleNamespace]:
        return [SimpleNamespace(**row) for row in self._executions.get(run_id, [])]


class _FakeSession:
    """Context-manager session whose ``get`` resolves runs by id."""

    def __init__(self, runs: dict[UUID, Any]) -> None:
        self._runs = runs

    def __enter__(self) -> _FakeSession:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def get(self, _model: Any, run_id: UUID) -> Any:
        return self._runs.get(run_id)


def _patch_session(monkeypatch: pytest.MonkeyPatch, runs: dict[UUID, Any]) -> None:
    monkeypatch.setattr(executor_module, "get_engine", lambda: None)
    monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(runs))


class _AssetA:
    key = "a"

    def __init__(self) -> None:
        self.materializable = True


class _AssetB:
    key = "b"

    def __init__(self) -> None:
        self.materializable = True


def test_succeeded_assets_are_marked_non_materializable(monkeypatch: pytest.MonkeyPatch) -> None:
    parent_id = uuid4()
    store = _FakeStore(
        {parent_id: [{"asset_key": "a", "status": "success"}, {"asset_key": "b", "status": "failed"}]}
    )
    _patch_session(monkeypatch, {parent_id: SimpleNamespace(retry_of=None)})

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    assets = [_AssetA(), _AssetB()]

    executor._skip_succeeded_assets(parent_id, assets)  # ty: ignore[invalid-argument-type]

    by_key = {type(a).key: a for a in assets}
    assert by_key["a"].materializable is False  # succeeded → skipped
    assert by_key["b"].materializable is True  # failed → re-runs


def test_success_carries_forward_across_the_lineage_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    # attempt1: a succeeded, b failed.  attempt2 (failed-only) re-ran only b,
    # which failed again — so attempt2 has no event for the skipped 'a'.
    # Retrying attempt2 must still skip 'a' by walking back to attempt1.
    root_id = uuid4()
    mid_id = uuid4()
    store = _FakeStore(
        {
            mid_id: [{"asset_key": "b", "status": "failed"}],
            root_id: [{"asset_key": "a", "status": "success"}, {"asset_key": "b", "status": "failed"}],
        }
    )
    _patch_session(
        monkeypatch,
        {mid_id: SimpleNamespace(retry_of=root_id), root_id: SimpleNamespace(retry_of=None)},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    assets = [_AssetA(), _AssetB()]

    executor._skip_succeeded_assets(mid_id, assets)  # ty: ignore[invalid-argument-type]

    by_key = {type(a).key: a for a in assets}
    assert by_key["a"].materializable is False
    assert by_key["b"].materializable is True


def test_closest_ancestor_status_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    # If an asset failed in the root but succeeded in a later attempt, the
    # most-recent (closest) success should win and the asset should be skipped.
    root_id = uuid4()
    mid_id = uuid4()
    store = _FakeStore(
        {
            mid_id: [{"asset_key": "a", "status": "success"}],
            root_id: [{"asset_key": "a", "status": "failed"}],
        }
    )
    _patch_session(
        monkeypatch,
        {mid_id: SimpleNamespace(retry_of=root_id), root_id: SimpleNamespace(retry_of=None)},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    assets = [_AssetA()]

    executor._skip_succeeded_assets(mid_id, assets)  # ty: ignore[invalid-argument-type]

    assert assets[0].materializable is False
