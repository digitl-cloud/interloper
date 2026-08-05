"""Unit tests for ``RunExecutor`` retry skip logic.

These avoid a live database by faking the store's asset-execution lookups and
the lineage-walk session, so they stay pure unit tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from interloper_db.models import Component, Run

from interloper_scheduler import executor as executor_module
from interloper_scheduler.executor import RunExecutor, run_event_metadata


class _FakeStore:
    """Returns canned asset_executions per run_id."""

    engine = None  # the fake Session ignores it

    def __init__(self, executions: dict[UUID, list[dict[str, Any]]]) -> None:
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
    monkeypatch.setattr(executor_module, "Session", lambda _engine: _FakeSession(runs))


class _Asset:
    def __init__(self, asset_id: UUID, key: str = "a") -> None:
        self.id = str(asset_id)
        self.key = key
        self.materializable = True


def test_succeeded_assets_are_marked_non_materializable(monkeypatch: pytest.MonkeyPatch) -> None:
    parent_id = uuid4()
    id_a, id_b = uuid4(), uuid4()
    store = _FakeStore(
        {parent_id: [{"asset_id": id_a, "status": "success"}, {"asset_id": id_b, "status": "failed"}]}
    )
    _patch_session(monkeypatch, {parent_id: SimpleNamespace(retry_of=None)})

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    asset_a, asset_b = _Asset(id_a), _Asset(id_b)

    executor._skip_succeeded_assets(parent_id, [asset_a, asset_b])  # ty: ignore[invalid-argument-type]

    assert asset_a.materializable is False  # succeeded → skipped
    assert asset_b.materializable is True  # failed → re-runs


def test_statuses_match_by_asset_id_not_key(monkeypatch: pytest.MonkeyPatch) -> None:
    # A run can span many assets sharing one key (e.g. an ads_stats per
    # account). One account's success must not skip the others' retries.
    parent_id = uuid4()
    id_a, id_b, id_c = uuid4(), uuid4(), uuid4()
    store = _FakeStore(
        {
            parent_id: [
                {"asset_id": id_a, "status": "success"},
                {"asset_id": id_b, "status": "failed"},
                {"asset_id": id_c, "status": "canceled"},
            ]
        }
    )
    _patch_session(monkeypatch, {parent_id: SimpleNamespace(retry_of=None)})

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    assets = [_Asset(id_a, key="ads_stats"), _Asset(id_b, key="ads_stats"), _Asset(id_c, key="ads_stats")]

    executor._skip_succeeded_assets(parent_id, assets)  # ty: ignore[invalid-argument-type]

    assert [a.materializable for a in assets] == [False, True, True]


def test_success_carries_forward_across_the_lineage_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    # attempt1: a succeeded, b failed.  attempt2 (failed-only) re-ran only b,
    # which failed again — so attempt2 has no event for the skipped 'a'.
    # Retrying attempt2 must still skip 'a' by walking back to attempt1.
    root_id = uuid4()
    mid_id = uuid4()
    id_a, id_b = uuid4(), uuid4()
    store = _FakeStore(
        {
            mid_id: [{"asset_id": id_b, "status": "failed"}],
            root_id: [{"asset_id": id_a, "status": "success"}, {"asset_id": id_b, "status": "failed"}],
        }
    )
    _patch_session(
        monkeypatch,
        {mid_id: SimpleNamespace(retry_of=root_id), root_id: SimpleNamespace(retry_of=None)},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    asset_a, asset_b = _Asset(id_a), _Asset(id_b)

    executor._skip_succeeded_assets(mid_id, [asset_a, asset_b])  # ty: ignore[invalid-argument-type]

    assert asset_a.materializable is False
    assert asset_b.materializable is True


def test_closest_ancestor_status_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    # If an asset failed in the root but succeeded in a later attempt, the
    # most-recent (closest) success should win and the asset should be skipped.
    root_id = uuid4()
    mid_id = uuid4()
    id_a = uuid4()
    store = _FakeStore(
        {
            mid_id: [{"asset_id": id_a, "status": "success"}],
            root_id: [{"asset_id": id_a, "status": "failed"}],
        }
    )
    _patch_session(
        monkeypatch,
        {mid_id: SimpleNamespace(retry_of=root_id), root_id: SimpleNamespace(retry_of=None)},
    )

    executor = RunExecutor(store=store)  # ty: ignore[invalid-argument-type]
    asset = _Asset(id_a)

    executor._skip_succeeded_assets(mid_id, [asset])  # ty: ignore[invalid-argument-type]

    assert asset.materializable is False


# -- run_event_metadata ----------------------------------------------------------


def test_run_event_metadata_carries_target_context() -> None:
    org = uuid4()
    target = Component(org_id=org, kind="job", key="nightly", name="Nightly sync")
    run = Run(id=uuid4(), org_id=org, component_id=target.id, backfill_id=uuid4())

    metadata = run_event_metadata(run, target)

    assert metadata == {
        "run_id": str(run.id),
        "backfill_id": str(run.backfill_id),
        "org_id": str(org),
        "target_id": str(target.id),
        "target_kind": "job",
        "target_key": "nightly",
        "target_name": "Nightly sync",
    }


def test_run_event_metadata_without_target() -> None:
    run = Run(id=uuid4(), org_id=uuid4())

    metadata = run_event_metadata(run, None)

    assert metadata == {"run_id": str(run.id), "backfill_id": None, "org_id": str(run.org_id)}
