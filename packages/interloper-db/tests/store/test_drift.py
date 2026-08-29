"""Tests for the ``DriftStore`` delegation surface."""

from __future__ import annotations

import interloper as il
import pytest
from interloper_assets.demo.source import DemoSource
from sqlalchemy import create_engine

from interloper_db.drift import ComponentStatus
from interloper_db.store import Store

_SOURCE_KEY = DemoSource.key
_ASSET_KEY = DemoSource.asset_types[0].key
_ENABLED = il.Catalog.from_assets([DemoSource])
_EMPTY = il.Catalog(components={})


def _store(catalog: il.Catalog) -> Store:
    """A store resolving drift against *catalog*.

    Args:
        catalog: The catalog stored keys are resolved against.

    Returns:
        The store whose ``drift`` facet the tests exercise. Its engine is
        never connected to: drift resolution reads no rows.
    """
    return Store(catalog=catalog, engine=create_engine("sqlite://"))


def test_drift_delegates_to_resolver() -> None:
    store = _store(_ENABLED)
    assert store.drift.source_status(_SOURCE_KEY) is ComponentStatus.OK
    assert store.drift.asset_status(_ASSET_KEY, source_key=_SOURCE_KEY) is ComponentStatus.OK


@pytest.mark.parametrize("key", ["definitely_not_a_real_component_key"])
def test_drift_reports_missing_against_real_universe(key: str) -> None:
    # No discovered override: resolves against the real installed universe.
    assert _store(_EMPTY).drift.source_status(key) is ComponentStatus.MISSING
