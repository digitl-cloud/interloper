"""Tests for the ``DriftMixin`` delegation surface."""

from __future__ import annotations

import interloper as il
import pytest
from interloper_assets.demo.source import DemoSource

from interloper_db.drift import ComponentStatus
from interloper_db.store.drift import DriftMixin

_SOURCE_KEY = DemoSource.key
_ASSET_KEY = DemoSource.asset_types[0].key
_ENABLED = il.Catalog.from_assets([DemoSource])
_EMPTY = il.Catalog(components={})


class _Store(DriftMixin):
    def __init__(self, catalog: il.Catalog) -> None:
        self._catalog = catalog


def test_drift_mixin_delegates_to_resolver() -> None:
    store = _Store(_ENABLED)
    assert store.source_status(_SOURCE_KEY) is ComponentStatus.OK
    assert store.asset_status(_ASSET_KEY, source_key=_SOURCE_KEY) is ComponentStatus.OK


@pytest.mark.parametrize("key", ["definitely_not_a_real_component_key"])
def test_drift_mixin_reports_missing_against_real_universe(key: str) -> None:
    # No discovered override: resolves against the real installed universe.
    assert _Store(_EMPTY).source_status(key) is ComponentStatus.MISSING
