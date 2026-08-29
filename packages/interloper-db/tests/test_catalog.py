"""Tests for catalog resolution (``interloper_db.catalog``).

These exercise the pure resolver functions against constructed catalogs — the
single source of truth that hydration and detection both consume — plus the
The ``DemoSource`` fixture is a real catalog
component, so resolution goes through the actual import path it would in prod.
"""

from __future__ import annotations

import interloper as il
from interloper_assets.demo.source import DemoSource

from interloper_db.catalog import resolve_source_cls

_SOURCE_KEY = DemoSource.key
_ASSET_KEY = DemoSource.asset_types[0].key

# Enabled = what this deployment exposes; discovered = the full code universe.
_ENABLED = il.Catalog.from_assets([DemoSource])
_EMPTY = il.Catalog(components={})


# -- source_status ------------------------------------------------------------


def test_resolve_source_cls_returns_class_when_present() -> None:
    assert resolve_source_cls(_ENABLED, _SOURCE_KEY) is DemoSource


def test_resolve_source_cls_returns_none_when_absent() -> None:
    assert resolve_source_cls(_EMPTY, _SOURCE_KEY) is None
