"""Tests for the catalog-drift resolution primitive (``interloper_db.drift``).

These exercise the pure resolver functions against constructed catalogs — the
single source of truth that hydration and detection both consume — plus the
The ``DemoSource`` fixture is a real catalog
component, so resolution goes through the actual import path it would in prod.
"""

from __future__ import annotations

import interloper as il
from interloper_assets.demo.source import DemoSource

from interloper_db.drift import (
    ComponentStatus,
    asset_status,
    resolve_source_cls,
    source_status,
)

_SOURCE_KEY = DemoSource.key
_ASSET_KEY = DemoSource.asset_types[0].key

# Enabled = what this deployment exposes; discovered = the full code universe.
_ENABLED = il.Catalog.from_assets([DemoSource])
_EMPTY = il.Catalog(components={})


# -- source_status ------------------------------------------------------------


def test_source_status_ok_when_in_enabled_catalog() -> None:
    assert source_status(_ENABLED, _SOURCE_KEY, discovered=_EMPTY) is ComponentStatus.OK


def test_source_status_disabled_when_in_discovered_but_not_enabled() -> None:
    # Key exists in code (discovered) but the deployment didn't enable it.
    assert source_status(_EMPTY, _SOURCE_KEY, discovered=_ENABLED) is ComponentStatus.DISABLED


def test_source_status_missing_when_in_neither() -> None:
    assert source_status(_EMPTY, "gone_from_code", discovered=_EMPTY) is ComponentStatus.MISSING


# -- asset_status: standalone -------------------------------------------------


def test_standalone_asset_status_resolves_like_a_source() -> None:
    assert asset_status(_ENABLED, _SOURCE_KEY, discovered=_EMPTY) is ComponentStatus.OK
    assert asset_status(_EMPTY, "gone", discovered=_EMPTY) is ComponentStatus.MISSING


# -- asset_status: source-owned -----------------------------------------------


def test_owned_asset_status_ok_when_key_in_source_asset_types() -> None:
    status = asset_status(_ENABLED, _ASSET_KEY, source_key=_SOURCE_KEY, discovered=_EMPTY)
    assert status is ComponentStatus.OK


def test_owned_asset_status_missing_when_key_drifted_out_of_source() -> None:
    # Source is live, but the asset key is no longer one of its asset_types.
    status = asset_status(_ENABLED, "renamed_away", source_key=_SOURCE_KEY, discovered=_EMPTY)
    assert status is ComponentStatus.MISSING


def test_owned_asset_status_cascades_missing_parent() -> None:
    status = asset_status(_EMPTY, _ASSET_KEY, source_key="gone", discovered=_EMPTY)
    assert status is ComponentStatus.MISSING


def test_owned_asset_status_cascades_disabled_parent() -> None:
    status = asset_status(_EMPTY, _ASSET_KEY, source_key=_SOURCE_KEY, discovered=_ENABLED)
    assert status is ComponentStatus.DISABLED


# -- resolve_source_cls -------------------------------------------------------


def test_resolve_source_cls_returns_class_when_present() -> None:
    assert resolve_source_cls(_ENABLED, _SOURCE_KEY) is DemoSource


def test_resolve_source_cls_returns_none_when_absent() -> None:
    assert resolve_source_cls(_EMPTY, _SOURCE_KEY) is None
