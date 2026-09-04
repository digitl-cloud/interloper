"""Tests for catalog-status resolution of bare component keys."""

from __future__ import annotations

import interloper as il
import pytest
from interloper_assets.demo.source import DemoSource

from interloper_db.store.status import ComponentStatus, asset_status, source_status

_SOURCE_KEY = DemoSource.key
_ASSET_KEY = DemoSource.asset_types[0].key
_ENABLED = il.Catalog.from_assets([DemoSource])
_EMPTY = il.Catalog(components={})


def test_missing_against_the_real_universe() -> None:
    # No discovered override: resolves against the real installed universe.
    assert source_status(_EMPTY, "definitely_not_a_real_component_key") is ComponentStatus.MISSING


def test_source_status_ok_when_in_enabled_catalog() -> None:
    assert source_status(_ENABLED, _SOURCE_KEY, discovered=_EMPTY) is ComponentStatus.OK


def test_source_status_disabled_when_in_discovered_but_not_enabled() -> None:
    # Key exists in code (discovered) but the deployment didn't enable it.
    assert source_status(_EMPTY, _SOURCE_KEY, discovered=_ENABLED) is ComponentStatus.DISABLED


def test_source_status_missing_when_in_neither() -> None:
    assert source_status(_EMPTY, "gone_from_code", discovered=_EMPTY) is ComponentStatus.MISSING


def test_standalone_asset_status_resolves_like_a_source() -> None:
    assert asset_status(_ENABLED, _SOURCE_KEY, discovered=_EMPTY) is ComponentStatus.OK
    assert asset_status(_EMPTY, "gone", discovered=_EMPTY) is ComponentStatus.MISSING


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


def test_an_enabled_source_whose_class_vanished_reads_as_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The defensive arm: the catalog said ok but the class did not resolve.

    Unreachable through normal operation — the catalog is built from importable
    classes — so the resolution is broken deliberately to prove the fallback
    reports ``missing`` rather than raising into the caller.
    """
    catalog = il.Catalog.from_assets([DemoSource])

    def unresolvable(key: str, catalog: object) -> None:
        raise ImportError("class went away")

    monkeypatch.setattr(
        il.Source, "resolve_key", classmethod(lambda cls, key, catalog: unresolvable(key, catalog))
    )

    assert asset_status(catalog, _ASSET_KEY, source_key=_SOURCE_KEY) is ComponentStatus.MISSING
