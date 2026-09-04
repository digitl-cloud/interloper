"""Tests for ``interloper_api.dependencies.state``.

The module holds process-wide state, so every test restores whatever it
found — otherwise a setter here reconfigures the suites that run later.
"""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace

import pytest

from interloper_api.dependencies import state


@pytest.fixture(autouse=True)
def restore_state() -> Iterator[None]:
    """Put every module-level slot back the way the test found it.

    Yields:
        ``None``; the teardown restores the saved values.
    """
    saved = (
        state._store,
        state._catalog,
        state._auth_config,
        state._smtp_config,
        state._features,
        state._admin_config,
        state._quota_defaults,
    )
    yield
    (
        state._store,
        state._catalog,
        state._auth_config,
        state._smtp_config,
        state._features,
        state._admin_config,
        state._quota_defaults,
    ) = saved


class TestRequiredSlots:
    """Store, catalog and auth config must be installed before any request."""

    def test_the_store_round_trips(self) -> None:
        store = SimpleNamespace(name="store")

        state.set_store(store)  # ty: ignore[invalid-argument-type]

        assert state.get_store() is store

    def test_an_uninitialized_store_is_an_actionable_error(self) -> None:
        state._store = None

        with pytest.raises(RuntimeError, match=r"Store not initialized. Call set_store\(\) first."):
            state.get_store()

    def test_the_catalog_round_trips(self) -> None:
        catalog = SimpleNamespace(name="catalog")

        state.set_catalog(catalog)  # ty: ignore[invalid-argument-type]

        assert state.get_catalog() is catalog

    def test_an_uninitialized_catalog_is_an_actionable_error(self) -> None:
        state._catalog = None

        with pytest.raises(RuntimeError, match=r"Catalog not initialized. Call set_catalog\(\) first."):
            state.get_catalog()

    def test_the_auth_config_round_trips(self) -> None:
        auth_config = SimpleNamespace(google_client_id="cid")

        state.set_auth_config(auth_config)

        assert state.get_auth_config() is auth_config

    def test_an_uninitialized_auth_config_is_an_actionable_error(self) -> None:
        state._auth_config = None

        with pytest.raises(RuntimeError, match=r"Auth config not initialized. Call set_auth_config\(\) first."):
            state.get_auth_config()


class TestOptionalSlots:
    """The rest degrade to a falsy default rather than raising."""

    def test_the_smtp_config_round_trips(self) -> None:
        smtp = SimpleNamespace(host="smtp.example.com")

        state.set_smtp_config(smtp)

        assert state.get_smtp_config() is smtp

    def test_an_unset_smtp_config_is_none(self) -> None:
        state._smtp_config = None

        assert state.get_smtp_config() is None

    def test_the_feature_flags_round_trip(self) -> None:
        state.set_features({"agent": True, "mcp": False})

        assert state.get_features() == {"agent": True, "mcp": False}

    def test_unset_feature_flags_are_empty(self) -> None:
        state._features = {}

        assert state.get_features() == {}

    def test_the_admin_config_snapshot_round_trips(self) -> None:
        snapshot = SimpleNamespace(launcher={"type": "kubernetes"})

        state.set_admin_config(snapshot)

        assert state.get_admin_config() is snapshot

    def test_an_unset_admin_config_is_none(self) -> None:
        state._admin_config = None

        assert state.get_admin_config() is None

    def test_the_quota_defaults_round_trip(self) -> None:
        defaults = SimpleNamespace(max_sources=10)

        state.set_quota_defaults(defaults)

        assert state.get_quota_defaults() is defaults

    def test_unset_quota_defaults_are_none(self) -> None:
        state._quota_defaults = None

        assert state.get_quota_defaults() is None
