"""Tests for the shared session policy (``interloper_db.store.base``)."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import interloper as il
import pytest
from cryptography.fernet import Fernet
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db.models import Component
from interloper_db.store import Store
from interloper_db.store import base as base_module

_ORG = uuid4()


@pytest.fixture
def store(component_db: Engine) -> Store:
    """A store over the in-memory database.

    Returns:
        A store with an empty catalog, reading and writing the fixture database.
    """
    return Store(catalog=il.Catalog(components={}))


def _count(engine: Engine) -> int:
    """Count the component rows visible to a fresh session.

    Args:
        engine: The engine to read through.

    Returns:
        The number of committed component rows.
    """
    with Session(engine) as session:
        return len(session.exec(select(Component)).all())


def _make(store: Store, key: str) -> Component:
    """Create a plaintext connection component.

    Args:
        store: The store to create through.
        key: The component key.

    Returns:
        The created row.
    """
    return store.components.create(org_id=_ORG, kind="connection", key=key, config={}, encrypted=False)


class TestTransaction:
    """Atomicity is asserted through rollback, not through durability.

    The fixture database is in-memory SQLite behind a ``StaticPool``, so every
    session shares one connection and an uncommitted write is already visible
    to the next reader. What a transaction guarantees here is that a failed
    block leaves nothing behind.
    """

    def test_call_outside_a_transaction_commits_on_its_own(self, store: Store, component_db: Engine):
        _make(store, "a")
        assert _count(component_db) == 1

    def test_completed_block_commits_every_call(self, store: Store, component_db: Engine):
        with store.transaction():
            _make(store, "a")
            _make(store, "b")
        assert _count(component_db) == 2

    def test_error_rolls_back_the_whole_block(self, store: Store, component_db: Engine):
        with pytest.raises(RuntimeError), store.transaction():
            _make(store, "a")
            _make(store, "b")
            raise RuntimeError("boom")
        assert _count(component_db) == 0

    def test_reads_see_writes_made_earlier_in_the_block(self, store: Store):
        with store.transaction():
            created = _make(store, "a")
            assert store.components.get(created.id).key == "a"

    def test_nested_block_joins_the_outer_one(self, store: Store, component_db: Engine):
        # An inner block must not commit: the outermost one owns the outcome.
        with pytest.raises(RuntimeError), store.transaction():
            _make(store, "a")
            with store.transaction():
                _make(store, "b")
            raise RuntimeError("boom")
        assert _count(component_db) == 0


class TestFromSettings:
    """The canonical constructor every long-lived process uses."""

    @pytest.fixture
    def settings(self, monkeypatch: pytest.MonkeyPatch, component_db: Engine):
        """Wire settings and the process engine for ``from_settings``.

        Args:
            monkeypatch: Fixture used to swap the settings and engine lookups.
            component_db: The in-memory engine ``from_settings`` should find.

        Returns:
            A callable taking the encryption key and returning the quota
            settings object it installed.
        """

        def install(encryption_key: str | None):
            quota = SimpleNamespace(max_sources=7)
            monkeypatch.setattr(
                "interloper.settings.AppSettings.get",
                classmethod(
                    lambda cls: SimpleNamespace(
                        secrets=SimpleNamespace(encryption_key=encryption_key), quota=quota
                    )
                ),
            )
            monkeypatch.setattr(base_module, "engine_from_settings", lambda: component_db)
            monkeypatch.setattr(
                il.Catalog, "from_settings", classmethod(lambda cls: il.Catalog(components={}))
            )
            return quota

        return install

    def test_a_configured_key_attaches_the_cipher(self, settings) -> None:
        settings(Fernet.generate_key().decode())

        store = Store.from_settings()

        assert store._encrypt is not None
        assert store._decrypt is not None

    def test_the_cipher_round_trips(self, settings) -> None:
        settings(Fernet.generate_key().decode())

        store = Store.from_settings()

        encrypt, decrypt = store._encrypt, store._decrypt
        assert encrypt is not None
        assert decrypt is not None
        assert decrypt(encrypt(b"secret")) == b"secret"

    def test_no_key_leaves_the_store_without_a_cipher(
        self, settings, caplog: pytest.LogCaptureFixture
    ) -> None:
        # Resource persistence then fails closed rather than writing plaintext.
        settings(None)

        with caplog.at_level("WARNING", logger="interloper_db.store.base"):
            store = Store.from_settings()

        assert store._encrypt is None
        assert store._decrypt is None
        assert "INTERLOPER_ENCRYPTION_KEY is not configured" in caplog.text
        assert "fail closed" in caplog.text

    def test_the_quota_defaults_are_carried_through(self, settings) -> None:
        quota = settings(Fernet.generate_key().decode())

        assert Store.from_settings()._quota_defaults is quota

    def test_the_quota_defaults_are_carried_through_without_a_key(self, settings) -> None:
        quota = settings(None)

        assert Store.from_settings()._quota_defaults is quota

    def test_the_engine_comes_from_the_process(self, settings, component_db: Engine) -> None:
        settings(None)

        assert Store.from_settings().engine is component_db

    def test_an_explicit_catalog_wins_over_the_configured_one(self, settings) -> None:
        settings(None)
        catalog = il.Catalog(components={})

        assert Store.from_settings(catalog=catalog)._catalog is catalog

    def test_without_one_the_configured_catalog_is_used(
        self, settings, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        settings(None)
        configured = il.Catalog(components={})
        monkeypatch.setattr(il.Catalog, "from_settings", classmethod(lambda cls: configured))

        assert Store.from_settings()._catalog is configured


class TestEngineProperty:
    """The engine is exposed so co-located SQL runs against the same one."""

    def test_it_reports_the_stores_engine(self, store: Store, component_db: Engine) -> None:
        assert store.engine is component_db

    def test_an_explicit_engine_wins_over_the_process_one(self, component_db: Engine) -> None:
        store = Store(catalog=il.Catalog(components={}), engine=component_db)

        assert store.engine is component_db


class TestFacets:
    """Every documented facet is wired at construction."""

    @pytest.mark.parametrize(
        "facet",
        ["auth", "organisations", "tokens", "relations", "quotas", "events", "runs", "components"],
    )
    def test_the_facet_is_present(self, store: Store, facet: str) -> None:
        assert getattr(store, facet) is not None

    def test_every_facet_shares_the_stores_engine(self, store: Store, component_db: Engine) -> None:
        # Otherwise a facet would read a different database than its siblings.
        for facet in ("auth", "organisations", "relations", "events", "runs", "components"):
            assert getattr(store, facet)._engine is component_db
