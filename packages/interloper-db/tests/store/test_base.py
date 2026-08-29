"""Tests for the shared session policy (``interloper_db.store.base``)."""

from __future__ import annotations

from uuid import uuid4

import interloper as il
import pytest
from sqlalchemy import Engine
from sqlmodel import Session, select

from interloper_db.models import Component
from interloper_db.store import Store

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
