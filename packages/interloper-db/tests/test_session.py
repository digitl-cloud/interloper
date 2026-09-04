"""Tests for ``interloper_db.session``.

The session policy is exercised through the ``Store`` in
``tests/store/test_base.py``; this covers the primitives directly, including
the dialect dispatch that keeps every upsert portable.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import Engine
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlmodel import Session, select

from interloper_db.models import Component
from interloper_db.session import commit, dialect_insert, session_scope, transaction

_ORG = uuid4()


class TestDialectInsert:
    """``on_conflict_do_update`` is dialect-specific, so the constructor is chosen."""

    def test_sqlite_gets_the_sqlite_constructor(self, component_db: Engine):
        with Session(component_db) as session:
            assert dialect_insert(session) is sqlite_insert

    def test_postgres_gets_the_postgres_constructor(self, component_db: Engine, monkeypatch):
        # The store's upserts must compile to ON CONFLICT on the real backend.
        with Session(component_db) as session:
            bind = session.get_bind()
            monkeypatch.setattr(type(bind.dialect), "name", "postgresql", raising=False)

            assert dialect_insert(session) is postgresql_insert


class TestSessionScope:
    """A standalone call opens its own session and commits on its own."""

    def test_a_write_lands(self, component_db: Engine):
        with session_scope(component_db) as session:
            session.add(Component(org_id=_ORG, kind="job", key="cron_job"))
            commit(session)

        with Session(component_db) as session:
            assert len(session.exec(select(Component)).all()) == 1

    def test_nested_scopes_share_one_session(self, component_db: Engine):
        with session_scope(component_db) as outer, session_scope(component_db) as inner:
            assert inner is outer


class TestTransaction:
    """An explicit block makes several calls one atomic unit."""

    def test_the_block_commits_once_completed(self, component_db: Engine):
        with transaction(component_db) as session:
            session.add(Component(org_id=_ORG, kind="job", key="a"))
            session.add(Component(org_id=_ORG, kind="job", key="b"))

        with Session(component_db) as session:
            assert len(session.exec(select(Component)).all()) == 2

    def test_an_error_rolls_the_whole_block_back(self, component_db: Engine):
        with pytest.raises(RuntimeError, match="boom"), transaction(component_db) as session:
            session.add(Component(org_id=_ORG, kind="job", key="a"))
            raise RuntimeError("boom")

        with Session(component_db) as session:
            assert session.exec(select(Component)).all() == []

    def test_a_call_inside_the_block_joins_it(self, component_db: Engine):
        with transaction(component_db) as outer, session_scope(component_db) as inner:
            assert inner is outer
