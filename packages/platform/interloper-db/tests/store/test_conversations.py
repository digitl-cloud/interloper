"""Tests for ``interloper_db.store.conversations``."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Engine, inspect, text
from sqlmodel import Session

from interloper_db import NotFoundError
from interloper_db.models import Conversation
from interloper_db.store import PageQuery, Store


def _member(store: Store) -> tuple[UUID, UUID]:
    profile = store.profiles.upsert(google_id=f"g-{uuid4().hex}", email=f"{uuid4().hex}@example.com", name="U")
    org = store.organisations.create(name="Acme", creator_id=profile.id)
    return org.id, profile.id


class TestConversations:
    def test_a_member_creates_lists_and_reads_their_own(self, store: Store):
        org_id, user_id = _member(store)
        first = store.conversations.create(org_id, user_id)
        second = store.conversations.create(org_id, user_id)
        # SQLite stamps whole seconds; set the clock apart so the order is the store's, not a tie-break.
        with Session(store.engine) as session:
            for row_id, moment in (
                (first.id, datetime(2026, 1, 1, tzinfo=UTC)),
                (second.id, datetime(2026, 1, 2, tzinfo=UTC)),
            ):
                row = session.get(Conversation, row_id)
                assert row is not None
                row.updated_at = moment
                session.add(row)
            session.commit()

        assert store.conversations.get(first.id, org_id=org_id, user_id=user_id).messages == []
        page = store.conversations.list(org_id, user_id, PageQuery())
        assert [c.id for c in page.items] == [second.id, first.id]
        assert page.total == 2

    def test_list_leaves_out_conversations_of_an_oauth_client(self, store: Store):
        org_id, user_id = _member(store)
        client, _ = store.oauth.register_client("Chat", ["https://chat.example.com/callback"], registered_by="operator")
        own = store.conversations.create(org_id, user_id)
        started_by_client = store.conversations.create(org_id, user_id, client_id=client.client_id)

        page = store.conversations.list(org_id, user_id, PageQuery())

        assert [c.id for c in page.items] == [own.id]
        assert page.total == 1
        assert (
            store.conversations.get(started_by_client.id, org_id=org_id, user_id=user_id).client_id == client.client_id
        )

    def test_another_member_or_org_reads_it_as_missing(self, store: Store):
        org_id, user_id = _member(store)
        other_org, other_user = _member(store)
        row = store.conversations.create(org_id, user_id)

        with pytest.raises(NotFoundError, match=f"{row.id} not found"):
            store.conversations.get(row.id, org_id=org_id, user_id=other_user)
        with pytest.raises(NotFoundError):
            store.conversations.get(row.id, org_id=other_org, user_id=user_id)
        assert store.conversations.list(org_id, other_user, PageQuery()).items == []

    def test_update_replaces_the_history_and_titles_once(self, store: Store):
        org_id, user_id = _member(store)
        row = store.conversations.create(org_id, user_id)

        store.conversations.update(row.id, messages=[{"kind": "request"}], title="x" * 100)
        saved = store.conversations.update(
            row.id, messages=[{"kind": "request"}, {"kind": "response"}], title="ignored"
        )

        assert saved.messages == [{"kind": "request"}, {"kind": "response"}]
        assert saved.title == "x" * 80

    def test_update_leaves_an_omitted_history_alone(self, store: Store):
        org_id, user_id = _member(store)
        row = store.conversations.create(org_id, user_id)
        store.conversations.update(row.id, messages=[{"kind": "request"}])

        titled = store.conversations.update(row.id, title="First question")

        assert titled.messages == [{"kind": "request"}]
        assert titled.title == "First question"

    def test_delete_and_the_missing_row_contract(self, store: Store):
        org_id, user_id = _member(store)
        row = store.conversations.create(org_id, user_id)

        store.conversations.delete(row.id)

        with pytest.raises(NotFoundError):
            store.conversations.delete(row.id)
        with pytest.raises(NotFoundError):
            store.conversations.update(row.id, messages=[])


class TestMigration021:
    @staticmethod
    def _migrate(auth_db: Engine, step: str) -> None:
        """Run one step of migration 021 against the fixture database.

        Args:
            auth_db: The fixture database.
            step: ``"upgrade"`` or ``"downgrade"``.
        """
        migration = importlib.import_module("interloper_db.migrations.versions.021_conversation_client")
        with auth_db.begin() as connection, Operations.context(MigrationContext.configure(connection)):
            getattr(migration, step)()

    def test_a_round_trip_drops_the_client_conversations_and_restores_the_column(self, store: Store, auth_db: Engine):
        org_id, user_id = _member(store)
        client, _ = store.oauth.register_client("Chat", ["https://chat.example.com/callback"], registered_by="operator")
        own = store.conversations.create(org_id, user_id)
        store.conversations.create(org_id, user_id, client_id=client.client_id)

        self._migrate(auth_db, "downgrade")
        with auth_db.connect() as connection:
            remaining = connection.execute(text("SELECT id FROM conversations")).scalars().all()
        assert [UUID(str(row_id)) for row_id in remaining] == [own.id]
        assert "client_id" not in {column["name"] for column in inspect(auth_db).get_columns("conversations")}

        self._migrate(auth_db, "upgrade")
        self._migrate(auth_db, "upgrade")
        assert "client_id" in {column["name"] for column in inspect(auth_db).get_columns("conversations")}
        indexes = {index["name"] for index in inspect(auth_db).get_indexes("conversations")}
        assert "ix_conversations_client_updated" in indexes
        assert store.conversations.get(own.id, org_id=org_id, user_id=user_id).client_id is None
        started_by_client = store.conversations.create(org_id, user_id, client_id=client.client_id)
        assert store.conversations.get(started_by_client.id, org_id=org_id, user_id=user_id).client_id == (
            client.client_id
        )
