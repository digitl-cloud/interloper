"""Tests for ``interloper_db.store.conversations``."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from interloper.errors import NotFoundError
from sqlmodel import Session

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
                (first.id, datetime(2026, 1, 1, tzinfo=timezone.utc)),
                (second.id, datetime(2026, 1, 2, tzinfo=timezone.utc)),
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
