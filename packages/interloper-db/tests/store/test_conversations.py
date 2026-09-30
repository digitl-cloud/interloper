"""Tests for ``interloper_db.store.conversations``."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from interloper.errors import NotFoundError

from interloper_db.store import Store


def _member(store: Store) -> tuple[UUID, UUID]:
    profile = store.auth.upsert_profile(google_id=f"g-{uuid4().hex}", email=f"{uuid4().hex}@example.com", name="U")
    org = store.organisations.create(name="Acme", creator_id=profile.id)
    return org.id, profile.id


class TestConversations:
    def test_a_member_creates_lists_and_reads_their_own(self, store: Store):
        org_id, user_id = _member(store)
        first = store.conversations.create(org_id, user_id)
        second = store.conversations.create(org_id, user_id)

        assert store.conversations.get(first.id, org_id=org_id, user_id=user_id).messages == []
        assert [c.id for c in store.conversations.list_all(org_id, user_id)] == [second.id, first.id]

    def test_another_member_or_org_reads_it_as_missing(self, store: Store):
        org_id, user_id = _member(store)
        other_org, other_user = _member(store)
        row = store.conversations.create(org_id, user_id)

        with pytest.raises(NotFoundError, match=f"{row.id} not found"):
            store.conversations.get(row.id, org_id=org_id, user_id=other_user)
        with pytest.raises(NotFoundError):
            store.conversations.get(row.id, org_id=other_org, user_id=user_id)
        assert store.conversations.list_all(org_id, other_user) == []

    def test_save_replaces_the_history_and_titles_once(self, store: Store):
        org_id, user_id = _member(store)
        row = store.conversations.create(org_id, user_id)

        store.conversations.save(row.id, [{"kind": "request"}], title="x" * 100)
        saved = store.conversations.save(row.id, [{"kind": "request"}, {"kind": "response"}], title="ignored")

        assert saved.messages == [{"kind": "request"}, {"kind": "response"}]
        assert saved.title == "x" * 80

    def test_delete_and_the_missing_row_contract(self, store: Store):
        org_id, user_id = _member(store)
        row = store.conversations.create(org_id, user_id)

        store.conversations.delete(row.id)

        with pytest.raises(NotFoundError):
            store.conversations.delete(row.id)
        with pytest.raises(NotFoundError):
            store.conversations.save(row.id, [])
