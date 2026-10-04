"""Tests for the membership store (``interloper_db.store.members``)."""

from __future__ import annotations

from uuid import uuid4

import pytest
from interloper.errors import ConfigError, NotFoundError

from interloper_db.store import PageQuery, Store


class TestRole:
    def test_the_creator_is_an_admin(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        assert store.members.role(org.id, profile.id) == "admin"

    def test_a_non_member_has_no_role(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        other = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        assert store.members.role(org.id, other.id) is None


class TestList:
    def test_members_are_listed_with_their_roles(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.members.add(org.id, bob.id, "editor")

        page = store.members.list(org.id, PageQuery())

        roles = {membership.profile.email: membership.role for membership in page.items if membership.profile}
        assert roles == {"ada@x": "admin", "bob@x": "editor"}
        assert page.total == 2

    def test_an_organisation_with_no_members_lists_nothing(self, store: Store):
        page = store.members.list(uuid4(), PageQuery())

        assert page.items == []
        assert page.total == 0


class TestCountByOrg:
    """Member counts for many organisations in one query."""

    def test_counts_members_per_organisation(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        busy = store.organisations.create(name="Busy", creator_id=ada.id)
        store.members.add(busy.id, bob.id, "viewer")
        quiet = store.organisations.create(name="Quiet", creator_id=ada.id)

        assert store.members.count_by_org([busy.id, quiet.id]) == {busy.id: 2, quiet.id: 1}

    def test_an_organisation_with_no_members_is_absent(self, store: Store):
        # A soft-deleted org has its memberships purged but stays in the ledger.
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        store.organisations.delete(org.id)

        assert store.members.count_by_org([org.id]) == {}

    def test_only_the_asked_organisations_are_counted(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        asked = store.organisations.create(name="Asked", creator_id=ada.id)
        store.organisations.create(name="Other", creator_id=ada.id)

        assert store.members.count_by_org([asked.id]) == {asked.id: 1}

    def test_no_organisations_is_an_empty_mapping(self, store: Store):
        assert store.members.count_by_org([]) == {}


class TestAdd:
    def test_adding_a_member_reports_whether_it_was_new(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)

        assert store.members.add(org.id, bob.id, "viewer") is True
        assert store.members.add(org.id, bob.id, "viewer") is False

    def test_an_unknown_role_is_refused(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)

        with pytest.raises(ConfigError, match="Unknown role 'owner'"):
            store.members.add(org.id, uuid4(), "owner")


class TestUpdate:
    def test_a_role_can_be_changed(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.members.add(org.id, bob.id, "viewer")

        store.members.update(org.id, bob.id, "admin")

        assert store.members.role(org.id, bob.id) == "admin"

    def test_returns_the_membership_with_its_profile(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.members.add(org.id, bob.id, "viewer")

        membership = store.members.update(org.id, bob.id, "editor")

        # Read after the session closed: the profile must already be loaded.
        assert membership.role == "editor"
        assert membership.organisation_id == org.id
        assert membership.profile is not None
        assert membership.profile.email == "bob@x"

    def test_changing_a_non_members_role_raises(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        stranger = uuid4()

        with pytest.raises(NotFoundError, match=f"User {stranger} is not a member"):
            store.members.update(org.id, stranger, "admin")

    def test_an_unknown_role_is_refused(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)

        with pytest.raises(ConfigError, match="Unknown role 'owner'"):
            store.members.update(org.id, ada.id, "owner")
        assert store.members.role(org.id, ada.id) == "admin"


class TestDelete:
    def test_a_member_can_be_removed(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.members.add(org.id, bob.id, "viewer")

        store.members.delete(org.id, bob.id)

        assert store.members.role(org.id, bob.id) is None

    def test_removing_a_non_member_raises(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        stranger = uuid4()

        with pytest.raises(NotFoundError, match=f"User {stranger} is not a member"):
            store.members.delete(org.id, stranger)
