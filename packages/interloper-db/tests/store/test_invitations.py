"""Tests for the invitation store (``interloper_db.store.invitations``)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from interloper.errors import ConfigError, NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session as SQLSession

from interloper_db.models import Invitation
from interloper_db.store import PageQuery, Store


class TestCreateListGetDelete:
    """Creation, listing, lookup and deletion, all within an organisation."""

    def test_a_created_invitation_carries_a_token(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)

        invitation = store.invitations.create(org.id, email="new@x", role="editor", invited_by=ada.id)

        assert invitation.token
        assert invitation.email == "new@x"
        assert invitation.role == "editor"

    def test_invitations_are_listed_per_organisation(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        mine = store.organisations.create(name="Mine", creator_id=ada.id)
        theirs = store.organisations.create(name="Theirs", creator_id=ada.id)
        store.invitations.create(mine.id, email="a@x", role="viewer", invited_by=ada.id)
        store.invitations.create(theirs.id, email="b@x", role="viewer", invited_by=ada.id)

        page = store.invitations.list(mine.id, PageQuery())

        assert [invitation.email for invitation in page.items] == ["a@x"]
        assert page.total == 1

    def test_an_organisation_with_no_invitations_lists_nothing(self, store: Store):
        assert store.invitations.list(uuid4(), PageQuery()).items == []

    def test_an_invitation_is_read_within_its_organisation(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        invitation = store.invitations.create(org.id, email="new@x", role="viewer", invited_by=ada.id)

        assert store.invitations.get(invitation.id, org_id=org.id).email == "new@x"

    def test_another_organisations_invitation_reads_as_missing(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        mine = store.organisations.create(name="Mine", creator_id=ada.id)
        theirs = store.organisations.create(name="Theirs", creator_id=ada.id)
        invitation = store.invitations.create(theirs.id, email="new@x", role="viewer", invited_by=ada.id)

        with pytest.raises(NotFoundError, match=f"Invitation {invitation.id} not found"):
            store.invitations.get(invitation.id, org_id=mine.id)
        with pytest.raises(NotFoundError):
            store.invitations.delete(invitation.id, org_id=mine.id)
        assert len(store.invitations.list(theirs.id, PageQuery()).items) == 1

    def test_an_invitation_can_be_deleted(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        invitation = store.invitations.create(org.id, email="new@x", role="viewer", invited_by=ada.id)

        store.invitations.delete(invitation.id, org_id=org.id)

        assert store.invitations.list(org.id, PageQuery()).items == []

    def test_deleting_a_missing_invitation_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Invitation {missing} not found"):
            store.invitations.delete(missing, org_id=uuid4())

    def test_an_unknown_role_is_refused(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)

        with pytest.raises(ConfigError, match="Unknown role 'owner'"):
            store.invitations.create(org.id, email="new@x", role="owner", invited_by=ada.id)
        assert store.invitations.list(org.id, PageQuery()).items == []


class TestReissue:
    """A fresh link for the same address and role, the old one revoked."""

    def test_the_old_token_stops_working(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        original = store.invitations.create(org.id, email="bob@x", role="editor", invited_by=ada.id)

        reissued = store.invitations.reissue(original.id, org_id=org.id, invited_by=ada.id)

        assert reissued.token != original.token
        assert store.invitations.accept(original.token, bob.id) is None
        assert store.invitations.accept(reissued.token, bob.id) is not None
        assert store.members.role(org.id, bob.id) == "editor"

    def test_keeps_the_address_and_role_under_a_new_id(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        carol = store.profiles.upsert(google_id="g3", email="carol@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        original = store.invitations.create(org.id, email="bob@x", role="editor", invited_by=ada.id)

        reissued = store.invitations.reissue(original.id, org_id=org.id, invited_by=carol.id)

        assert reissued.id != original.id
        assert reissued.email == "bob@x"
        assert reissued.role == "editor"
        assert reissued.invited_by == carol.id
        assert [invitation.id for invitation in store.invitations.list(org.id, PageQuery()).items] == [reissued.id]
        with pytest.raises(NotFoundError):
            store.invitations.get(original.id, org_id=org.id)

    def test_another_organisations_invitation_reads_as_missing(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        mine = store.organisations.create(name="Mine", creator_id=ada.id)
        theirs = store.organisations.create(name="Theirs", creator_id=ada.id)
        invitation = store.invitations.create(theirs.id, email="new@x", role="viewer", invited_by=ada.id)

        with pytest.raises(NotFoundError, match=f"Invitation {invitation.id} not found"):
            store.invitations.reissue(invitation.id, org_id=mine.id, invited_by=ada.id)
        assert store.invitations.get(invitation.id, org_id=theirs.id).token == invitation.token


class TestAccept:
    def test_accept_adds_membership_and_returns_usable_org(self, store: Store):
        admin = store.profiles.upsert(google_id="g-admin", email="admin@example.com", name="Admin")
        invitee = store.profiles.upsert(google_id="g-invitee", email="new@example.com", name="New")
        org = store.organisations.create(name="Acme", creator_id=admin.id)
        invitation = store.invitations.create(org.id, email=invitee.email, role="editor", invited_by=admin.id)

        joined = store.invitations.accept(invitation.token, invitee.id)

        assert joined is not None
        # Attributes must be loaded on the detached instance (regression:
        # expunging the commit-expired org made any access raise
        # DetachedInstanceError).
        assert joined.id == org.id
        assert joined.name == "Acme"
        assert store.members.role(org.id, invitee.id) == "editor"
        assert store.invitations.list(org.id, PageQuery()).items == []

    def test_accept_invalid_token_returns_none(self, store: Store):
        invitee = store.profiles.upsert(google_id="g-invitee", email="new@example.com", name="New")

        assert store.invitations.accept("no-such-token", invitee.id) is None

    def test_an_expired_invitation_is_refused_and_swept(self, store: Store, auth_db: Engine):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        invitation = store.invitations.create(org.id, email="bob@x", role="viewer", invited_by=ada.id)
        with SQLSession(auth_db) as session:
            row = session.get(Invitation, invitation.id)
            assert row is not None
            row.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
            session.add(row)
            session.commit()

        assert store.invitations.accept(invitation.token, bob.id) is None
        # The stale row is deleted rather than left to accumulate.
        assert store.invitations.list(org.id, PageQuery()).items == []

    def test_accepting_twice_leaves_one_membership(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        first = store.invitations.create(org.id, email="bob@x", role="viewer", invited_by=ada.id)
        store.invitations.accept(first.token, bob.id)
        second = store.invitations.create(org.id, email="bob@x", role="admin", invited_by=ada.id)

        store.invitations.accept(second.token, bob.id)

        members = store.members.list(org.id, PageQuery(limit=None)).items
        assert len([membership for membership in members if membership.user_id == bob.id]) == 1
        # The existing membership is left as it was, not re-roled.
        assert store.members.role(org.id, bob.id) == "viewer"


class TestHasPending:
    def _invite(self, store: Store, email: str) -> Invitation:
        """Invite *email* to a fresh organisation.

        Args:
            store: The store under test.
            email: Address to invite.

        Returns:
            The created invitation.
        """
        admin = store.profiles.upsert(google_id="g-admin", email="admin@example.com", name="Admin")
        org = store.organisations.create(name="Acme", creator_id=admin.id)
        return store.invitations.create(org.id, email=email, role="viewer", invited_by=admin.id)

    def test_pending_invitation_matches_case_insensitively(self, store: Store):
        self._invite(store, "New@Example.com")

        assert store.invitations.has_pending("new@example.com")

    def test_no_invitation_returns_false(self, store: Store):
        assert not store.invitations.has_pending("nobody@example.com")

    def test_expired_invitation_returns_false(self, store: Store, auth_db: Engine):
        invitation = self._invite(store, "new@example.com")

        with SQLSession(auth_db) as session:
            db_invitation = session.get(Invitation, invitation.id)
            assert db_invitation is not None
            db_invitation.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
            session.add(db_invitation)
            session.commit()

        assert not store.invitations.has_pending("new@example.com")
