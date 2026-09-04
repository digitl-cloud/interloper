"""Tests for the organisation store (``interloper_db.store.organisations``)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from interloper.errors import NotFoundError
from sqlalchemy import Engine
from sqlmodel import Session as SQLSession
from sqlmodel import select

from interloper_db.models import (
    Backfill,
    Component,
    ComponentRelation,
    Event,
    Invitation,
    PersonalAccessToken,
    Profile,
    Run,
    UserOrganisation,
)
from interloper_db.store import Store


class TestDeleteOrganisation:
    def _seed_org_data(self, session: SQLSession, org_id) -> None:
        """Plant one row of every org-owned kind directly (no catalog needed)."""
        source = Component(org_id=org_id, kind="source", key="demo")
        asset = Component(org_id=org_id, kind="asset", key="demo.a", parent_id=source.id)
        session.add(source)
        session.add(asset)
        session.add(
            ComponentRelation(
                src_id=asset.id, dst_id=source.id, org_id=org_id, src_kind="asset", dst_kind="source", type="owner"
            )
        )
        backfill = Backfill(org_id=org_id, start_key="2026-01-01", end_key="2026-01-02")
        session.add(backfill)
        session.commit()
        run = Run(org_id=org_id, component_id=source.id)
        session.add(run)
        session.commit()
        session.add(Event(org_id=org_id, run_id=run.id, event_type="run_started", timestamp=datetime.now(timezone.utc)))
        session.commit()

    def test_purges_payload_but_keeps_the_ledger(self, store: Store, auth_db: Engine):
        admin = store.auth.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        org = store.organisations.create(name="Doomed", creator_id=admin.id)
        keeper = store.organisations.create(name="Keeper", creator_id=admin.id)
        store.organisations.add_member(org.id, admin.id, "admin")
        store.organisations.add_member(keeper.id, admin.id, "admin")
        store.organisations.create_invitation(
            org_id=org.id, email="new@example.com", role="viewer", invited_by=admin.id
        )
        store.tokens.create(user_id=admin.id, organisation_id=org.id, name="laptop")
        session_token = store.auth.create_session(user_id=admin.id, organisation_id=org.id)
        with SQLSession(auth_db) as session:
            self._seed_org_data(session, org.id)
            db_admin = session.get(Profile, admin.id)
            assert db_admin is not None
            db_admin.last_organisation_id = org.id
            session.add(db_admin)
            session.commit()

        store.organisations.delete(org.id)

        # The org reads as missing everywhere but the row survives, stamped.
        with pytest.raises(NotFoundError):
            store.organisations.get(org.id)
        assert store.organisations.get(org.id, include_deleted=True).deleted_at is not None
        with SQLSession(auth_db) as session:
            # Sensitive payload is purged...
            for model in (Component, ComponentRelation):
                assert session.exec(select(model).where(model.org_id == org.id)).first() is None
            assert (
                session.exec(select(UserOrganisation).where(UserOrganisation.organisation_id == org.id)).first()
                is None
            )
            assert session.exec(select(Invitation).where(Invitation.organisation_id == org.id)).first() is None
            assert (
                session.exec(select(PersonalAccessToken).where(PersonalAccessToken.organisation_id == org.id)).first()
                is None
            )
            # ...but execution history survives for billing, detached from
            # the purged components via the FK's SET NULL.
            surviving_run = session.exec(select(Run).where(Run.org_id == org.id)).one()
            assert surviving_run.component_id is None
            assert session.exec(select(Backfill).where(Backfill.org_id == org.id)).first() is not None
            assert session.exec(select(Event).where(Event.org_id == org.id)).first() is not None
        # The user, their session, and the other organisation survive; org refs are cleared.
        resolved = store.auth.resolve_session(session_token)
        assert resolved is not None
        profile, auth_session = resolved
        assert auth_session.organisation_id is None
        assert profile.last_organisation_id is None
        assert store.organisations.get(keeper.id).deleted_at is None
        assert store.organisations.member_role(admin.id, keeper.id) == "admin"

    def test_double_delete_reads_as_missing(self, store: Store):
        org = store.organisations.create(name="Once")
        store.organisations.delete(org.id)
        with pytest.raises(NotFoundError):
            store.organisations.delete(org.id)
        with pytest.raises(NotFoundError):
            store.organisations.update(org.id, "Renamed")

    def test_missing_organisation_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.organisations.delete(uuid4())


class TestOrganisationActivity:
    def test_composes_and_sorts_the_derived_feed(self, store: Store, auth_db: Engine):
        admin = store.auth.upsert_profile(google_id="g-act", email="act@example.com", name="Act Min")
        org = store.organisations.create(name="Busy", creator_id=admin.id)
        store.organisations.add_member(org.id, admin.id, "admin")
        store.organisations.create_invitation(
            org_id=org.id, email="new@example.com", role="viewer", invited_by=admin.id
        )
        with SQLSession(auth_db) as session:
            session.add(Component(org_id=org.id, kind="source", key="bing_ads", name="Bing"))
            session.add(
                Run(
                    id=uuid4(),
                    org_id=org.id,
                    status="success",
                    completed_at=datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc),
                )
            )
            session.add(
                Run(
                    id=uuid4(),
                    org_id=org.id,
                    status="success",
                    completed_at=datetime(2026, 8, 10, 13, 0, tzinfo=timezone.utc),
                )
            )
            session.add(Run(id=uuid4(), org_id=org.id, status="failed"))
            session.commit()

        entries = store.organisations.list_activity(org.id)

        kinds = [entry["kind"] for entry in entries]
        assert set(kinds) == {"org_created", "member_joined", "invitation_sent", "source_added", "runs_completed"}
        whens = [entry["when"] for entry in entries]
        assert whens == sorted(whens, reverse=True)
        assert all(when.tzinfo is not None for when in whens)
        joined = next(entry for entry in entries if entry["kind"] == "member_joined")
        assert joined["subject"] == "Act Min" and joined["extra"] == "admin"
        invited = next(entry for entry in entries if entry["kind"] == "invitation_sent")
        assert invited["subject"] == "new@example.com" and invited["extra"] == "Act Min"
        runs = next(entry for entry in entries if entry["kind"] == "runs_completed")
        assert runs["subject"] == "2"  # only the successful runs, aggregated per day

    def test_limit_caps_the_feed(self, store: Store):
        admin = store.auth.upsert_profile(google_id="g-cap", email="cap@example.com", name="Cap")
        org = store.organisations.create(name="Capped", creator_id=admin.id)
        store.organisations.add_member(org.id, admin.id, "admin")
        assert len(store.organisations.list_activity(org.id, limit=1)) == 1

    def test_unknown_org_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.organisations.list_activity(uuid4())


class TestAcceptInvitation:
    def test_accept_adds_membership_and_returns_usable_org(self, store: Store):
        admin = store.auth.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        invitee = store.auth.upsert_profile(google_id="g-invitee", email="new@example.com", name="New")
        org = store.organisations.create(name="Acme", creator_id=admin.id)
        invitation = store.organisations.create_invitation(
            org_id=org.id, email=invitee.email, role="member", invited_by=admin.id
        )

        joined = store.organisations.accept_invitation(invitation.token, invitee.id)

        assert joined is not None
        # Attributes must be loaded on the detached instance (regression:
        # expunging the commit-expired org made any access raise
        # DetachedInstanceError).
        assert joined.id == org.id
        assert joined.name == "Acme"
        assert store.organisations.member_role(invitee.id, org.id) == "member"
        assert store.organisations.get_invitation_by_token(invitation.token) is None

    def test_accept_invalid_token_returns_none(self, store: Store):
        invitee = store.auth.upsert_profile(google_id="g-invitee", email="new@example.com", name="New")

        assert store.organisations.accept_invitation("no-such-token", invitee.id) is None


class TestHasPendingInvitation:
    def _invite(self, store: Store, email: str) -> Invitation:
        admin = store.auth.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        org = store.organisations.create(name="Acme", creator_id=admin.id)
        return store.organisations.create_invitation(org_id=org.id, email=email, role="member", invited_by=admin.id)

    def test_pending_invitation_matches_case_insensitively(self, store: Store):
        self._invite(store, "New@Example.com")

        assert store.organisations.has_pending_invitation("new@example.com")

    def test_no_invitation_returns_false(self, store: Store):
        assert not store.organisations.has_pending_invitation("nobody@example.com")

    def test_expired_invitation_returns_false(self, store: Store, auth_db: Engine):
        invitation = self._invite(store, "new@example.com")

        with SQLSession(auth_db) as session:
            db_invitation = session.get(Invitation, invitation.id)
            assert db_invitation is not None
            db_invitation.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
            session.add(db_invitation)
            session.commit()

        assert not store.organisations.has_pending_invitation("new@example.com")


class TestUpdate:
    """Renaming, and the soft-delete guard on it."""

    def test_renames_the_organisation(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        renamed = store.organisations.update(org.id, name="Acme Corp")

        assert renamed.name == "Acme Corp"
        assert store.organisations.get(org.id).name == "Acme Corp"

    def test_a_missing_organisation_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Organisation {missing} not found"):
            store.organisations.update(missing, name="Acme")

    def test_a_soft_deleted_organisation_reads_as_missing(self, store: Store):
        # The ledger row survives the delete, but it is not writable.
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        store.organisations.delete(org.id)

        with pytest.raises(NotFoundError, match=f"Organisation {org.id} not found"):
            store.organisations.update(org.id, name="Acme Corp")


class TestListAll:
    """The super-admin listing pairs each organisation with its member count."""

    def test_counts_members_per_organisation(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        busy = store.organisations.create(name="Busy", creator_id=ada.id)
        store.organisations.add_member(busy.id, bob.id, "viewer")
        store.organisations.create(name="Quiet", creator_id=ada.id)

        counts = {org.name: count for org, count in store.organisations.list_all()}

        assert counts == {"Busy": 2, "Quiet": 1}

    def test_an_organisation_with_no_members_counts_zero(self, store: Store, auth_db: Engine):
        # A soft-deleted org has its memberships purged but stays in the ledger.
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        store.organisations.delete(org.id)

        counts = {org.name: count for org, count in store.organisations.list_all()}

        assert counts == {"Acme": 0}

    def test_no_organisations_is_an_empty_list(self, store: Store):
        assert store.organisations.list_all() == []


class TestListForUser:
    """Membership-scoped listing."""

    def test_lists_only_the_users_organisations(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        mine = store.organisations.create(name="Mine", creator_id=ada.id)
        store.organisations.create(name="Theirs", creator_id=bob.id)

        assert [org.id for org in store.organisations.list_for_user(ada.id)] == [mine.id]

    def test_a_user_with_no_memberships_gets_an_empty_list(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")

        assert store.organisations.list_for_user(profile.id) == []


class TestMembers:
    """Roles, listing, and the two mutations."""

    def test_the_creator_is_an_admin(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        assert store.organisations.member_role(profile.id, org.id) == "admin"

    def test_a_non_member_has_no_role(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        other = store.auth.upsert_profile(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        assert store.organisations.member_role(other.id, org.id) is None

    def test_members_are_listed_with_their_roles(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.organisations.add_member(org.id, bob.id, "editor")

        members = {profile.email: role for profile, role in store.organisations.list_members(org.id)}

        assert members == {"ada@x": "admin", "bob@x": "editor"}

    def test_an_organisation_with_no_members_lists_nothing(self, store: Store):
        assert store.organisations.list_members(uuid4()) == []

    def test_adding_a_member_reports_whether_it_was_new(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)

        assert store.organisations.add_member(org.id, bob.id, "viewer") is True
        assert store.organisations.add_member(org.id, bob.id, "viewer") is False

    def test_a_role_can_be_changed(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.organisations.add_member(org.id, bob.id, "viewer")

        store.organisations.update_member_role(org.id, bob.id, "admin")

        assert store.organisations.member_role(bob.id, org.id) == "admin"

    def test_changing_a_non_members_role_raises(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        stranger = uuid4()

        with pytest.raises(NotFoundError, match=f"User {stranger} is not a member"):
            store.organisations.update_member_role(org.id, stranger, "admin")

    def test_a_member_can_be_removed(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.organisations.add_member(org.id, bob.id, "viewer")

        store.organisations.remove_member(org.id, bob.id)

        assert store.organisations.member_role(bob.id, org.id) is None

    def test_removing_a_non_member_raises(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        stranger = uuid4()

        with pytest.raises(NotFoundError, match=f"User {stranger} is not a member"):
            store.organisations.remove_member(org.id, stranger)


class TestInvitations:
    """Creation, listing, token lookup and deletion."""

    def test_a_created_invitation_carries_a_token(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)

        invitation = store.organisations.create_invitation(
            org_id=org.id, email="new@x", role="editor", invited_by=ada.id
        )

        assert invitation.token
        assert invitation.email == "new@x"
        assert invitation.role == "editor"

    def test_invitations_are_listed_per_organisation(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        mine = store.organisations.create(name="Mine", creator_id=ada.id)
        theirs = store.organisations.create(name="Theirs", creator_id=ada.id)
        store.organisations.create_invitation(org_id=mine.id, email="a@x", role="viewer", invited_by=ada.id)
        store.organisations.create_invitation(org_id=theirs.id, email="b@x", role="viewer", invited_by=ada.id)

        assert [inv.email for inv in store.organisations.list_invitations(mine.id)] == ["a@x"]

    def test_an_organisation_with_no_invitations_lists_nothing(self, store: Store):
        assert store.organisations.list_invitations(uuid4()) == []

    def test_an_invitation_resolves_by_token(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        invitation = store.organisations.create_invitation(
            org_id=org.id, email="new@x", role="viewer", invited_by=ada.id
        )

        found = store.organisations.get_invitation_by_token(invitation.token)

        assert found is not None
        assert found.id == invitation.id

    def test_an_unknown_token_resolves_to_nothing(self, store: Store):
        assert store.organisations.get_invitation_by_token("never-issued") is None

    def test_an_invitation_can_be_deleted(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        invitation = store.organisations.create_invitation(
            org_id=org.id, email="new@x", role="viewer", invited_by=ada.id
        )

        store.organisations.delete_invitation(invitation.id)

        assert store.organisations.list_invitations(org.id) == []

    def test_deleting_a_missing_invitation_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Invitation {missing} not found"):
            store.organisations.delete_invitation(missing)


class TestAcceptInvitationEdges:
    """The arms of ``accept_invitation`` beyond the happy path."""

    def test_an_expired_invitation_is_refused_and_swept(self, store: Store, auth_db: Engine):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        invitation = store.organisations.create_invitation(
            org_id=org.id, email="bob@x", role="viewer", invited_by=ada.id
        )
        with SQLSession(auth_db) as session:
            row = session.get(Invitation, invitation.id)
            assert row is not None
            row.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
            session.add(row)
            session.commit()

        assert store.organisations.accept_invitation(invitation.token, bob.id) is None
        # The stale row is deleted rather than left to accumulate.
        assert store.organisations.list_invitations(org.id) == []

    def test_accepting_twice_leaves_one_membership(self, store: Store):
        ada = store.auth.upsert_profile(google_id="g1", email="ada@x")
        bob = store.auth.upsert_profile(google_id="g2", email="bob@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        first = store.organisations.create_invitation(
            org_id=org.id, email="bob@x", role="viewer", invited_by=ada.id
        )
        store.organisations.accept_invitation(first.token, bob.id)
        second = store.organisations.create_invitation(
            org_id=org.id, email="bob@x", role="admin", invited_by=ada.id
        )

        store.organisations.accept_invitation(second.token, bob.id)

        members = store.organisations.list_members(org.id)
        assert len([m for m in members if m[0].id == bob.id]) == 1
        # The existing membership is left as it was, not re-roled.
        assert store.organisations.member_role(bob.id, org.id) == "viewer"
