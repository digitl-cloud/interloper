"""Tests for the organisation store (``interloper_db.store.organisations``)."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

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
    Organisation,
    PersonalAccessToken,
    Profile,
    Run,
    UserOrganisation,
)
from interloper_db.store import ActivityEntry, OrganisationQuery, PageQuery, Store


class TestDeleteOrganisation:
    def _seed_org_data(self, session: SQLSession, org_id: UUID) -> None:
        """Plant one row of every org-owned kind directly (no catalog needed).

        Args:
            session: Open session the rows are written through.
            org_id: Organisation the rows belong to.
        """
        source = Component(org_id=org_id, kind="source", key="demo")
        asset = Component(org_id=org_id, kind="asset", key="demo.a", parent_id=source.id)
        session.add(source)
        session.add(asset)
        session.add(
            ComponentRelation(
                src_id=asset.id, dst_id=source.id, org_id=org_id, src_kind="asset", dst_kind="source", name="owner"
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
        admin = store.profiles.upsert(google_id="g-admin", email="admin@example.com", name="Admin")
        org = store.organisations.create(name="Doomed", creator_id=admin.id)
        keeper = store.organisations.create(name="Keeper", creator_id=admin.id)
        store.members.add(org.id, admin.id, "admin")
        store.members.add(keeper.id, admin.id, "admin")
        store.invitations.create(org.id, email="new@example.com", role="viewer", invited_by=admin.id)
        store.tokens.create(admin.id, org.id, name="laptop")
        session_token = store.sessions.create(admin.id, org_id=org.id)
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
        with SQLSession(auth_db) as session:
            retained = session.get(Organisation, org.id)
            assert retained is not None
            assert retained.deleted_at is not None
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
        resolved = store.sessions.resolve(session_token)
        assert resolved is not None
        profile, auth_session = resolved
        assert auth_session.organisation_id is None
        assert profile.last_organisation_id is None
        assert store.organisations.get(keeper.id).deleted_at is None
        assert store.members.role(keeper.id, admin.id) == "admin"

    def test_double_delete_reads_as_missing(self, store: Store):
        org = store.organisations.create(name="Once")
        store.organisations.delete(org.id)
        with pytest.raises(NotFoundError):
            store.organisations.delete(org.id)
        with pytest.raises(NotFoundError):
            store.organisations.update(org.id, name="Renamed")

    def test_missing_organisation_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.organisations.delete(uuid4())


class TestActivity:
    def test_composes_and_sorts_the_derived_feed(self, store: Store, auth_db: Engine):
        admin = store.profiles.upsert(google_id="g-act", email="act@example.com", name="Act Min")
        org = store.organisations.create(name="Busy", creator_id=admin.id)
        store.members.add(org.id, admin.id, "admin")
        store.invitations.create(org.id, email="new@example.com", role="viewer", invited_by=admin.id)
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

        entries = store.organisations.activity(org.id, PageQuery()).items

        assert all(isinstance(entry, ActivityEntry) for entry in entries)
        kinds = [entry.kind for entry in entries]
        assert set(kinds) == {"org_created", "member_joined", "invitation_sent", "source_added", "runs_completed"}
        whens = [entry.when for entry in entries]
        assert whens == sorted(whens, reverse=True)
        assert all(when.tzinfo is not None for when in whens)
        joined = next(entry for entry in entries if entry.kind == "member_joined")
        assert joined.subject == "Act Min" and joined.extra == "admin"
        invited = next(entry for entry in entries if entry.kind == "invitation_sent")
        assert invited.subject == "new@example.com" and invited.extra == "Act Min"
        runs = next(entry for entry in entries if entry.kind == "runs_completed")
        assert runs.subject == "2"  # only the successful runs, aggregated per day

    def test_limit_caps_the_feed(self, store: Store):
        admin = store.profiles.upsert(google_id="g-cap", email="cap@example.com", name="Cap")
        org = store.organisations.create(name="Capped", creator_id=admin.id)
        store.members.add(org.id, admin.id, "admin")

        assert len(store.organisations.activity(org.id, PageQuery(limit=1)).items) == 1

    def test_the_feed_is_windowed_over_its_whole_length(self, store: Store):
        admin = store.profiles.upsert(google_id="g-page", email="page@example.com", name="Pager")
        org = store.organisations.create(name="Paged", creator_id=admin.id)
        store.invitations.create(org.id, email="a@example.com", role="viewer", invited_by=admin.id)
        store.invitations.create(org.id, email="b@example.com", role="viewer", invited_by=admin.id)
        whole = store.organisations.activity(org.id, PageQuery(limit=None))

        second = store.organisations.activity(org.id, PageQuery(limit=2, offset=1))

        assert whole.total == 4
        assert second.total == 4
        assert second.items == whole.items[1:3]

    def test_unknown_org_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.organisations.activity(uuid4(), PageQuery())


class TestUpdate:
    """Renaming, and the soft-delete guard on it."""

    def test_renames_the_organisation(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
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
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        store.organisations.delete(org.id)

        with pytest.raises(NotFoundError, match=f"Organisation {org.id} not found"):
            store.organisations.update(org.id, name="Acme Corp")


class TestList:
    """Every organisation, or one profile's, with soft-deleted ones opt-in."""

    def test_lists_every_organisation(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        store.organisations.create(name="First", creator_id=ada.id)
        store.organisations.create(name="Second", creator_id=bob.id)

        page = store.organisations.list(OrganisationQuery(limit=None))

        assert sorted(org.name for org in page.items) == ["First", "Second"]
        assert page.total == 2

    def test_soft_deleted_organisations_are_hidden_by_default(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        live = store.organisations.create(name="Live", creator_id=profile.id)
        gone = store.organisations.create(name="Gone", creator_id=profile.id)
        store.organisations.delete(gone.id)

        default = store.organisations.list(OrganisationQuery(limit=None))
        everything = store.organisations.list(OrganisationQuery(include_deleted=True, limit=None))

        assert [org.id for org in default.items] == [live.id]
        assert {org.id for org in everything.items} == {live.id, gone.id}

    def test_no_organisations_is_an_empty_page(self, store: Store):
        page = store.organisations.list(OrganisationQuery(include_deleted=True, limit=None))

        assert page.items == []
        assert page.total == 0

    def test_lists_only_the_users_organisations(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        bob = store.profiles.upsert(google_id="g2", email="bob@x")
        mine = store.organisations.create(name="Mine", creator_id=ada.id)
        store.organisations.create(name="Theirs", creator_id=bob.id)

        page = store.organisations.list(OrganisationQuery(user_id=ada.id))

        assert [org.id for org in page.items] == [mine.id]
        assert page.total == 1

    def test_a_user_with_no_memberships_gets_an_empty_page(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")

        assert store.organisations.list(OrganisationQuery(user_id=profile.id)).items == []

    def test_a_users_soft_deleted_organisation_is_opt_in(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=ada.id)
        store.organisations.delete(org.id)

        # The delete purges memberships, so the user no longer reaches it either way.
        assert store.organisations.list(OrganisationQuery(user_id=ada.id)).items == []
        assert store.organisations.list(OrganisationQuery(user_id=ada.id, include_deleted=True)).items == []

    def test_a_window_reports_the_whole_count(self, store: Store):
        ada = store.profiles.upsert(google_id="g1", email="ada@x")
        for name in ("A", "B", "C"):
            store.organisations.create(name=name, creator_id=ada.id)

        whole = store.organisations.list(OrganisationQuery(user_id=ada.id, limit=None))
        page = store.organisations.list(OrganisationQuery(user_id=ada.id, limit=2, offset=1))

        assert [org.id for org in page.items] == [org.id for org in whole.items[1:3]]
        assert page.total == 3
