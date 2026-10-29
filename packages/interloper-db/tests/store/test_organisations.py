"""Tests for the organisation store (``interloper_db.store.organisations``)."""

from __future__ import annotations

import importlib
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from interloper.errors import ConfigError
from sqlalchemy import Engine, inspect, text
from sqlmodel import Session as SQLSession
from sqlmodel import select

from interloper_db import Audience, ConflictError, NotFoundError
from interloper_db.models import (
    Backfill,
    Component,
    ComponentRelation,
    Event,
    Invitation,
    Organisation,
    Profile,
    Run,
    UserOrganisation,
)
from interloper_db.store import OrganisationQuery, Store


class TestKey:
    def test_create_derives_the_key_from_the_name(self, store: Store):
        assert store.organisations.create(name="Swarovski AG").key == "swarovski-ag"

    def test_reserved_name_gets_suffix(self, store: Store):
        assert store.organisations.create(name="Settings").key == "settings-2"

    def test_duplicate_name_dedupes(self, store: Store):
        keys = [store.organisations.create(name="Acme").key for _ in range(3)]
        assert keys == ["acme", "acme-2", "acme-3"]

    def test_a_deleted_organisation_keeps_its_key(self, store: Store):
        first = store.organisations.create(name="Acme")
        store.organisations.delete(first.id)
        assert store.organisations.create(name="Acme").key == "acme-2"

    def test_get_by_key(self, store: Store):
        org = store.organisations.create(name="Swarovski AG")
        assert store.organisations.get_by_key("swarovski-ag").id == org.id

    def test_get_by_key_misses_unknown_and_deleted(self, store: Store):
        org = store.organisations.create(name="Acme")
        with pytest.raises(NotFoundError):
            store.organisations.get_by_key("nope")
        store.organisations.delete(org.id)
        with pytest.raises(NotFoundError):
            store.organisations.get_by_key("acme")

    def test_update_renames_without_touching_the_key(self, store: Store):
        org = store.organisations.create(name="Acme")
        updated = store.organisations.update(org.id, name="Acme Corp")
        assert (updated.name, updated.key) == ("Acme Corp", "acme")

    def test_update_sets_a_free_key(self, store: Store):
        org = store.organisations.create(name="Acme")
        updated = store.organisations.update(org.id, key="acme-eu")
        assert updated.key == "acme-eu"
        assert store.organisations.get_by_key("acme-eu").id == org.id

    def test_update_sets_name_and_key_together(self, store: Store):
        org = store.organisations.create(name="Acme")
        updated = store.organisations.update(org.id, name="Acme Corp", key="acme-corp")
        assert (updated.name, updated.key) == ("Acme Corp", "acme-corp")

    def test_update_to_its_own_key_is_a_no_op(self, store: Store):
        org = store.organisations.create(name="Acme")
        assert store.organisations.update(org.id, key="acme").key == "acme"

    def test_rename_to_taken_key_conflicts(self, store: Store):
        store.organisations.create(name="Acme")
        other = store.organisations.create(name="Globex")
        with pytest.raises(ConflictError, match="acme"):
            store.organisations.update(other.id, key="acme")
        assert store.organisations.get(other.id).key == "globex"

    def test_update_refuses_a_reserved_or_malformed_key(self, store: Store):
        org = store.organisations.create(name="Acme")
        with pytest.raises(ConfigError):
            store.organisations.update(org.id, key="admin")
        with pytest.raises(ConfigError):
            store.organisations.update(org.id, key="Not Valid")


class TestMigration019:
    @staticmethod
    def _migrate(auth_db: Engine, step: str) -> None:
        """Run one step of migration 019 against the fixture database.

        Args:
            auth_db: The fixture database.
            step: ``"upgrade"`` or ``"downgrade"``.
        """
        migration = importlib.import_module("interloper_db.migrations.versions.019_organisation_key")
        with auth_db.begin() as connection, Operations.context(MigrationContext.configure(connection)):
            getattr(migration, step)()

    def test_an_upgraded_database_keeps_its_slugs_as_keys(self, store: Store, auth_db: Engine):
        org = store.organisations.create(name="Acme")
        self._migrate(auth_db, "downgrade")
        self._migrate(auth_db, "upgrade")
        assert store.organisations.get_by_key("acme").id == org.id
        assert "ix_organisations_key" in {index["name"] for index in inspect(auth_db).get_indexes("organisations")}

    def test_a_fresh_database_drops_the_empty_slug(self, store: Store, auth_db: Engine):
        # Migration 016 adds ``slug`` beside the ``key`` create_all made.
        with auth_db.begin() as connection:
            connection.execute(text("ALTER TABLE organisations ADD COLUMN slug varchar"))
            connection.execute(text("CREATE UNIQUE INDEX ix_organisations_slug ON organisations (slug)"))
        self._migrate(auth_db, "upgrade")
        columns = {column["name"] for column in inspect(auth_db).get_columns("organisations")}
        assert "slug" not in columns
        assert store.organisations.create(name="Acme").key == "acme"

    def test_a_database_already_on_key_is_left_alone(self, store: Store, auth_db: Engine):
        org = store.organisations.create(name="Acme")
        self._migrate(auth_db, "upgrade")
        assert store.organisations.get_by_key("acme").id == org.id


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
        _, raw_token = store.tokens.create_personal(admin.id, "laptop")
        session_token = store.tokens.create_session(admin.id)
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
                session.exec(select(UserOrganisation).where(UserOrganisation.organisation_id == org.id)).first() is None
            )
            assert session.exec(select(Invitation).where(Invitation.organisation_id == org.id)).first() is None
            # ...but execution history survives for billing, detached from
            # the purged components via the FK's SET NULL.
            surviving_run = session.exec(select(Run).where(Run.org_id == org.id)).one()
            assert surviving_run.component_id is None
            assert session.exec(select(Backfill).where(Backfill.org_id == org.id)).first() is not None
            assert session.exec(select(Event).where(Event.org_id == org.id)).first() is not None
        # The user, their session, and the other organisation survive; org refs are cleared.
        resolved = store.tokens.resolve(session_token, Audience.APP)
        assert resolved is not None
        profile, _ = resolved
        assert profile.last_organisation_id is None
        assert store.organisations.get(keeper.id).deleted_at is None
        assert store.members.role(keeper.id, admin.id) == "admin"
        # A token is its user's, not the organisation's: it keeps working.
        assert store.tokens.resolve(raw_token, Audience.API) is not None

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
