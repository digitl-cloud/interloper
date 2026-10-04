"""Tests for the login session store (``interloper_db.store.sessions``)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlmodel import Session, select

from interloper_db.models import AuthSession
from interloper_db.store import Store


class TestCreateAndResolve:
    """Session creation, resolution and expiry."""

    def test_a_created_session_resolves_to_its_profile(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")

        token = store.sessions.create(profile.id)
        resolved = store.sessions.resolve(token)

        assert resolved is not None
        found, session_row = resolved
        assert found.id == profile.id
        assert session_row.user_id == profile.id

    def test_the_raw_token_is_not_stored(self, store: Store):
        # Only its hash is persisted, so a database leak yields no usable session.
        profile = store.profiles.upsert(google_id="g1", email="ada@x")

        token = store.sessions.create(profile.id)
        resolved = store.sessions.resolve(token)

        assert resolved is not None
        assert resolved[1].token_hash != token

    def test_an_unknown_token_resolves_to_nothing(self, store: Store):
        assert store.sessions.resolve("never-issued") is None

    def test_an_expired_session_resolves_to_nothing_and_is_swept(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        token = store.sessions.create(profile.id)
        with Session(store.engine) as session:
            row = session.exec(select(AuthSession)).one()
            row.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
            session.add(row)
            session.commit()

        assert store.sessions.resolve(token) is None
        # The expired row is deleted rather than left to accumulate.
        with Session(store.engine) as session:
            assert session.exec(select(AuthSession)).all() == []

    def test_a_session_whose_profile_vanished_resolves_to_nothing(self, store: Store):
        # The foreign key makes this unreachable through the store's own API,
        # so the orphan is fabricated with constraints off; the guard exists
        # for a row that should not be there, and this proves what it does.
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        token = store.sessions.create(profile.id)
        with store.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
            connection.exec_driver_sql("DELETE FROM profiles")
            connection.commit()

        assert store.sessions.resolve(token) is None

    def test_a_session_can_be_created_already_scoped(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        token = store.sessions.create(profile.id, org_id=org.id)
        resolved = store.sessions.resolve(token)

        assert resolved is not None
        assert resolved[1].organisation_id == org.id


class TestSwitchOrg:
    """Switching the active organisation, and remembering it on the profile."""

    def test_it_updates_the_session(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        token = store.sessions.create(profile.id)

        store.sessions.switch_org(token, org.id, profile.id)
        resolved = store.sessions.resolve(token)

        assert resolved is not None
        assert resolved[1].organisation_id == org.id

    def test_it_records_the_preference(self, store: Store):
        # So the next login lands in the org the user last used.
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        token = store.sessions.create(profile.id)

        store.sessions.switch_org(token, org.id, profile.id)

        assert store.profiles.get(profile.id).last_organisation_id == org.id

    def test_an_unknown_token_is_a_no_op(self, store: Store):
        store.sessions.switch_org("never-issued", uuid4(), uuid4())

    def test_an_unknown_token_still_records_the_preference(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        store.sessions.switch_org("never-issued", org.id, profile.id)

        assert store.profiles.get(profile.id).last_organisation_id == org.id

    def test_an_unknown_user_id_leaves_the_session_updated(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        token = store.sessions.create(profile.id)

        store.sessions.switch_org(token, org.id, uuid4())

        resolved = store.sessions.resolve(token)
        assert resolved is not None
        assert resolved[1].organisation_id == org.id


class TestDeleteAll:
    """A logout ends every browser, not just the one that asked."""

    def test_all_of_the_users_sessions_go(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        first = store.sessions.create(profile.id)
        second = store.sessions.create(profile.id)

        store.sessions.delete_all(profile.id)

        assert store.sessions.resolve(first) is None
        assert store.sessions.resolve(second) is None

    def test_another_users_sessions_survive(self, store: Store):
        mine = store.profiles.upsert(google_id="g1", email="ada@x")
        theirs = store.profiles.upsert(google_id="g2", email="bob@x")
        my_token = store.sessions.create(mine.id)
        their_token = store.sessions.create(theirs.id)

        store.sessions.delete_all(mine.id)

        assert store.sessions.resolve(my_token) is None
        assert store.sessions.resolve(their_token) is not None

    def test_a_user_with_no_sessions_is_a_no_op(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")

        store.sessions.delete_all(profile.id)
