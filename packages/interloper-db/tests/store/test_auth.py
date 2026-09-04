"""Tests for the auth store (``interloper_db.store.auth``)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from interloper.errors import NotFoundError
from sqlmodel import Session, select

from interloper_db.models import AuthSession
from interloper_db.store import Store


class TestListAllProfiles:
    def test_lists_profiles_with_their_organisations(self, store: Store):
        admin = store.auth.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        loner = store.auth.upsert_profile(google_id="g-loner", email="loner@example.com", name="Loner")
        org_a = store.organisations.create(name="Acme", creator_id=admin.id)
        org_b = store.organisations.create(name="Beta", creator_id=admin.id)
        store.organisations.add_member(org_a.id, admin.id, "admin")
        store.organisations.add_member(org_b.id, admin.id, "admin")

        orgs = {profile.id: organisations for profile, organisations in store.auth.list_all_profiles()}

        assert sorted(org.name for org in orgs[admin.id]) == ["Acme", "Beta"]
        assert orgs[loner.id] == []


class TestDeleteProfile:
    def test_deletes_profile_and_everything_anchored_to_it(self, store: Store):
        admin = store.auth.upsert_profile(google_id="g-admin", email="admin@example.com", name="Admin")
        keeper = store.auth.upsert_profile(google_id="g-keeper", email="keeper@example.com", name="Keeper")
        org = store.organisations.create(name="Acme", creator_id=admin.id)
        store.organisations.add_member(org.id, admin.id, "admin")
        store.organisations.add_member(org.id, keeper.id, "viewer")
        session_token = store.auth.create_session(user_id=admin.id)
        keeper_token = store.auth.create_session(user_id=keeper.id)
        store.tokens.create(user_id=admin.id, organisation_id=org.id, name="laptop")
        store.organisations.create_invitation(
            org_id=org.id, email="new@example.com", role="viewer", invited_by=admin.id
        )

        store.auth.delete_profile(admin.id)

        with pytest.raises(NotFoundError):
            store.auth.get_profile(admin.id)
        assert store.auth.resolve_session(session_token) is None
        assert not store.organisations.has_pending_invitation("new@example.com")
        assert store.organisations.member_role(admin.id, org.id) is None
        # Other users' data is untouched.
        assert store.auth.get_profile(keeper.id).id == keeper.id
        assert store.auth.resolve_session(keeper_token) is not None
        assert store.organisations.member_role(keeper.id, org.id) == "viewer"

    def test_missing_profile_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.auth.delete_profile(uuid4())


class TestGetProfileByGoogleId:
    def test_returns_matching_profile(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g-1", email="user@example.com", name="User")

        found = store.auth.get_profile_by_google_id("g-1")

        assert found is not None
        assert found.id == profile.id

    def test_returns_none_when_absent(self, store: Store):
        assert store.auth.get_profile_by_google_id("g-missing") is None


class TestUpdateProfile:
    def test_updates_name_and_timezone_independently(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g-upd", email="upd@example.com", name="Google Name")

        renamed = store.auth.update_profile(profile.id, name="Custom Name")
        assert renamed.name == "Custom Name"
        assert renamed.timezone is None

        zoned = store.auth.update_profile(profile.id, timezone="Europe/Berlin")
        assert zoned.name == "Custom Name"
        assert zoned.timezone == "Europe/Berlin"

    def test_missing_profile_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.auth.update_profile(uuid4(), name="Ghost")

    def test_user_set_name_survives_login_upsert(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g-keep", email="keep@example.com", name="Google Name")
        store.auth.update_profile(profile.id, name="Custom Name")

        relogged = store.auth.upsert_profile(google_id="g-keep", email="keep@example.com", name="Google Name")

        assert relogged.name == "Custom Name"

    def test_login_upsert_fills_an_empty_name(self, store: Store):
        store.auth.upsert_profile(google_id="g-fill", email="fill@example.com")

        filled = store.auth.upsert_profile(google_id="g-fill", email="fill@example.com", name="Google Name")

        assert filled.name == "Google Name"


class TestSetSuperAdmin:
    """Platform-wide privilege, toggled by id."""

    def test_promotes_a_profile(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")

        promoted = store.auth.set_super_admin(profile.id)

        assert promoted.is_super_admin is True
        assert store.auth.get_profile(profile.id).is_super_admin is True

    def test_demotes_when_asked(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        store.auth.set_super_admin(profile.id)

        store.auth.set_super_admin(profile.id, is_super_admin=False)

        assert store.auth.get_profile(profile.id).is_super_admin is False

    def test_a_missing_profile_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Profile {missing} not found"):
            store.auth.set_super_admin(missing)


class TestUpsertProfileAvatar:
    """The login upsert refreshes Google-managed fields."""

    def test_a_new_avatar_replaces_the_old_one(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x", avatar_url="https://a/1.png")

        store.auth.upsert_profile(google_id="g1", email="ada@x", avatar_url="https://a/2.png")

        assert store.auth.get_profile(profile.id).avatar_url == "https://a/2.png"

    def test_an_omitted_avatar_leaves_the_stored_one(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x", avatar_url="https://a/1.png")

        store.auth.upsert_profile(google_id="g1", email="ada@x")

        assert store.auth.get_profile(profile.id).avatar_url == "https://a/1.png"

    def test_the_email_is_refreshed_on_every_login(self, store: Store):
        # Google owns the address; a change there must land.
        profile = store.auth.upsert_profile(google_id="g1", email="old@x")

        store.auth.upsert_profile(google_id="g1", email="new@x")

        assert store.auth.get_profile(profile.id).email == "new@x"


class TestSessions:
    """Session creation, resolution, expiry and the active-organisation pointer."""

    def test_a_created_session_resolves_to_its_profile(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")

        token = store.auth.create_session(profile.id)
        resolved = store.auth.resolve_session(token)

        assert resolved is not None
        found, session_row = resolved
        assert found.id == profile.id
        assert session_row.user_id == profile.id

    def test_the_raw_token_is_not_stored(self, store: Store):
        # Only its hash is persisted, so a database leak yields no usable session.
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")

        token = store.auth.create_session(profile.id)
        resolved = store.auth.resolve_session(token)

        assert resolved is not None
        assert resolved[1].token_hash != token

    def test_an_unknown_token_resolves_to_nothing(self, store: Store):
        assert store.auth.resolve_session("never-issued") is None

    def test_an_expired_session_resolves_to_nothing_and_is_swept(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        token = store.auth.create_session(profile.id)
        with Session(store.engine) as session:
            row = session.exec(select(AuthSession)).one()
            row.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
            session.add(row)
            session.commit()

        assert store.auth.resolve_session(token) is None
        # The expired row is deleted rather than left to accumulate.
        with Session(store.engine) as session:
            assert session.exec(select(AuthSession)).all() == []

    def test_a_session_whose_profile_vanished_resolves_to_nothing(self, store: Store):
        # The foreign key makes this unreachable through the store's own API,
        # so the orphan is fabricated with constraints off — the guard exists
        # for a row that should not be there, and this proves what it does.
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        token = store.auth.create_session(profile.id)
        with store.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
            connection.exec_driver_sql("DELETE FROM profiles")
            connection.commit()

        assert store.auth.resolve_session(token) is None

    def test_a_session_can_be_created_already_scoped(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        token = store.auth.create_session(profile.id, organisation_id=org.id)
        resolved = store.auth.resolve_session(token)

        assert resolved is not None
        assert resolved[1].organisation_id == org.id


class TestSetSessionOrg:
    """Switching the active organisation, and remembering it on the profile."""

    def test_it_updates_the_session(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        token = store.auth.create_session(profile.id)

        store.auth.set_session_org(token, org.id)
        resolved = store.auth.resolve_session(token)

        assert resolved is not None
        assert resolved[1].organisation_id == org.id

    def test_a_user_id_also_records_the_preference(self, store: Store):
        # So the next login lands in the org the user last used.
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        token = store.auth.create_session(profile.id)

        store.auth.set_session_org(token, org.id, user_id=profile.id)

        assert store.auth.get_profile(profile.id).last_organisation_id == org.id

    def test_without_a_user_id_the_profile_is_untouched(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        token = store.auth.create_session(profile.id)

        store.auth.set_session_org(token, org.id)

        assert store.auth.get_profile(profile.id).last_organisation_id is None

    def test_an_unknown_token_is_a_no_op(self, store: Store):
        store.auth.set_session_org("never-issued", uuid4())

    def test_an_unknown_user_id_leaves_the_session_updated(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)
        token = store.auth.create_session(profile.id)

        store.auth.set_session_org(token, org.id, user_id=uuid4())

        resolved = store.auth.resolve_session(token)
        assert resolved is not None
        assert resolved[1].organisation_id == org.id


class TestDeleteUserSessions:
    """A logout ends every browser, not just the one that asked."""

    def test_all_of_the_users_sessions_go(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")
        first = store.auth.create_session(profile.id)
        second = store.auth.create_session(profile.id)

        store.auth.delete_user_sessions(profile.id)

        assert store.auth.resolve_session(first) is None
        assert store.auth.resolve_session(second) is None

    def test_another_users_sessions_survive(self, store: Store):
        mine = store.auth.upsert_profile(google_id="g1", email="ada@x")
        theirs = store.auth.upsert_profile(google_id="g2", email="bob@x")
        my_token = store.auth.create_session(mine.id)
        their_token = store.auth.create_session(theirs.id)

        store.auth.delete_user_sessions(mine.id)

        assert store.auth.resolve_session(my_token) is None
        assert store.auth.resolve_session(their_token) is not None

    def test_a_user_with_no_sessions_is_a_no_op(self, store: Store):
        profile = store.auth.upsert_profile(google_id="g1", email="ada@x")

        store.auth.delete_user_sessions(profile.id)
