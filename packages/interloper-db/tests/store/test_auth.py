"""Tests for the auth store (``interloper_db.store.auth``)."""

from __future__ import annotations

from uuid import uuid4

import pytest
from interloper.errors import NotFoundError

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

        assert store.auth.get_profile(admin.id) is None
        assert store.auth.resolve_session(session_token) is None
        assert not store.organisations.has_pending_invitation("new@example.com")
        assert store.organisations.member_role(admin.id, org.id) is None
        # Other users' data is untouched.
        assert store.auth.get_profile(keeper.id) is not None
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
