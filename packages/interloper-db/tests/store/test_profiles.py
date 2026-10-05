"""Tests for the profile store (``interloper_db.store.profiles``)."""

from __future__ import annotations

from uuid import uuid4

import pytest
from interloper.errors import NotFoundError

from interloper_db.store import ProfileQuery, Store


class TestList:
    def test_lists_profiles_with_their_organisations(self, store: Store):
        admin = store.profiles.upsert(google_id="g-admin", email="admin@example.com", name="Admin")
        loner = store.profiles.upsert(google_id="g-loner", email="loner@example.com", name="Loner")
        org_a = store.organisations.create(name="Acme", creator_id=admin.id)
        org_b = store.organisations.create(name="Beta", creator_id=admin.id)
        store.members.add(org_a.id, admin.id, "admin")
        store.members.add(org_b.id, admin.id, "admin")

        page = store.profiles.list(ProfileQuery(limit=None))
        orgs = {profile.id: profile.organisations for profile in page.items}

        assert sorted(org.name for org in orgs[admin.id]) == ["Acme", "Beta"]
        assert orgs[loner.id] == []
        assert page.total == 2

    def test_a_window_reports_the_whole_count(self, store: Store):
        for index in range(3):
            store.profiles.upsert(google_id=f"g-{index}", email=f"user{index}@example.com")

        page = store.profiles.list(ProfileQuery(limit=2))

        assert len(page.items) == 2
        assert page.total == 3

    def test_filters_on_the_super_admin_flag(self, store: Store):
        admin = store.profiles.upsert(google_id="g-admin", email="admin@example.com")
        member = store.profiles.upsert(google_id="g-member", email="member@example.com")
        store.profiles.set_super_admin(admin.id, value=True)

        super_admins = store.profiles.list(ProfileQuery(super_admin=True, limit=None))
        others = store.profiles.list(ProfileQuery(super_admin=False, limit=None))

        assert [profile.id for profile in super_admins.items] == [admin.id]
        assert [profile.id for profile in others.items] == [member.id]


class TestDelete:
    def test_deletes_profile_and_everything_anchored_to_it(self, store: Store):
        admin = store.profiles.upsert(google_id="g-admin", email="admin@example.com", name="Admin")
        keeper = store.profiles.upsert(google_id="g-keeper", email="keeper@example.com", name="Keeper")
        org = store.organisations.create(name="Acme", creator_id=admin.id)
        store.members.add(org.id, admin.id, "admin")
        store.members.add(org.id, keeper.id, "viewer")
        session_token = store.sessions.create(admin.id)
        keeper_token = store.sessions.create(keeper.id)
        store.tokens.create(admin.id, org.id, name="laptop")
        store.invitations.create(org.id, email="new@example.com", role="viewer", invited_by=admin.id)

        store.profiles.delete(admin.id)

        with pytest.raises(NotFoundError):
            store.profiles.get(admin.id)
        assert store.sessions.resolve(session_token) is None
        assert not store.invitations.has_pending("new@example.com")
        assert store.members.role(org.id, admin.id) is None
        # Other users' data is untouched.
        assert store.profiles.get(keeper.id).id == keeper.id
        assert store.sessions.resolve(keeper_token) is not None
        assert store.members.role(org.id, keeper.id) == "viewer"

    def test_missing_profile_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.profiles.delete(uuid4())


class TestGetByGoogleId:
    def test_returns_matching_profile(self, store: Store):
        profile = store.profiles.upsert(google_id="g-1", email="user@example.com", name="User")

        found = store.profiles.get_by_google_id("g-1")

        assert found is not None
        assert found.id == profile.id

    def test_returns_none_when_absent(self, store: Store):
        assert store.profiles.get_by_google_id("g-missing") is None


class TestUpdate:
    def test_updates_name_and_timezone_independently(self, store: Store):
        profile = store.profiles.upsert(google_id="g-upd", email="upd@example.com", name="Google Name")

        renamed = store.profiles.update(profile.id, name="Custom Name")
        assert renamed.name == "Custom Name"
        assert renamed.timezone is None

        zoned = store.profiles.update(profile.id, timezone="Europe/Berlin")
        assert zoned.name == "Custom Name"
        assert zoned.timezone == "Europe/Berlin"

    def test_missing_profile_raises(self, store: Store):
        with pytest.raises(NotFoundError):
            store.profiles.update(uuid4(), name="Ghost")

    def test_user_set_name_survives_login_upsert(self, store: Store):
        profile = store.profiles.upsert(google_id="g-keep", email="keep@example.com", name="Google Name")
        store.profiles.update(profile.id, name="Custom Name")

        relogged = store.profiles.upsert(google_id="g-keep", email="keep@example.com", name="Google Name")

        assert relogged.name == "Custom Name"

    def test_login_upsert_fills_an_empty_name(self, store: Store):
        store.profiles.upsert(google_id="g-fill", email="fill@example.com")

        filled = store.profiles.upsert(google_id="g-fill", email="fill@example.com", name="Google Name")

        assert filled.name == "Google Name"


class TestSetSuperAdmin:
    """Platform-wide privilege, toggled by id."""

    def test_promotes_a_profile(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")

        promoted = store.profiles.set_super_admin(profile.id, value=True)

        assert promoted.is_super_admin is True
        assert store.profiles.get(profile.id).is_super_admin is True

    def test_demotes_when_asked(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        store.profiles.set_super_admin(profile.id, value=True)

        store.profiles.set_super_admin(profile.id, value=False)

        assert store.profiles.get(profile.id).is_super_admin is False

    def test_returns_the_profile_with_its_organisations(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x")
        org = store.organisations.create(name="Acme", creator_id=profile.id)

        promoted = store.profiles.set_super_admin(profile.id, value=True)

        assert [organisation.id for organisation in promoted.organisations] == [org.id]

    def test_a_missing_profile_raises(self, store: Store):
        missing = uuid4()

        with pytest.raises(NotFoundError, match=f"Profile {missing} not found"):
            store.profiles.set_super_admin(missing, value=True)


class TestPromoteSuperAdmins:
    """Bulk, promote-only bootstrap from a list of emails."""

    def test_promotes_the_listed_profiles_case_insensitively(self, store: Store):
        ada = store.profiles.upsert(google_id="g-ada", email="Ada@Example.com")
        bob = store.profiles.upsert(google_id="g-bob", email="bob@example.com")
        eve = store.profiles.upsert(google_id="g-eve", email="eve@example.com")

        promoted = store.profiles.promote_super_admins(["ada@example.com", "BOB@example.com"])

        assert {profile.id for profile in promoted} == {ada.id, bob.id}
        assert all(profile.is_super_admin for profile in promoted)
        assert store.profiles.get(eve.id).is_super_admin is False

    def test_returns_only_the_profiles_it_promoted(self, store: Store):
        ada = store.profiles.upsert(google_id="g-ada", email="ada@example.com")
        store.profiles.set_super_admin(ada.id, value=True)

        assert store.profiles.promote_super_admins(["ada@example.com"]) == []
        assert store.profiles.get(ada.id).is_super_admin is True

    def test_emails_without_a_profile_are_ignored(self, store: Store):
        assert store.profiles.promote_super_admins(["ghost@example.com"]) == []

    def test_an_empty_list_promotes_nobody(self, store: Store):
        store.profiles.upsert(google_id="g-ada", email="ada@example.com")

        assert store.profiles.promote_super_admins([]) == []


class TestUpsertAvatar:
    """The login upsert refreshes Google-managed fields."""

    def test_a_new_avatar_replaces_the_old_one(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x", avatar_url="https://a/1.png")

        store.profiles.upsert(google_id="g1", email="ada@x", avatar_url="https://a/2.png")

        assert store.profiles.get(profile.id).avatar_url == "https://a/2.png"

    def test_an_omitted_avatar_leaves_the_stored_one(self, store: Store):
        profile = store.profiles.upsert(google_id="g1", email="ada@x", avatar_url="https://a/1.png")

        store.profiles.upsert(google_id="g1", email="ada@x")

        assert store.profiles.get(profile.id).avatar_url == "https://a/1.png"

    def test_the_email_is_refreshed_on_every_login(self, store: Store):
        # Google owns the address; a change there must land.
        profile = store.profiles.upsert(google_id="g1", email="old@x")

        store.profiles.upsert(google_id="g1", email="new@x")

        assert store.profiles.get(profile.id).email == "new@x"
