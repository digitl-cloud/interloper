"""Tests for ``interloper_api.routes.admin`` (super-admin platform-wide surface).

The critical property is that every endpoint is gated by ``require_super_admin``
and is *not* bound to the session's active organisation. A lightweight fake
store stands in for persistence so these stay pure unit tests.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError
from interloper_db import OrganisationQuery, Page, PageQuery, ProfileQuery, UsageQuery
from interloper_db.store.insights import ActivityEntry
from interloper_db.store.quotas import QUOTAS

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import get_admin_config, get_auth_config, get_current_user, get_store
from interloper_api.notifications import SuperAdminPromotionEmail
from interloper_api.routes import admin as admin_module


class FakeStore:
    """In-memory stand-in exposing only the store facets the admin routes reach for."""

    def __init__(self) -> None:
        self.org = SimpleNamespace(id=uuid4(), name="Acme", created_at=datetime.now(timezone.utc), deleted_at=None)
        self.profile_rows: dict[UUID, SimpleNamespace] = {}
        self.member = self.add_profile("member@acme.test", name="Member")
        self.deleted_profiles: list[UUID] = []
        self.quota_updates: list[tuple[UUID, dict]] = []
        self.listed_orgs: list[OrganisationQuery] = []
        self.listed_profiles: list[ProfileQuery] = []
        self.counted_orgs: list[list[UUID]] = []
        self.activity_calls: list[tuple[UUID, PageQuery]] = []
        self.activity: list[ActivityEntry] = []
        self.profiles = SimpleNamespace(
            delete=self._delete_profile,
            get=self._get_profile,
            list=self._list_profiles,
            set_super_admin=self._set_super_admin,
        )
        self.organisations = SimpleNamespace(
            list=self._list_organisations,
            create=self._create_organisation,
            get=self._get_organisation,
        )
        self.insights = SimpleNamespace(feed=self._activity)
        self.members = SimpleNamespace(count_by_org=self._count_by_org)
        self.quotas = SimpleNamespace(
            all_overrides=self._all_quota_overrides,
            set_overrides=self._set_quota,
        )
        self.usage = SimpleNamespace(
            current_period=self._current_period,
            list=self._list_usage,
            sources_by_org=self._sources_by_org,
            max_assets_per_source_by_org=self._max_assets_per_source_by_org,
            successful_runs_by_org=self._successful_runs_by_org,
        )

    def _delete_profile(self, user_id: UUID) -> None:
        self.deleted_profiles.append(user_id)

    # -- users --
    def _get_profile(self, user_id: UUID) -> SimpleNamespace:
        if user_id not in self.profile_rows:
            raise NotFoundError(f"Profile {user_id} not found")
        return self.profile_rows[user_id]

    def _list_profiles(self, query: ProfileQuery) -> Page:
        self.listed_profiles.append(query)
        rows = [
            row
            for row in self.profile_rows.values()
            if query.super_admin is None or row.is_super_admin is query.super_admin
        ]
        return Page.window(rows, query)

    def _set_super_admin(self, user_id: UUID, *, value: bool) -> SimpleNamespace:
        row = self._get_profile(user_id)
        row.is_super_admin = value
        return row

    def add_profile(self, email: str, *, name: str | None = None, is_super_admin: bool = False) -> SimpleNamespace:
        """Store a profile row the user routes can read and change.

        Args:
            email: The profile's email.
            name: The profile's display name.
            is_super_admin: Whether the profile starts as a super-admin.

        Returns:
            The stored row.
        """
        row = SimpleNamespace(
            id=uuid4(),
            email=email,
            name=name,
            avatar_url=None,
            is_super_admin=is_super_admin,
            created_at=datetime.now(timezone.utc),
            organisations=[self.org],
        )
        self.profile_rows[row.id] = row
        return row

    # -- organisations --
    def _list_organisations(self, query: OrganisationQuery) -> Page:
        self.listed_orgs.append(query)
        return Page.window([self.org], query)

    def _count_by_org(self, org_ids: list[UUID]) -> dict[UUID, int]:
        self.counted_orgs.append(list(org_ids))
        return {self.org.id: 1}

    def _create_organisation(self, name: str, creator_id: UUID | None = None):
        return SimpleNamespace(id=uuid4(), name=name, created_at=datetime.now(timezone.utc), deleted_at=None)

    def _get_organisation(self, org_id: UUID):
        return self.org

    def _activity(self, org_id: UUID, query: PageQuery) -> Page:
        self.activity_calls.append((org_id, query))
        return Page.window(self.activity, query)

    # -- quotas --
    def _current_period(self):
        return date(2026, 8, 1)

    def _all_quota_overrides(self):
        return {self.org.id: {"max_sources": 5}}

    def _list_usage(self, query: UsageQuery) -> Page:
        assert query.limit is None
        rows = [
            SimpleNamespace(
                org_id=self.org.id,
                metric="successful_runs",
                period_start=date(2026, 8, 1),
                used=7,
                reserved=1,
            )
        ]
        return Page(items=[row for row in rows if row.period_start == query.period_start], total=1)

    def _sources_by_org(self):
        return {self.org.id: 2}

    def _max_assets_per_source_by_org(self):
        return {self.org.id: 4}

    def _successful_runs_by_org(self, period_start):
        return {self.org.id: 8}

    def _set_quota(self, org_id: UUID, limits: dict):
        self.quota_updates.append((org_id, limits))
        return {key: value for key, value in limits.items() if value is not None}


def _profile(*, is_super_admin: bool):
    return SimpleNamespace(
        id=uuid4(),
        email="user@test",
        name="User",
        avatar_url=None,
        is_super_admin=is_super_admin,
    )


def _app(store: FakeStore, *, is_super_admin: bool) -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(admin_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: _profile(is_super_admin=is_super_admin)
    return app


def _client(store: FakeStore, *, is_super_admin: bool) -> TestClient:
    """A client for the probe app.

    Args:
        store: The fake store the routes resolve against.
        is_super_admin: Whether the caller is a super-admin.

    Returns:
        The client.
    """
    return TestClient(_app(store, is_super_admin=is_super_admin))


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


# -- gating -------------------------------------------------------------------


def test_non_super_admin_is_forbidden(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get("/admin/organisations")
    assert resp.status_code == 403


def test_config_snapshot_redacts_secrets(fake_settings: SimpleNamespace) -> None:
    snapshot = admin_module.AdminConfigResponse.from_settings(fake_settings, features={"agent": True})
    payload = snapshot.model_dump_json()
    secrets = (
        "oauth-secret",
        "smtp-secret",
        "pg-secret",
        "key-material",
        "l-secret",
        "nested-secret",
        "r-secret",
        "pull-secret",
        "secret-header",
        "collector:4317",
    )
    for secret in secrets:
        assert secret not in payload
    assert snapshot.auth.google_oauth_configured is True
    assert snapshot.data.encryption_configured is True
    assert snapshot.services.reaper.timeout == 3600
    assert snapshot.services.telemetry.enabled is True
    assert snapshot.services.telemetry.endpoint_configured is True


def test_config_snapshot_allowlists_launcher_and_runner_config(fake_settings: SimpleNamespace) -> None:
    snapshot = admin_module.AdminConfigResponse.from_settings(fake_settings, features={"agent": True})
    assert snapshot.deployment.launcher.config == {
        "image": "ghcr.io/x:1",
        "namespace": "prod",
        "service_account_name": "sa",
        "ttl_seconds_after_finished": 300,
        "runner_config": {"max_workers": 4},
    }
    assert snapshot.deployment.runner.config == {"max_workers": 8}
    # Explicitly configured keys are excluded from the class defaults.
    assert snapshot.deployment.launcher.defaults == {"runner_type": "async"}
    assert snapshot.deployment.runner.defaults == {}


def test_config_snapshot_surfaces_class_defaults_for_unset_config(fake_settings: SimpleNamespace) -> None:
    fake_settings.runner = SimpleNamespace(type="async", config={})
    snapshot = admin_module.AdminConfigResponse.from_settings(fake_settings, features={"agent": True})
    assert snapshot.deployment.runner.defaults == {"max_workers": 4}


def test_config_snapshot_reports_hydrated_catalog_by_kind(fake_settings: SimpleNamespace) -> None:
    catalog = SimpleNamespace(
        components={
            "demo": SimpleNamespace(kind="source"),
            "csv": SimpleNamespace(kind="destination"),
            "bigquery": SimpleNamespace(kind="destination"),
        }
    )
    snapshot = admin_module.AdminConfigResponse.from_settings(fake_settings, features={"agent": True}, catalog=catalog)
    assert snapshot.data.catalog == {"destination": ["bigquery", "csv"], "source": ["demo"]}


def test_non_super_admin_cannot_read_config(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get("/admin/config")
    assert resp.status_code == 403


def test_super_admin_reads_config(store: FakeStore, fake_settings: SimpleNamespace) -> None:
    app = _app(store, is_super_admin=True)
    snapshot = admin_module.AdminConfigResponse.from_settings(fake_settings, features={"agent": True})
    app.dependency_overrides[get_admin_config] = lambda: snapshot
    client = TestClient(app)
    resp = client.get("/admin/config")
    assert resp.status_code == 200
    assert resp.json()["deployment"]["launcher"]["type"] == "kubernetes"
    assert resp.json()["auth"]["allowed_domains"] == ["digitlcloud.com"]


def test_config_unavailable_returns_503(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/config")
    assert resp.status_code == 503


def test_non_super_admin_cannot_read_quotas(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get("/admin/quotas")
    assert resp.status_code == 403


def test_super_admin_reads_quota_overview(store: FakeStore) -> None:
    from interloper_api.dependencies import get_quota_defaults

    app = _app(store, is_super_admin=True)
    app.dependency_overrides[get_quota_defaults] = lambda: SimpleNamespace(
        max_sources=10, max_assets_per_source=20, max_successful_runs_per_month=100
    )
    client = TestClient(app)
    resp = client.get("/admin/quotas")
    assert resp.status_code == 200
    body = resp.json()
    assert body["period_start"] == "2026-08-01"
    assert body["defaults"]["max_successful_runs_per_month"] == 100

    # Field descriptors drive the admin UI: registry order (sorted), registry labels.
    assert [field["key"] for field in body["fields"]] == list(QUOTAS.keys())
    by_key = {field["key"]: field for field in body["fields"]}
    assert by_key["max_sources"] == {"key": "max_sources", "label": "Max sources", "default": 10}
    assert by_key["max_backfill_partitions"]["default"] is None

    (org,) = body["organisations"]
    assert org["limits"]["max_sources"] == 5
    # Overrides win field-by-field; unset fields fall back to the defaults.
    assert org["effective"]["max_sources"] == 5
    assert org["effective"]["max_assets_per_source"] == 20
    assert org["sources"] == 2
    assert org["max_assets_per_source"] == 4
    assert org["successful_runs"] == 7
    assert org["reserved_runs"] == 1
    assert org["recomputed_successful_runs"] == 8


def test_quota_overview_defaults_absent_means_unlimited(store: FakeStore) -> None:
    from interloper_api.dependencies import get_quota_defaults

    app = _app(store, is_super_admin=True)
    app.dependency_overrides[get_quota_defaults] = lambda: None
    client = TestClient(app)
    resp = client.get("/admin/quotas")
    assert resp.status_code == 200
    body = resp.json()
    assert body["defaults"] == dict.fromkeys(QUOTAS.keys())
    (org,) = body["organisations"]
    assert org["effective"]["max_assets_per_source"] is None


def test_non_super_admin_cannot_list_users(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get("/admin/users")
    assert resp.status_code == 403


def test_super_admin_lists_all_users(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/users")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    (user,) = body["items"]
    assert user["email"] == "member@acme.test"
    assert user["organisations"] == [{"id": str(store.org.id), "name": "Acme"}]
    assert user["is_super_admin"] is False
    assert store.listed_profiles == [ProfileQuery()]


def test_list_users_forwards_the_page_window(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/users", params={"limit": 20, "offset": 40})
    assert resp.status_code == 200
    assert store.listed_profiles == [ProfileQuery(limit=20, offset=40)]


def test_list_users_rejects_a_page_larger_than_the_cap(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/users", params={"limit": 501})
    assert resp.status_code == 422
    assert store.listed_profiles == []


def test_non_super_admin_cannot_delete_user(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).delete(f"/admin/users/{uuid4()}")
    assert resp.status_code == 403
    assert store.deleted_profiles == []


def test_delete_user(store: FakeStore) -> None:
    target = uuid4()
    resp = _client(store, is_super_admin=True).delete(f"/admin/users/{target}")
    assert resp.status_code == 204
    assert resp.content == b""
    assert store.deleted_profiles == [target]


def test_cannot_delete_own_account(store: FakeStore) -> None:
    app = _app(store, is_super_admin=True)
    me = _profile(is_super_admin=True)
    app.dependency_overrides[get_current_user] = lambda: me
    client = TestClient(app)
    resp = client.delete(f"/admin/users/{me.id}")
    assert resp.status_code == 400
    assert store.deleted_profiles == []


class TestUpdateUser:
    """``PATCH /admin/users/{id}`` grants and revokes super-admin access."""

    @pytest.fixture
    def mailer(self, monkeypatch: pytest.MonkeyPatch) -> list[tuple[SuperAdminPromotionEmail, str]]:
        """Configure SMTP and record every promotion email instead of sending it.

        Args:
            monkeypatch: Fixture used to swap the SMTP config and the transport.

        Returns:
            The recorded ``(email, recipient)`` pairs.
        """
        sent: list[tuple[SuperAdminPromotionEmail, str]] = []
        monkeypatch.setattr(admin_module, "get_smtp_config", lambda: SimpleNamespace(enabled=True))
        monkeypatch.setattr(SuperAdminPromotionEmail, "send", lambda email, smtp_config, to: sent.append((email, to)))
        return sent

    @staticmethod
    def _client(store: FakeStore, *, configured: list[str] | None = None) -> tuple[TestClient, SimpleNamespace]:
        """A client signed in as a stored super-admin.

        Args:
            store: The fake store the routes resolve against.
            configured: The emails ``auth.super_admin_emails`` lists.

        Returns:
            The client and the calling super-admin's row.
        """
        me = store.add_profile("me@test", name="Me", is_super_admin=True)
        app = _app(store, is_super_admin=True)
        app.dependency_overrides[get_current_user] = lambda: me
        app.dependency_overrides[get_auth_config] = lambda: SimpleNamespace(super_admin_emails=configured or [])
        return TestClient(app), me

    def test_non_super_admin_is_forbidden(self, store: FakeStore) -> None:
        resp = _client(store, is_super_admin=False).patch(
            f"/admin/users/{store.member.id}", json={"is_super_admin": True}
        )
        assert resp.status_code == 403
        assert store.member.is_super_admin is False

    def test_promotion_emails_every_super_admin(
        self, store: FakeStore, mailer: list[tuple[SuperAdminPromotionEmail, str]]
    ) -> None:
        client, _ = self._client(store)
        store.add_profile("other@test", is_super_admin=True)

        resp = client.patch(f"/admin/users/{store.member.id}", json={"is_super_admin": True})

        assert resp.status_code == 200
        assert resp.json()["is_super_admin"] is True
        assert resp.json()["organisations"] == [{"id": str(store.org.id), "name": "Acme"}]
        assert sorted(to for _, to in mailer) == ["me@test", "member@acme.test", "other@test"]
        email = mailer[0][0]
        assert (email.promoted_email, email.promoted_by) == ("member@acme.test", "Me")
        assert email.admin_url == "http://testserver/admin/users"

    def test_promoting_a_super_admin_again_sends_nothing(
        self, store: FakeStore, mailer: list[tuple[SuperAdminPromotionEmail, str]]
    ) -> None:
        client, _ = self._client(store)
        store.member.is_super_admin = True

        resp = client.patch(f"/admin/users/{store.member.id}", json={"is_super_admin": True})

        assert resp.status_code == 200
        assert mailer == []

    def test_revocation_demotes_without_email(
        self, store: FakeStore, mailer: list[tuple[SuperAdminPromotionEmail, str]]
    ) -> None:
        client, _ = self._client(store)
        store.member.is_super_admin = True

        resp = client.patch(f"/admin/users/{store.member.id}", json={"is_super_admin": False})

        assert resp.status_code == 200
        assert resp.json()["is_super_admin"] is False
        assert store.member.is_super_admin is False
        assert mailer == []

    def test_a_configured_super_admin_cannot_be_revoked(self, store: FakeStore) -> None:
        client, _ = self._client(store, configured=["member@acme.test"])
        store.member.is_super_admin = True

        resp = client.patch(f"/admin/users/{store.member.id}", json={"is_super_admin": False})

        assert resp.status_code == 409
        assert "auth.super_admin_emails" in resp.json()["detail"]
        assert store.member.is_super_admin is True

    def test_cannot_change_own_access(self, store: FakeStore) -> None:
        client, me = self._client(store)

        resp = client.patch(f"/admin/users/{me.id}", json={"is_super_admin": False})

        assert resp.status_code == 400
        assert me.is_super_admin is True

    def test_an_unknown_user_is_not_found(self, store: FakeStore) -> None:
        client, _ = self._client(store)

        resp = client.patch(f"/admin/users/{uuid4()}", json={"is_super_admin": True})

        assert resp.status_code == 404

    def test_promotion_succeeds_without_smtp(self, store: FakeStore, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(admin_module, "get_smtp_config", lambda: None)
        client, _ = self._client(store)

        resp = client.patch(f"/admin/users/{store.member.id}", json={"is_super_admin": True})

        assert resp.status_code == 200
        assert store.member.is_super_admin is True


def test_super_admin_lists_all_organisations(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/organisations")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    (org,) = body["items"]
    assert org["name"] == "Acme"
    assert org["member_count"] == 1
    # Soft-deleted organisations stay listed on the admin surface.
    assert store.listed_orgs == [OrganisationQuery(include_deleted=True)]
    assert store.counted_orgs == [[store.org.id]]


def test_list_organisations_forwards_the_page_window(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/organisations", params={"limit": 3, "offset": 6})
    assert resp.status_code == 200
    assert store.listed_orgs == [OrganisationQuery(include_deleted=True, limit=3, offset=6)]


def test_list_organisations_rejects_a_page_larger_than_the_cap(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/organisations", params={"limit": 501})
    assert resp.status_code == 422
    assert store.listed_orgs == []


# -- organisations ------------------------------------------------------------


def test_create_organisation_does_not_add_creator(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).post("/admin/organisations", json={"name": "New"})
    assert resp.status_code == 201
    assert resp.json()["name"] == "New"
    assert resp.json()["member_count"] == 0
    assert resp.json()["deleted_at"] is None


def test_non_super_admin_cannot_create_organisation(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).post("/admin/organisations", json={"name": "New"})
    assert resp.status_code == 403


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("PATCH", "/admin/organisations/{org_id}"),
        ("DELETE", "/admin/organisations/{org_id}"),
        ("GET", "/admin/organisations/{org_id}/members"),
        ("POST", "/admin/organisations/{org_id}/members"),
        ("GET", "/admin/organisations/{org_id}/invitations"),
        ("POST", "/admin/organisations/{org_id}/invitations"),
    ],
)
def test_org_management_moved_to_the_organisation_routes(store: FakeStore, method: str, path: str) -> None:
    """Managing one organisation goes through ``/organisations/{org_id}/...``, which admits super-admins."""
    resp = _client(store, is_super_admin=True).request(method, path.format(org_id=store.org.id), json={})
    assert resp.status_code == 404


def test_non_super_admin_cannot_update_quota(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).patch(f"/admin/organisations/{uuid4()}/quota", json={})
    assert resp.status_code == 403


def test_update_quota_passes_only_provided_fields(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{store.org.id}/quota",
        json={"max_sources": 5, "max_successful_runs_per_month": None},
    )
    assert resp.status_code == 200
    assert store.quota_updates == [(store.org.id, {"max_sources": 5, "max_successful_runs_per_month": None})]
    body = resp.json()
    assert body["max_sources"] == 5
    assert body["max_successful_runs_per_month"] is None


def test_update_quota_rejects_negative_limits(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{store.org.id}/quota",
        json={"max_sources": -1},
    )
    assert resp.status_code == 422
    assert store.quota_updates == []


def test_deleted_org_is_listed_but_not_manageable(store: FakeStore) -> None:
    """Soft-deleted orgs surface in the list (billing history) but 404 on management routes."""
    store.org.deleted_at = datetime.now(timezone.utc)

    def _deleted(org_id: UUID) -> None:
        raise NotFoundError(f"Organisation {org_id} not found")

    store.organisations.get = _deleted  # deleted orgs read as missing
    client = _client(store, is_super_admin=True)

    listed = client.get("/admin/organisations")
    assert listed.status_code == 200
    assert listed.json()["items"][0]["deleted_at"] is not None

    resp = client.patch(f"/admin/organisations/{store.org.id}/quota", json={"max_sources": 1})
    assert resp.status_code == 404
    assert store.quota_updates == []


def test_non_super_admin_cannot_read_activity(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get(f"/admin/organisations/{uuid4()}/activity")
    assert resp.status_code == 403


def test_activity_feed_composes_titles(store: FakeStore) -> None:
    when = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    store.activity = [
        ActivityEntry(kind="runs_completed", when=when, subject="1204"),
        ActivityEntry(kind="invitation_sent", when=when, subject="a@b.io", extra="Root"),
        ActivityEntry(kind="member_joined", when=when, subject="Jonas", extra="editor"),
        ActivityEntry(kind="org_created", when=when),
    ]

    resp = _client(store, is_super_admin=True).get(f"/admin/organisations/{store.org.id}/activity")
    assert resp.status_code == 200
    assert resp.json()["total"] == 4
    assert store.activity_calls == [(store.org.id, PageQuery())]
    titles = [(entry["kind"], entry["title"], entry["detail"]) for entry in resp.json()["items"]]
    assert titles == [
        ("runs_completed", "1,204 runs completed successfully", None),
        ("invitation_sent", "Invitation sent to a@b.io", "Invited by Root"),
        ("member_joined", "Jonas joined the organisation", "Role: editor"),
        ("org_created", "Organisation created", None),
    ]


def test_activity_feed_forwards_the_page_window(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get(
        f"/admin/organisations/{store.org.id}/activity", params={"limit": 10, "offset": 10}
    )
    assert resp.status_code == 200
    assert store.activity_calls == [(store.org.id, PageQuery(limit=10, offset=10))]


def test_quota_payload_is_derived_from_the_registry() -> None:
    """Registering a quota surfaces it in the payload with no wire-model edit."""
    assert set(admin_module._quota_limits({})) == set(QUOTAS.keys())
    assert [field.key for field in admin_module._quota_fields({})] == list(QUOTAS.keys())


def test_update_quota_rejects_an_unknown_quota(store: FakeStore) -> None:
    """A bad key is a 422 at the boundary, not a KeyError out of the store."""
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{store.org.id}/quota",
        json={"max_bananas": 5},
    )
    assert resp.status_code == 422
    assert "max_bananas" in resp.text
    assert store.quota_updates == []


# -- Activity titles ----------------------------------------------------------


class TestActivityTitles:
    """``AdminActivityEntry.from_entry`` words each derived entry kind."""

    _WHEN = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

    @classmethod
    def _title(cls, kind: str, subject: str | None = None, extra: str | None = None) -> tuple[str, str | None]:
        entry = admin_module.AdminActivityEntry.from_entry(
            ActivityEntry(kind=kind, when=cls._WHEN, subject=subject, extra=extra)
        )
        assert (entry.kind, entry.when) == (kind, cls._WHEN)
        return entry.title, entry.detail

    @pytest.mark.parametrize(
        ("kind", "subject", "extra", "expected"),
        [
            ("org_created", None, None, ("Organisation created", None)),
            ("org_deleted", None, None, ("Organisation deleted", "Retained read-only for billing history.")),
            ("member_joined", "ada@x", "admin", ("ada@x joined the organisation", "Role: admin")),
            ("member_joined", "ada@x", None, ("ada@x joined the organisation", None)),
            ("invitation_sent", "new@x", "Ada", ("Invitation sent to new@x", "Invited by Ada")),
            ("invitation_sent", "new@x", None, ("Invitation sent to new@x", None)),
            ("source_added", "facebook_ads", None, ("Source added: facebook_ads", None)),
        ],
    )
    def test_each_kind_renders(
        self, kind: str, subject: str | None, extra: str | None, expected: tuple[str, str | None]
    ) -> None:
        assert self._title(kind, subject, extra) == expected

    @pytest.mark.parametrize(
        ("count", "expected"),
        [("1", "1 run completed successfully"), ("2", "2 runs completed successfully")],
    )
    def test_the_run_count_is_pluralised(self, count: str, expected: str) -> None:
        assert self._title("runs_completed", count) == (expected, None)

    def test_large_run_counts_are_thousands_separated(self) -> None:
        title, _ = self._title("runs_completed", "12345")

        assert title == "12,345 runs completed successfully"

    def test_an_unknown_kind_falls_back_to_its_name(self) -> None:
        # A new derived kind must render as something rather than crashing.
        assert self._title("future_kind", "x") == ("future_kind", None)


# -- Config-snapshot introspection --------------------------------------------


class TestRegistryDefaults:
    """The snapshot annotates configured values with the registry's defaults."""

    def test_an_unregistered_launcher_has_no_defaults(self) -> None:
        assert admin_module._launcher_defaults("not-a-launcher") == {}

    def test_a_missing_scheduler_extra_yields_no_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # An API-only image is built without interloper-scheduler.
        monkeypatch.setitem(sys.modules, "interloper_scheduler.launcher", None)

        assert admin_module._launcher_defaults("kubernetes") == {}

    def test_a_registered_launcher_exposes_its_defaults(self) -> None:
        defaults = admin_module._launcher_defaults("in_process")

        assert isinstance(defaults, dict)

    def test_an_unregistered_runner_has_no_defaults(self) -> None:
        assert admin_module._runner_defaults("not-a-runner") == {}

    def test_a_registered_runner_exposes_its_field_defaults(self) -> None:
        defaults = admin_module._runner_defaults("async")

        assert defaults["max_workers"] == 4

    def test_an_uninstalled_package_leaves_the_version_unknown(
        self, monkeypatch: pytest.MonkeyPatch, fake_settings: SimpleNamespace
    ) -> None:
        # Running from a source checkout rather than an installed dist.
        def missing(name: str) -> str:
            raise admin_module.metadata.PackageNotFoundError(name)

        monkeypatch.setattr(admin_module.metadata, "version", missing)

        snapshot = admin_module.AdminConfigResponse.from_settings(fake_settings, {"agent": True})

        assert snapshot.deployment.version is None
