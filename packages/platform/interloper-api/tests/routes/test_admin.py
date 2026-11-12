"""Tests for ``interloper_api.routes.admin`` (super-admin platform-wide surface).

The critical property is that every endpoint is gated by ``require_super_admin``
and is *not* bound to the session's active organisation. A lightweight fake
store stands in for persistence so these stay pure unit tests.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.catalog import Catalog
from interloper.component import ComponentDefinition
from interloper.settings import SETTINGS, Settings
from interloper_db import NotFoundError, OrganisationQuery, Page, PageQuery, ProfileQuery, Store, UsageQuery
from interloper_db.crypto import hash_token
from interloper_db.models import Audience, Conversation
from interloper_db.organisation_keys import derive_key
from interloper_db.session import session_scope
from interloper_db.store.insights import ActivityEntry
from interloper_db.store.quotas import QUOTAS

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import State, get_session_caller, get_settings, get_store
from interloper_api.notifications import SuperAdminPromotionEmail
from interloper_api.routes import admin as admin_module
from interloper_api.settings import ApiSettings


class FakeStore:
    """In-memory stand-in exposing only the store facets the admin routes reach for."""

    def __init__(self) -> None:
        self.org = SimpleNamespace(id=uuid4(), name="Acme", key="acme", created_at=datetime.now(UTC), deleted_at=None)
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
        self.tokens = SimpleNamespace(resolve=lambda raw, audience: None)
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
            created_at=datetime.now(UTC),
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
        return SimpleNamespace(
            id=uuid4(), name=name, key=derive_key(name), created_at=datetime.now(UTC), deleted_at=None
        )

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


def _app(store: FakeStore | Store, *, is_super_admin: bool) -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(admin_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_session_caller] = lambda: _profile(is_super_admin=is_super_admin)
    return app


def _client(store: FakeStore | Store, *, is_super_admin: bool) -> TestClient:
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


def _section(snapshot: admin_module.AdminConfigResponse, name: str) -> admin_module.AdminSettingsSection:
    """The snapshot's section of that name.

    Args:
        snapshot: The config snapshot.
        name: The section's name.

    Returns:
        The section.
    """
    return next(section for section in snapshot.sections if section.name == name)


def _field(section: admin_module.AdminSettingsSection, name: str) -> admin_module.AdminSettingsField:
    """The section's field of that name.

    Args:
        section: The snapshot's section.
        name: The field's name, dotted for a key below a mapping field.

    Returns:
        The field.
    """
    return next(field for field in section.fields if field.name == name)


def test_config_snapshot_redacts_secrets(fake_settings: ApiSettings) -> None:
    snapshot = admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=Catalog())
    payload = snapshot.model_dump_json()
    secrets = (
        "oauth-secret",
        "smtp-secret",
        "pg-secret",
        "key-material",
        "r-secret",
        "secret-header",
        "collector:4317",
    )
    for secret in secrets:
        assert secret not in payload
    secret = _field(_section(snapshot, "auth"), "google_client_secret")
    assert (secret.public, secret.configured, secret.value) == (False, True, None)
    assert _field(_section(snapshot, "encryption"), "key").configured is True
    assert _field(_section(snapshot, "otel"), "endpoint").configured is True


def test_config_snapshot_shows_every_declared_section(fake_settings: ApiSettings) -> None:
    snapshot = admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=Catalog())

    assert [section.name for section in snapshot.sections] == sorted(Settings.declared())
    assert (_section(snapshot, "auth").extension, _section(snapshot, "server").extension) == ("api", "core")
    assert _section(snapshot, "smtp").description == "SMTP settings for sending invitation emails."


def test_config_snapshot_shows_the_values_of_public_fields(fake_settings: ApiSettings) -> None:
    snapshot = admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=Catalog())

    assert _field(_section(snapshot, "auth"), "allowed_domains").value == ["digitlcloud.com"]
    assert _field(_section(snapshot, "quota"), "max_successful_runs_per_month").value == 25
    assert _field(_section(snapshot, "otel"), "sample_ratio").value == 1.0


def test_config_snapshot_reaches_public_keys_of_a_mapping_and_hides_the_rest(fake_settings: ApiSettings) -> None:
    runner = _section(
        admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=Catalog()), "runner"
    )

    assert _field(runner, "config.max_workers").value == 8
    config = _field(runner, "config")
    assert (config.public, config.configured, config.value) == (False, True, None)
    assert "config.image" not in {field.name for field in runner.fields}


def test_config_snapshot_reports_whether_a_field_is_configured(fake_settings: ApiSettings) -> None:
    snapshot = admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=Catalog())
    smtp = _section(snapshot, "smtp")

    assert _field(smtp, "host").configured is True
    assert _field(smtp, "port").configured is False


def test_config_snapshot_leaves_out_a_section_whose_package_cannot_be_imported(
    fake_settings: ApiSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(admin_module, "SETTINGS", {name: cls for name, cls in SETTINGS.items() if name != "smtp"})

    snapshot = admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=Catalog())

    assert "smtp" not in {section.name for section in snapshot.sections}


def test_config_snapshot_reports_hydrated_catalog_by_kind(fake_settings: ApiSettings) -> None:
    catalog = Catalog(
        components={
            key: ComponentDefinition(kind=kind, key=key, path=f"tests:{key}", name=key)
            for key, kind in (("demo", "source"), ("csv", "destination"), ("bigquery", "destination"))
        }
    )
    snapshot = admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=catalog)
    assert snapshot.catalog == {"destination": ["bigquery", "csv"], "source": ["demo"]}


def test_config_snapshot_leaves_the_version_unknown_without_an_installed_package(
    monkeypatch: pytest.MonkeyPatch, fake_settings: ApiSettings
) -> None:
    # Running from a source checkout rather than an installed dist.
    def missing(name: str) -> str:
        raise admin_module.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(admin_module.metadata, "version", missing)

    assert admin_module.AdminConfigResponse.from_declared({"agent": True}, Catalog()).version is None


def test_non_super_admin_cannot_read_config(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get("/admin/config")
    assert resp.status_code == 403


def test_super_admin_reads_config(store: FakeStore, fake_settings: ApiSettings, app_state: State) -> None:
    app = _app(store, is_super_admin=True)
    snapshot = admin_module.AdminConfigResponse.from_declared(features={"agent": True}, catalog=Catalog())
    app.state.interloper = dataclasses.replace(app_state, admin_config=snapshot)
    client = TestClient(app)
    resp = client.get("/admin/config")
    assert resp.status_code == 200
    auth = next(section for section in resp.json()["sections"] if section["name"] == "auth")
    assert next(field for field in auth["fields"] if field["name"] == "allowed_domains")["value"] == ["digitlcloud.com"]


def test_non_super_admin_cannot_read_quotas(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get("/admin/quotas")
    assert resp.status_code == 403


def test_super_admin_reads_quota_overview(store: FakeStore) -> None:
    app = _app(store, is_super_admin=True)
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(
        quota=SimpleNamespace(max_sources=10, max_assets_per_source=20, max_successful_runs_per_month=100)
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
    app = _app(store, is_super_admin=True)
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(quota=None)
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


@pytest.mark.parametrize("path", ["/admin/organisations", "/admin/users"])
def test_a_super_admin_token_is_refused_and_the_session_cookie_passes(store: FakeStore, path: str) -> None:
    app = _app(store, is_super_admin=True)
    app.dependency_overrides.pop(get_session_caller)
    store.tokens = SimpleNamespace(resolve=lambda raw, audience: (_profile(is_super_admin=True), SimpleNamespace()))
    client = TestClient(app)

    refused = client.get(path, headers={"Authorization": "Bearer ilp_x"})
    client.cookies.set("session_token", "cookie")
    passed = client.get(path)

    assert refused.status_code == 401
    assert passed.status_code == 200


def test_delete_user(store: FakeStore) -> None:
    target = uuid4()
    resp = _client(store, is_super_admin=True).delete(f"/admin/users/{target}")
    assert resp.status_code == 204
    assert resp.content == b""
    assert store.deleted_profiles == [target]


def test_cannot_delete_own_account(store: FakeStore) -> None:
    app = _app(store, is_super_admin=True)
    me = _profile(is_super_admin=True)
    app.dependency_overrides[get_session_caller] = lambda: me
    client = TestClient(app)
    resp = client.delete(f"/admin/users/{me.id}")
    assert resp.status_code == 400
    assert store.deleted_profiles == []


_SMTP_ENABLED = SimpleNamespace(enabled=True)


class TestUpdateUser:
    """``PATCH /admin/users/{id}`` grants and revokes super-admin access."""

    @pytest.fixture
    def mailer(self, monkeypatch: pytest.MonkeyPatch) -> list[tuple[SuperAdminPromotionEmail, str]]:
        """Record every promotion email instead of sending it.

        Args:
            monkeypatch: Fixture used to swap the transport.

        Returns:
            The recorded ``(email, recipient)`` pairs.
        """
        sent: list[tuple[SuperAdminPromotionEmail, str]] = []
        monkeypatch.setattr(SuperAdminPromotionEmail, "send", lambda email, smtp_config, to: sent.append((email, to)))
        return sent

    @staticmethod
    def _client(
        store: FakeStore, *, configured: list[str] | None = None, smtp: Any = _SMTP_ENABLED
    ) -> tuple[TestClient, SimpleNamespace]:
        """A client signed in as a stored super-admin.

        Args:
            store: The fake store the routes resolve against.
            configured: The emails ``auth.super_admin_emails`` lists.
            smtp: The mailer configuration, enabled by default.

        Returns:
            The client and the calling super-admin's row.
        """
        me = store.add_profile("me@test", name="Me", is_super_admin=True)
        app = _app(store, is_super_admin=True)
        app.dependency_overrides[get_session_caller] = lambda: me
        settings = SimpleNamespace(auth=SimpleNamespace(super_admin_emails=configured or []), smtp=smtp)
        app.dependency_overrides[get_settings] = lambda: settings
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

    def test_promotion_succeeds_without_smtp(self, store: FakeStore) -> None:
        client, _ = self._client(store, smtp=None)

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
    assert org["key"] == "acme"
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
    store.org.deleted_at = datetime.now(UTC)

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
    when = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
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

    _WHEN = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)

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


class TestOAuthClients:
    """The operator OAuth clients, over a real store so the delete cascade is the one the CLI runs."""

    def test_non_super_admins_are_refused(self, oauth_store: Store) -> None:
        operator, _ = oauth_store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )
        client = _client(oauth_store, is_super_admin=False)

        listed = client.get("/admin/oauth-clients")
        created = client.post("/admin/oauth-clients", json={"name": "X", "redirect_uris": ["https://x.example/cb"]})
        deleted = client.delete(f"/admin/oauth-clients/{operator.client_id}")

        assert [listed.status_code, created.status_code, deleted.status_code] == [403, 403, 403]
        assert [row.name for row in oauth_store.oauth.list_clients()] == ["Slack"]

    def test_lists_operator_clients_only_without_secret_material(self, oauth_store: Store) -> None:
        oauth_store.oauth.register_client("Claude Code", ["http://localhost/cb"], registered_by="dynamic")
        operator, secret = oauth_store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )

        response = _client(oauth_store, is_super_admin=True).get("/admin/oauth-clients")

        assert response.status_code == 200
        [row] = response.json()
        assert row["id"] == operator.client_id
        assert row["name"] == "Slack"
        assert row["redirect_uris"] == ["https://slack.example.com/cb"]
        assert row["client_host"] is None
        assert set(row) == {"id", "name", "client_host", "redirect_uris", "created_at"}
        assert secret is not None
        assert secret not in response.text
        assert str(operator.secret_hash) not in response.text

    def test_create_returns_the_secret_once(self, oauth_store: Store) -> None:
        client = _client(oauth_store, is_super_admin=True)

        response = client.post(
            "/admin/oauth-clients",
            json={"name": "  Slack  ", "redirect_uris": ["https://slack.example.com/cb", "cursor://cb"]},
        )

        assert response.status_code == 201
        body = response.json()
        stored = oauth_store.oauth.get_client(body["id"])
        assert stored is not None
        assert stored.name == "Slack"
        assert stored.registered_by == "operator"
        assert stored.redirect_uris == ["https://slack.example.com/cb", "cursor://cb"]
        assert body["client_secret"].startswith("ilc_")
        assert stored.secret_hash == hash_token(body["client_secret"])
        assert body["client_secret"] not in client.get("/admin/oauth-clients").text

    @pytest.mark.parametrize(
        "redirect_uris", [[], ["javascript:alert(1)"], ["http://slack.example.com/cb"], ["https://ok.example/cb", "x"]]
    )
    def test_create_refuses_what_registration_refuses(self, oauth_store: Store, redirect_uris: list[str]) -> None:
        response = _client(oauth_store, is_super_admin=True).post(
            "/admin/oauth-clients", json={"name": "Slack", "redirect_uris": redirect_uris}
        )

        assert response.status_code == 422
        assert oauth_store.oauth.list_clients() == []

    @pytest.mark.parametrize("name", ["", "   ", "x" * 101])
    def test_create_refuses_a_blank_or_overlong_name(self, oauth_store: Store, name: str) -> None:
        response = _client(oauth_store, is_super_admin=True).post(
            "/admin/oauth-clients", json={"name": name, "redirect_uris": ["https://slack.example.com/cb"]}
        )

        assert response.status_code == 422
        assert oauth_store.oauth.list_clients() == []

    def test_delete_ends_every_grant_the_client_holds(self, oauth_store: Store) -> None:
        client = _client(oauth_store, is_super_admin=True)
        client_id = client.post(
            "/admin/oauth-clients", json={"name": "Slack", "redirect_uris": ["https://slack.example.com/cb"]}
        ).json()["id"]
        user = oauth_store.profiles.upsert(google_id="g-user", email="user@example.com", name="User")
        organisation = oauth_store.organisations.create(name="Acme", creator_id=user.id)
        access = oauth_store.tokens.create_access(user.id, client_id, Audience.API)
        refresh = oauth_store.oauth.issue_refresh(user.id, client_id, Audience.API)
        conversation = oauth_store.conversations.create(organisation.id, user.id, client_id)

        response = client.delete(f"/admin/oauth-clients/{client_id}")

        assert response.status_code == 204
        assert oauth_store.oauth.get_client(client_id) is None
        assert oauth_store.tokens.resolve(access, Audience.API) is None
        assert oauth_store.oauth.find_refresh(refresh) is None
        with session_scope(oauth_store.engine) as session:
            assert session.get(Conversation, conversation.id) is None

    def test_delete_never_reaches_a_self_registered_client(self, oauth_store: Store) -> None:
        dynamic, _ = oauth_store.oauth.register_client("Claude Code", ["http://localhost/cb"], registered_by="dynamic")
        client = _client(oauth_store, is_super_admin=True)

        assert client.delete(f"/admin/oauth-clients/{dynamic.client_id}").status_code == 404
        assert client.delete("/admin/oauth-clients/no-such-client").status_code == 404
        assert oauth_store.oauth.get_client(dynamic.client_id) is not None
