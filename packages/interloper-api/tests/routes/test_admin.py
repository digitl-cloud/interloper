"""Tests for ``interloper_api.routes.admin`` (super-admin cross-org surface).

The critical property is that every endpoint is gated by ``require_super_admin``
and is *not* bound to the session's active organisation. A lightweight fake
store stands in for persistence so these stay pure unit tests.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError
from interloper_db.store.quotas import QUOTAS

from interloper_api.dependencies import get_admin_config, get_current_user, get_store
from interloper_api.routes import admin as admin_module


class FakeStore:
    """In-memory stand-in exposing only the store facets the admin routes reach for."""

    def __init__(self) -> None:
        self.org = SimpleNamespace(id=uuid4(), name="Acme", created_at=datetime.now(timezone.utc), deleted_at=None)
        self.member = SimpleNamespace(
            id=uuid4(), email="member@acme.test", name="Member", avatar_url=None
        )
        self.role_updates: list[tuple[UUID, UUID, str]] = []
        self.removed: list[tuple[UUID, UUID]] = []
        self.created_invites: list[dict] = []
        self.added_members: list[tuple[UUID, UUID, str]] = []
        self.already_member = False
        self.deleted_profiles: list[UUID] = []
        self.deleted_organisations: list[UUID] = []
        self.quota_updates: list[tuple[UUID, dict]] = []
        self.auth = SimpleNamespace(
            delete_profile=self._delete_profile,
            list_all_profiles=self._list_all_profiles,
        )
        self.organisations = SimpleNamespace(
            delete=self._delete_organisation,
            list_all=self._list_all_organisations,
            create=self._create_organisation,
            update=self._update_organisation,
            get=self._get_organisation,
            list_members=self._list_org_members,
            add_member=self._add_org_member,
            update_member_role=self._update_member_role,
            remove_member=self._remove_org_member,
            list_invitations=self._list_invitations,
            create_invitation=self._create_invitation,
            delete_invitation=self._delete_invitation,
        )
        self.quotas = SimpleNamespace(
            current_period_start=self._current_period_start,
            list_overrides=self._list_quota_overrides,
            list_usage=self._list_usage,
            count_sources_by_org=self._count_sources_by_org,
            max_assets_per_source_by_org=self._max_assets_per_source_by_org,
            count_successful_runs_by_org=self._count_successful_runs_by_org,
            set_overrides=self._set_quota,
        )

    def _delete_profile(self, user_id: UUID) -> None:
        self.deleted_profiles.append(user_id)

    def _delete_organisation(self, org_id: UUID) -> None:
        self.deleted_organisations.append(org_id)

    # -- users --
    def _list_all_profiles(self):
        return [
            (
                SimpleNamespace(
                    id=self.member.id,
                    email=self.member.email,
                    name=self.member.name,
                    avatar_url=None,
                    is_super_admin=False,
                    created_at=datetime.now(timezone.utc),
                ),
                [self.org],
            )
        ]

    # -- organisations --
    def _list_all_organisations(self):
        return [(self.org, 1)]

    def _create_organisation(self, name: str, creator_id: UUID | None = None):
        return SimpleNamespace(id=uuid4(), name=name, created_at=datetime.now(timezone.utc))

    def _update_organisation(self, org_id: UUID, name: str):
        return SimpleNamespace(id=org_id, name=name, created_at=self.org.created_at)

    def _get_organisation(self, org_id: UUID):
        return self.org

    # -- members --
    def _list_org_members(self, org_id: UUID):
        return [(self.member, "admin")]

    def _add_org_member(self, org_id: UUID, user_id: UUID, role: str) -> bool:
        if self.already_member:
            return False
        self.added_members.append((org_id, user_id, role))
        return True

    def _update_member_role(self, org_id: UUID, user_id: UUID, role: str) -> None:
        self.role_updates.append((org_id, user_id, role))

    def _remove_org_member(self, org_id: UUID, user_id: UUID) -> None:
        self.removed.append((org_id, user_id))

    # -- invitations --
    def _list_invitations(self, org_id: UUID):
        return [
            SimpleNamespace(
                id=uuid4(),
                email="invitee@acme.test",
                role="viewer",
                created_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc),
            )
        ]

    def _create_invitation(self, org_id: UUID, email: str, role: str, invited_by: UUID):
        self.created_invites.append({"org_id": org_id, "email": email, "role": role})
        return SimpleNamespace(
            id=uuid4(),
            email=email,
            role=role,
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

    def _delete_invitation(self, invitation_id: UUID) -> None:
        pass

    # -- quotas --
    def _current_period_start(self):
        import datetime as dt

        return dt.date(2026, 8, 1)

    def _list_quota_overrides(self):
        return {self.org.id: {"max_sources": 5}}

    def _list_usage(self, *, period_start=None, org_id=None):
        import datetime as dt

        return [
            SimpleNamespace(
                org_id=self.org.id,
                metric="successful_runs",
                period_start=dt.date(2026, 8, 1),
                used=7,
                reserved=1,
            )
        ]

    def _count_sources_by_org(self):
        return {self.org.id: 2}

    def _max_assets_per_source_by_org(self):
        return {self.org.id: 4}

    def _count_successful_runs_by_org(self, period_start):
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


def _client(store: FakeStore, *, is_super_admin: bool) -> TestClient:
    app = FastAPI()
    app.include_router(admin_module.router)

    @app.exception_handler(NotFoundError)
    async def _not_found(_request, exc: NotFoundError):  # mirrors create_app's handler
        from fastapi.responses import JSONResponse

        return JSONResponse(status_code=404, content={"detail": str(exc)})

    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: _profile(is_super_admin=is_super_admin)
    return TestClient(app)


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
        "oauth-secret", "smtp-secret", "pg-secret", "key-material",
        "l-secret", "nested-secret", "r-secret", "pull-secret",
        "secret-header", "collector:4317",
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
    client = _client(store, is_super_admin=True)
    snapshot = admin_module.AdminConfigResponse.from_settings(fake_settings, features={"agent": True})
    client.app.dependency_overrides[get_admin_config] = lambda: snapshot
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

    client = _client(store, is_super_admin=True)
    client.app.dependency_overrides[get_quota_defaults] = lambda: SimpleNamespace(
        max_sources=10, max_assets_per_source=20, max_successful_runs_per_month=100
    )
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

    client = _client(store, is_super_admin=True)
    client.app.dependency_overrides[get_quota_defaults] = lambda: None
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
    assert body[0]["email"] == "member@acme.test"
    assert [org["name"] for org in body[0]["organisations"]] == ["Acme"]
    assert body[0]["is_super_admin"] is False


def test_non_super_admin_cannot_delete_user(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).delete(f"/admin/users/{uuid4()}")
    assert resp.status_code == 403
    assert store.deleted_profiles == []


def test_delete_user(store: FakeStore) -> None:
    target = uuid4()
    resp = _client(store, is_super_admin=True).delete(f"/admin/users/{target}")
    assert resp.status_code == 200
    assert store.deleted_profiles == [target]


def test_cannot_delete_own_account(store: FakeStore) -> None:
    client = _client(store, is_super_admin=True)
    me = _profile(is_super_admin=True)
    client.app.dependency_overrides[get_current_user] = lambda: me
    resp = client.delete(f"/admin/users/{me.id}")
    assert resp.status_code == 400
    assert store.deleted_profiles == []


def test_super_admin_lists_all_organisations(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/organisations")
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["name"] == "Acme"
    assert body[0]["member_count"] == 1


# -- organisations ------------------------------------------------------------


def test_create_organisation_does_not_add_creator(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).post("/admin/organisations", json={"name": "New"})
    assert resp.status_code == 201
    assert resp.json()["name"] == "New"
    assert resp.json()["member_count"] == 0


def test_delete_organisation_requires_matching_name(store: FakeStore) -> None:
    client = _client(store, is_super_admin=True)
    resp = client.request("DELETE", f"/admin/organisations/{store.org.id}", json={"name": "Wrong"})
    assert resp.status_code == 400
    assert store.deleted_organisations == []


def test_delete_organisation(store: FakeStore) -> None:
    client = _client(store, is_super_admin=True)
    resp = client.request("DELETE", f"/admin/organisations/{store.org.id}", json={"name": "Acme"})
    assert resp.status_code == 200
    assert store.deleted_organisations == [store.org.id]


def test_non_super_admin_cannot_delete_organisation(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).request(
        "DELETE", f"/admin/organisations/{store.org.id}", json={"name": "Acme"}
    )
    assert resp.status_code == 403
    assert store.deleted_organisations == []


def test_rename_organisation(store: FakeStore) -> None:
    org_id = store.org.id
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{org_id}", json={"name": "Renamed"}
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "Renamed"


# -- members ------------------------------------------------------------------


def test_list_members_of_any_org(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get(f"/admin/organisations/{store.org.id}/members")
    assert resp.status_code == 200
    assert resp.json()[0]["email"] == "member@acme.test"


def test_update_member_role(store: FakeStore) -> None:
    user_id = uuid4()
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{store.org.id}/members/{user_id}", json={"role": "editor"}
    )
    assert resp.status_code == 200
    assert store.role_updates[0][2] == "editor"


def test_update_member_role_rejects_invalid_role(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{store.org.id}/members/{uuid4()}", json={"role": "root"}
    )
    assert resp.status_code == 400


def test_missing_member_maps_to_404() -> None:
    # Store mutations raise NotFoundError; the app-level handler turns it into 404.
    class RaisingStore(FakeStore):
        def _update_member_role(self, org_id: UUID, user_id: UUID, role: str) -> None:
            raise NotFoundError(f"User {user_id} is not a member of organisation {org_id}")

    store = RaisingStore()
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{store.org.id}/members/{uuid4()}", json={"role": "editor"}
    )
    assert resp.status_code == 404


def test_join_organisation_without_invitation(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).post(
        f"/admin/organisations/{store.org.id}/members", json={"role": "admin"}
    )
    assert resp.status_code == 201
    assert resp.json()["role"] == "admin"
    assert store.added_members[0][0] == store.org.id
    assert store.created_invites == []


def test_join_organisation_conflicts_when_already_member(store: FakeStore) -> None:
    store.already_member = True
    resp = _client(store, is_super_admin=True).post(
        f"/admin/organisations/{store.org.id}/members", json={"role": "admin"}
    )
    assert resp.status_code == 409


def test_join_organisation_rejects_invalid_role(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).post(
        f"/admin/organisations/{store.org.id}/members", json={"role": "root"}
    )
    assert resp.status_code == 400


def test_remove_member(store: FakeStore) -> None:
    user_id = uuid4()
    resp = _client(store, is_super_admin=True).delete(
        f"/admin/organisations/{store.org.id}/members/{user_id}"
    )
    assert resp.status_code == 200
    assert store.removed[0][1] == user_id


# -- invitations --------------------------------------------------------------


def test_invite_into_any_org(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).post(
        f"/admin/organisations/{store.org.id}/invitations",
        json={"email": "x@acme.test", "role": "viewer"},
    )
    assert resp.status_code == 201
    assert store.created_invites[0]["email"] == "x@acme.test"


def test_non_super_admin_cannot_update_quota(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).patch(f"/admin/organisations/{uuid4()}/quota", json={})
    assert resp.status_code == 403


def test_update_quota_passes_only_provided_fields(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).patch(
        f"/admin/organisations/{store.org.id}/quota",
        json={"max_sources": 5, "max_successful_runs_per_month": None},
    )
    assert resp.status_code == 200
    assert store.quota_updates == [
        (store.org.id, {"max_sources": 5, "max_successful_runs_per_month": None})
    ]
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
    assert listed.json()[0]["deleted_at"] is not None

    resp = client.patch(f"/admin/organisations/{store.org.id}/quota", json={"max_sources": 1})
    assert resp.status_code == 404
    assert store.quota_updates == []


def test_non_super_admin_cannot_read_activity(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get(f"/admin/organisations/{uuid4()}/activity")
    assert resp.status_code == 403


def test_activity_feed_composes_titles(store: FakeStore) -> None:
    when = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    store.activity = [  # ty: ignore[unresolved-attribute]
        {"kind": "runs_completed", "when": when, "subject": "1204", "extra": None},
        {"kind": "invitation_sent", "when": when, "subject": "a@b.io", "extra": "Root"},
        {"kind": "member_joined", "when": when, "subject": "Jonas", "extra": "editor"},
        {"kind": "org_created", "when": when, "subject": None, "extra": None},
    ]
    store.organisations.list_activity = lambda org_id: store.activity  # ty: ignore[unresolved-attribute]

    resp = _client(store, is_super_admin=True).get(f"/admin/organisations/{store.org.id}/activity")
    assert resp.status_code == 200
    titles = [(entry["kind"], entry["title"], entry["detail"]) for entry in resp.json()]
    assert titles == [
        ("runs_completed", "1,204 runs completed successfully", None),
        ("invitation_sent", "Invitation sent to a@b.io", "Invited by Root"),
        ("member_joined", "Jonas joined the organisation", "Role: editor"),
        ("org_created", "Organisation created", None),
    ]


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


# -- Cross-org invitations -----------------------------------------------------


def test_list_invitations_404s_before_touching_anything(store: FakeStore) -> None:
    """The org is resolved first, so an unknown id never reaches the invitations."""

    def missing(org_id):
        raise NotFoundError(f"Organisation {org_id} not found")

    store.organisations.get = missing

    resp = _client(store, is_super_admin=True).get(f"/admin/organisations/{uuid4()}/invitations")

    assert resp.status_code == 404


def test_list_invitations_returns_the_orgs_pending_ones(store: FakeStore) -> None:
    """A super-admin sees any organisation's outstanding invitations."""
    invitation_id = uuid4()
    store.organisations.list_invitations = lambda org_id: [
        SimpleNamespace(
            id=invitation_id,
            email="new@acme.test",
            role="viewer",
            created_at=None,
            expires_at=datetime.now(timezone.utc),
        )
    ]

    resp = _client(store, is_super_admin=True).get(f"/admin/organisations/{store.org.id}/invitations")

    assert resp.status_code == 200
    assert [row["id"] for row in resp.json()] == [str(invitation_id)]


def test_cancel_invitation_deletes_it(store: FakeStore) -> None:
    """The invitation id alone identifies the row; the org only scopes the path."""
    deleted: list[UUID] = []
    store.organisations.delete_invitation = deleted.append
    invitation_id = uuid4()

    resp = _client(store, is_super_admin=True).delete(
        f"/admin/organisations/{store.org.id}/invitations/{invitation_id}"
    )

    assert resp.json() == {"status": "ok"}
    assert deleted == [invitation_id]


# -- Activity titles ----------------------------------------------------------


class TestActivityTitles:
    """``_activity_title`` renders each derived entry kind."""

    @pytest.mark.parametrize(
        ("entry", "expected"),
        [
            ({"kind": "org_created", "subject": "", "extra": None}, ("Organisation created", None)),
            (
                {"kind": "org_deleted", "subject": "", "extra": None},
                ("Organisation deleted", "Retained read-only for billing history."),
            ),
            (
                {"kind": "member_joined", "subject": "ada@x", "extra": "admin"},
                ("ada@x joined the organisation", "Role: admin"),
            ),
            ({"kind": "member_joined", "subject": "ada@x", "extra": None}, ("ada@x joined the organisation", None)),
            (
                {"kind": "invitation_sent", "subject": "new@x", "extra": "Ada"},
                ("Invitation sent to new@x", "Invited by Ada"),
            ),
            ({"kind": "invitation_sent", "subject": "new@x", "extra": None}, ("Invitation sent to new@x", None)),
            ({"kind": "source_added", "subject": "facebook_ads", "extra": None}, ("Source added — facebook_ads", None)),
        ],
    )
    def test_each_kind_renders(self, entry: dict, expected: tuple[str, str | None]) -> None:
        assert admin_module._activity_title(entry) == expected

    @pytest.mark.parametrize(
        ("count", "expected"),
        [("1", "1 run completed successfully"), ("2", "2 runs completed successfully")],
    )
    def test_the_run_count_is_pluralised(self, count: str, expected: str) -> None:
        title, detail = admin_module._activity_title({"kind": "runs_completed", "subject": count, "extra": None})

        assert (title, detail) == (expected, None)

    def test_large_run_counts_are_thousands_separated(self) -> None:
        title, _ = admin_module._activity_title({"kind": "runs_completed", "subject": "12345", "extra": None})

        assert title == "12,345 runs completed successfully"

    def test_an_unknown_kind_falls_back_to_its_name(self) -> None:
        # A new derived kind must render as something rather than crashing.
        assert admin_module._activity_title({"kind": "future_kind", "subject": "x", "extra": None}) == (
            "future_kind",
            None,
        )


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


# -- Invitation email ----------------------------------------------------------


class TestAdminInvitationEmail:
    """``_send_invitation_email`` reads the SMTP config itself and never raises.

    Unlike the organisations route's copy, this one looks the config up from
    the process state rather than taking it as an argument.
    """

    @staticmethod
    def _invitation() -> SimpleNamespace:
        return SimpleNamespace(token="tok", email="new@acme.test")

    @staticmethod
    def _request() -> Request:
        """Build a request stand-in carrying only the base URL.

        Returns:
            The stand-in, typed as a ``Request`` for the helper under test.
        """
        return cast(Request, SimpleNamespace(base_url="https://app.example.com/"))

    @pytest.fixture
    def smtp(self, monkeypatch: pytest.MonkeyPatch):
        """Install an SMTP config into the process state.

        Args:
            monkeypatch: Fixture used to set the state slot.

        Returns:
            A callable taking the config to install.
        """
        from interloper_api.dependencies import state as state_module

        def install(config) -> None:
            monkeypatch.setattr(state_module, "_smtp_config", config)

        return install

    def test_an_unconfigured_mailer_is_logged_and_skipped(
        self, smtp, caplog: pytest.LogCaptureFixture
    ) -> None:
        smtp(None)

        with caplog.at_level("WARNING", logger="interloper_api.routes.admin"):
            admin_module._send_invitation_email(self._request(), self._invitation(), "Acme", "Ada")

        assert "SMTP not configured" in caplog.text

    def test_a_disabled_mailer_is_treated_as_unconfigured(
        self, smtp, caplog: pytest.LogCaptureFixture
    ) -> None:
        smtp(SimpleNamespace(enabled=False))

        with caplog.at_level("WARNING", logger="interloper_api.routes.admin"):
            admin_module._send_invitation_email(self._request(), self._invitation(), "Acme", "Ada")

        assert "SMTP not configured" in caplog.text

    def test_a_configured_mailer_gets_the_invite_url(
        self, smtp, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        built: list[dict] = []

        class FakeEmail:
            def __init__(self, **kwargs) -> None:
                built.append(kwargs)

            def send(self, smtp_config, email) -> None:
                built[-1]["sent_to"] = email

        smtp(SimpleNamespace(enabled=True))
        monkeypatch.setattr(admin_module, "InvitationEmail", FakeEmail)

        admin_module._send_invitation_email(self._request(), self._invitation(), "Acme", "Ada")

        assert built[0]["invite_url"] == "https://app.example.com/invite/tok"
        assert built[0]["org_name"] == "Acme"
        assert built[0]["sent_to"] == "new@acme.test"

    def test_a_mailer_failure_is_logged_not_raised(
        self, smtp, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        class FailingEmail:
            def __init__(self, **kwargs) -> None:
                pass

            def send(self, smtp_config, email) -> None:
                raise OSError("smtp unreachable")

        smtp(SimpleNamespace(enabled=True))
        monkeypatch.setattr(admin_module, "InvitationEmail", FailingEmail)

        with caplog.at_level("ERROR", logger="interloper_api.routes.admin"):
            admin_module._send_invitation_email(self._request(), self._invitation(), "Acme", "Ada")

        assert "Failed to send invitation email to new@acme.test" in caplog.text
