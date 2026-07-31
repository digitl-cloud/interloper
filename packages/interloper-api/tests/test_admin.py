"""Tests for ``interloper_api.routes.admin`` (super-admin cross-org surface).

The critical property is that every endpoint is gated by ``require_super_admin``
and is *not* bound to the session's active organisation. A lightweight fake
store stands in for persistence so these stay pure unit tests.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError

from interloper_api.dependencies import get_admin_config, get_current_user, get_store
from interloper_api.routes import admin as admin_module


class FakeStore:
    """In-memory stand-in implementing only the methods admin routes call."""

    def __init__(self) -> None:
        self.org = SimpleNamespace(id=uuid4(), name="Acme", created_at=datetime.now(timezone.utc))
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

    def delete_profile(self, user_id: UUID) -> None:
        self.deleted_profiles.append(user_id)

    def delete_organisation(self, org_id: UUID) -> None:
        self.deleted_organisations.append(org_id)

    # -- users --
    def list_all_profiles(self):
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
    def list_all_organisations(self):
        return [(self.org, 1)]

    def create_organisation(self, name: str, creator_id: UUID | None = None):
        return SimpleNamespace(id=uuid4(), name=name, created_at=datetime.now(timezone.utc))

    def update_organisation(self, org_id: UUID, name: str):
        return SimpleNamespace(id=org_id, name=name, created_at=self.org.created_at)

    def get_organisation(self, org_id: UUID):
        return self.org

    # -- members --
    def list_org_members(self, org_id: UUID):
        return [(self.member, "admin")]

    def add_org_member(self, org_id: UUID, user_id: UUID, role: str) -> bool:
        if self.already_member:
            return False
        self.added_members.append((org_id, user_id, role))
        return True

    def update_member_role(self, org_id: UUID, user_id: UUID, role: str) -> None:
        self.role_updates.append((org_id, user_id, role))

    def remove_org_member(self, org_id: UUID, user_id: UUID) -> None:
        self.removed.append((org_id, user_id))

    # -- invitations --
    def list_invitations(self, org_id: UUID):
        return [
            SimpleNamespace(
                id=uuid4(),
                email="invitee@acme.test",
                role="viewer",
                created_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc),
            )
        ]

    def create_invitation(self, org_id: UUID, email: str, role: str, invited_by: UUID):
        self.created_invites.append({"org_id": org_id, "email": email, "role": role})
        return SimpleNamespace(
            id=uuid4(),
            email=email,
            role=role,
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc),
        )

    def delete_invitation(self, invitation_id: UUID) -> None:
        pass


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
    snapshot = admin_module.build_config_snapshot(fake_settings, features={"agent": True})
    payload = snapshot.model_dump_json()
    secrets = (
        "oauth-secret", "smtp-secret", "pg-secret", "key-material",
        "l-secret", "nested-secret", "r-secret", "pull-secret",
    )
    for secret in secrets:
        assert secret not in payload
    assert snapshot.auth.google_oauth_configured is True
    assert snapshot.data.encryption_configured is True
    assert snapshot.services.reaper.timeout == 3600


def test_config_snapshot_allowlists_launcher_and_runner_config(fake_settings: SimpleNamespace) -> None:
    snapshot = admin_module.build_config_snapshot(fake_settings, features={"agent": True})
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
    snapshot = admin_module.build_config_snapshot(fake_settings, features={"agent": True})
    assert snapshot.deployment.runner.defaults == {"max_workers": 4}


def test_config_snapshot_reports_hydrated_catalog_by_kind(fake_settings: SimpleNamespace) -> None:
    catalog = SimpleNamespace(
        components={
            "demo": SimpleNamespace(kind="source"),
            "csv": SimpleNamespace(kind="destination"),
            "bigquery": SimpleNamespace(kind="destination"),
        }
    )
    snapshot = admin_module.build_config_snapshot(fake_settings, features={"agent": True}, catalog=catalog)
    assert snapshot.data.catalog == {"destination": ["bigquery", "csv"], "source": ["demo"]}


def test_non_super_admin_cannot_read_config(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=False).get("/admin/config")
    assert resp.status_code == 403


def test_super_admin_reads_config(store: FakeStore, fake_settings: SimpleNamespace) -> None:
    client = _client(store, is_super_admin=True)
    snapshot = admin_module.build_config_snapshot(fake_settings, features={"agent": True})
    client.app.dependency_overrides[get_admin_config] = lambda: snapshot
    resp = client.get("/admin/config")
    assert resp.status_code == 200
    assert resp.json()["deployment"]["launcher"]["type"] == "kubernetes"
    assert resp.json()["auth"]["allowed_domains"] == ["digitlcloud.com"]


def test_config_unavailable_returns_503(store: FakeStore) -> None:
    resp = _client(store, is_super_admin=True).get("/admin/config")
    assert resp.status_code == 503


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
        def update_member_role(self, org_id: UUID, user_id: UUID, role: str) -> None:
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
