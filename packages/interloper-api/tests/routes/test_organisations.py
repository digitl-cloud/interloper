"""Tests for ``interloper_api.routes.organisations``.

Every route below the collection takes the organisation from the path and
serves its own members by role and the platform's super-admins through the
same endpoints. The authorization is the real ``authorize_organisation``,
resolved against the fake store's memberships, so each route is proven for
an org admin, a member with too low a role, a non-member and a super-admin.
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError
from interloper_db import OrganisationQuery, Page, PageQuery

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import get_current_user, get_store
from interloper_api.notifications import InvitationEmail
from interloper_api.routes import organisations as organisations_module

_ORG_ID = uuid4()
_OTHER_ORG_ID = uuid4()
_USER_ID = uuid4()
_SUPER_ADMIN_ID = uuid4()
_NOW = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)


def _profile(
    user_id: UUID = _USER_ID,
    *,
    name: str | None = "Ada",
    email: str = "ada@example.com",
    is_super_admin: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(id=user_id, email=email, name=name, avatar_url=None, is_super_admin=is_super_admin)


def _super_admin() -> SimpleNamespace:
    return _profile(_SUPER_ADMIN_ID, name="Root", email="root@example.com", is_super_admin=True)


def _invitation(
    invitation_id: UUID, email: str = "new@example.com", role: str = "viewer", org_id: UUID = _ORG_ID
) -> SimpleNamespace:
    return SimpleNamespace(
        id=invitation_id,
        org_id=org_id,
        email=email,
        role=role,
        token=f"token-{invitation_id}",
        created_at=_NOW,
        expires_at=_NOW + dt.timedelta(days=7),
    )


class FakeStore:
    """In-memory stand-in exposing only the store facets these routes reach for.

    The caller (``_USER_ID``) is an admin of ``_ORG_ID`` unless a test
    changes ``roles``; ``_OTHER_ORG_ID`` exists but the caller is not in it.
    """

    def __init__(self) -> None:
        """Set up the recorders and the default fixture data."""
        self.orgs: dict[UUID, SimpleNamespace] = {
            _ORG_ID: SimpleNamespace(id=_ORG_ID, name="Dev Org", created_at=_NOW),
            _OTHER_ORG_ID: SimpleNamespace(id=_OTHER_ORG_ID, name="Other Org", created_at=_NOW),
        }
        self.roles: dict[tuple[UUID, UUID], str] = {(_ORG_ID, _USER_ID): "admin"}
        self.profiles: dict[UUID, SimpleNamespace] = {_USER_ID: _profile()}
        self.invitation_rows: list[SimpleNamespace] = []
        self.user_orgs: list[SimpleNamespace] = []

        self.created_orgs: list[tuple[str, UUID | None]] = []
        self.renamed_orgs: list[tuple[UUID, str]] = []
        self.deleted_orgs: list[UUID] = []
        self.listed_orgs: list[OrganisationQuery] = []
        self.session_org_calls: list[tuple[str, UUID, UUID]] = []
        self.added_members: list[tuple[UUID, UUID, str]] = []
        self.role_updates: list[tuple[UUID, UUID, str]] = []
        self.removed_members: list[tuple[UUID, UUID]] = []
        self.listed_members: list[tuple[UUID, PageQuery]] = []
        self.listed_invitations: list[tuple[UUID, PageQuery]] = []
        self.created_invitations: list[dict[str, Any]] = []
        self.deleted_invitations: list[UUID] = []
        self.reissued: list[tuple[UUID, UUID, UUID]] = []

        self.organisations = SimpleNamespace(
            create=self._create_org,
            list=self._list_orgs,
            get=self._get_org,
            update=self._update_org,
            delete=self._delete_org,
        )
        self.members = SimpleNamespace(
            role=lambda org_id, user_id: self.roles.get((org_id, user_id)),
            list=self._list_members,
            add=self._add_member,
            update=self._update_member,
            delete=self._delete_member,
        )
        self.invitations = SimpleNamespace(
            list=self._list_invitations,
            create=self._create_invitation,
            delete=self._delete_invitation,
            reissue=self._reissue_invitation,
        )
        self.sessions = SimpleNamespace(switch_org=self._switch_org)

    # -- organisations --

    def _create_org(self, name: str, creator_id: UUID | None = None) -> SimpleNamespace:
        self.created_orgs.append((name, creator_id))
        return SimpleNamespace(id=_ORG_ID, name=name, created_at=_NOW)

    def _list_orgs(self, query: OrganisationQuery) -> Page:
        self.listed_orgs.append(query)
        return Page.window(self.user_orgs, query)

    def _get_org(self, org_id: UUID) -> SimpleNamespace:
        if org_id not in self.orgs:
            raise NotFoundError(f"Organisation {org_id} not found")
        return self.orgs[org_id]

    def _update_org(self, org_id: UUID, *, name: str) -> SimpleNamespace:
        self.renamed_orgs.append((org_id, name))
        return SimpleNamespace(id=org_id, name=name, created_at=_NOW)

    def _delete_org(self, org_id: UUID) -> None:
        self.deleted_orgs.append(org_id)

    def _switch_org(self, token: str, org_id: UUID, user_id: UUID) -> None:
        self.session_org_calls.append((token, org_id, user_id))

    # -- members --

    def _membership(self, org_id: UUID, user_id: UUID) -> SimpleNamespace:
        return SimpleNamespace(profile=self.profiles[user_id], role=self.roles[(org_id, user_id)])

    def _list_members(self, org_id: UUID, query: PageQuery) -> Page:
        self.listed_members.append((org_id, query))
        memberships = [self._membership(org, user) for org, user in self.roles if org == org_id]
        return Page.window(memberships, query)

    def _add_member(self, org_id: UUID, user_id: UUID, role: str) -> bool:
        if (org_id, user_id) in self.roles:
            return False
        self.added_members.append((org_id, user_id, role))
        return True

    def _update_member(self, org_id: UUID, user_id: UUID, role: str) -> SimpleNamespace:
        if (org_id, user_id) not in self.roles:
            raise NotFoundError(f"User {user_id} is not a member of organisation {org_id}")
        self.role_updates.append((org_id, user_id, role))
        self.roles[(org_id, user_id)] = role
        return self._membership(org_id, user_id)

    def _delete_member(self, org_id: UUID, user_id: UUID) -> None:
        self.removed_members.append((org_id, user_id))

    # -- invitations --

    def _find_invitation(self, invitation_id: UUID, org_id: UUID) -> SimpleNamespace:
        for invitation in self.invitation_rows:
            if invitation.id == invitation_id and invitation.org_id == org_id:
                return invitation
        raise NotFoundError(f"Invitation {invitation_id} not found")

    def _list_invitations(self, org_id: UUID, query: PageQuery) -> Page:
        self.listed_invitations.append((org_id, query))
        return Page.window([row for row in self.invitation_rows if row.org_id == org_id], query)

    def _create_invitation(self, org_id: UUID, *, email: str, role: str, invited_by: UUID) -> SimpleNamespace:
        self.created_invitations.append({"org_id": org_id, "email": email, "role": role, "invited_by": invited_by})
        return _invitation(uuid4(), email=email, role=role, org_id=org_id)

    def _delete_invitation(self, invitation_id: UUID, *, org_id: UUID) -> None:
        self._find_invitation(invitation_id, org_id)
        self.deleted_invitations.append(invitation_id)

    def _reissue_invitation(self, invitation_id: UUID, *, org_id: UUID, invited_by: UUID) -> SimpleNamespace:
        previous = self._find_invitation(invitation_id, org_id)
        self.reissued.append((invitation_id, org_id, invited_by))
        replacement = _invitation(uuid4(), email=previous.email, role=previous.role, org_id=org_id)
        replacement.expires_at = previous.expires_at + dt.timedelta(days=3)
        self.invitation_rows = [row for row in self.invitation_rows if row is not previous] + [replacement]
        return replacement


@pytest.fixture
def store() -> FakeStore:
    """A fresh fake store for each test.

    Returns:
        The fake store.
    """
    return FakeStore()


@pytest.fixture
def no_smtp(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default the route tests to email being unconfigured.

    Args:
        monkeypatch: Fixture used to stub the SMTP lookup.
    """
    monkeypatch.setattr(organisations_module, "get_smtp_config", lambda: None)


@pytest.fixture
def mailer(monkeypatch: pytest.MonkeyPatch) -> list[tuple[InvitationEmail, str]]:
    """Configure email and record every invitation handed to SMTP instead of sending it.

    Args:
        monkeypatch: Fixture used to stub the SMTP lookup and the send.

    Returns:
        The ``(email, recipient)`` pairs sent, in order.
    """
    sent: list[tuple[InvitationEmail, str]] = []
    monkeypatch.setattr(organisations_module, "get_smtp_config", lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr(InvitationEmail, "send", lambda email, smtp_config, to: sent.append((email, to)))
    return sent


@pytest.fixture
def app(store: FakeStore, no_smtp: None) -> FastAPI:
    """Mount the organisations router, the caller being ``_USER_ID``.

    Args:
        store: The fake store the routes resolve against.
        no_smtp: Leaves email unconfigured unless a test says otherwise.

    Returns:
        The probe app.
    """
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(organisations_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: _profile()
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """A client calling as ``_USER_ID``, an admin of ``_ORG_ID``.

    Args:
        app: The probe app.

    Returns:
        The client.
    """
    return TestClient(app)


@pytest.fixture
def super_client(app: FastAPI) -> TestClient:
    """A client calling as a super-admin who belongs to no organisation.

    Args:
        app: The probe app.

    Returns:
        The client.
    """
    app.dependency_overrides[get_current_user] = lambda: _super_admin()
    return TestClient(app)


# -- Organisations -------------------------------------------------------------


class TestCreateOrganisation:
    """``POST /organisations``: the creator becomes its admin."""

    def test_creates_and_returns_the_organisation(self, client: TestClient, store: FakeStore) -> None:
        response = client.post("/organisations", json={"name": "Acme"})

        assert response.status_code == 201
        assert response.json() == {"id": str(_ORG_ID), "name": "Acme", "created_at": "2026-06-01T00:00:00Z"}
        assert store.created_orgs == [("Acme", _USER_ID)]

    def test_the_new_org_becomes_the_sessions_active_one(self, client: TestClient, store: FakeStore) -> None:
        client.cookies.set("session_token", "tok")

        client.post("/organisations", json={"name": "Acme"})

        assert store.session_org_calls == [("tok", _ORG_ID, _USER_ID)]

    def test_without_a_session_cookie_no_org_is_selected(self, client: TestClient, store: FakeStore) -> None:
        client.post("/organisations", json={"name": "Acme"})

        assert store.session_org_calls == []

    def test_a_missing_name_is_rejected(self, client: TestClient) -> None:
        assert client.post("/organisations", json={}).status_code == 422


class TestListOrganisations:
    """``GET /organisations``: scoped to the caller's memberships."""

    def test_lists_the_users_organisations(self, client: TestClient, store: FakeStore) -> None:
        store.user_orgs = [
            SimpleNamespace(id=_ORG_ID, name="Dev Org", created_at=_NOW),
            SimpleNamespace(id=uuid4(), name="Other", created_at=None),
        ]

        response = client.get("/organisations")

        assert response.status_code == 200
        assert [org["name"] for org in response.json()["items"]] == ["Dev Org", "Other"]
        assert response.json()["total"] == 2
        assert store.listed_orgs == [OrganisationQuery(user_id=_USER_ID)]

    def test_a_user_with_no_memberships_gets_an_empty_page(self, client: TestClient) -> None:
        assert client.get("/organisations").json() == {"items": [], "total": 0}

    def test_the_page_window_is_forwarded(self, client: TestClient, store: FakeStore) -> None:
        client.get("/organisations", params={"limit": 5, "offset": 10})

        assert store.listed_orgs == [OrganisationQuery(user_id=_USER_ID, limit=5, offset=10)]

    def test_a_page_larger_than_the_cap_is_a_422(self, client: TestClient, store: FakeStore) -> None:
        assert client.get("/organisations", params={"limit": 501}).status_code == 422
        assert store.listed_orgs == []


class TestUpdateOrganisation:
    """``PATCH /organisations/{org_id}``: the org's admin or a super-admin renames it."""

    def test_an_org_admin_renames_their_organisation(self, client: TestClient, store: FakeStore) -> None:
        response = client.patch(f"/organisations/{_ORG_ID}", json={"name": "Renamed"})

        assert response.status_code == 200
        assert response.json() == {"id": str(_ORG_ID), "name": "Renamed", "created_at": "2026-06-01T00:00:00Z"}
        assert store.renamed_orgs == [(_ORG_ID, "Renamed")]

    def test_an_editor_cannot_rename(self, client: TestClient, store: FakeStore) -> None:
        store.roles[(_ORG_ID, _USER_ID)] = "editor"

        response = client.patch(f"/organisations/{_ORG_ID}", json={"name": "Renamed"})

        assert response.status_code == 403
        assert response.json()["detail"] == "Requires admin role or higher"
        assert store.renamed_orgs == []

    def test_a_non_member_reads_the_organisation_as_missing(self, client: TestClient, store: FakeStore) -> None:
        response = client.patch(f"/organisations/{_OTHER_ORG_ID}", json={"name": "Renamed"})

        assert response.status_code == 404
        assert response.json()["detail"] == f"Organisation {_OTHER_ORG_ID} not found"
        assert store.renamed_orgs == []

    def test_a_super_admin_renames_any_organisation(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.patch(f"/organisations/{_OTHER_ORG_ID}", json={"name": "Renamed"})

        assert response.status_code == 200
        assert response.json()["name"] == "Renamed"
        assert store.renamed_orgs == [(_OTHER_ORG_ID, "Renamed")]

    def test_a_super_admin_still_gets_a_404_on_a_missing_organisation(
        self, super_client: TestClient, store: FakeStore
    ) -> None:
        missing = uuid4()

        response = super_client.patch(f"/organisations/{missing}", json={"name": "Renamed"})

        assert response.status_code == 404
        assert response.json()["detail"] == f"Organisation {missing} not found"
        assert store.renamed_orgs == []


class TestDeleteOrganisation:
    """``DELETE /organisations/{org_id}``: super-admin only, confirmed by the exact name."""

    def test_a_super_admin_deletes_with_the_matching_name(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.request("DELETE", f"/organisations/{_ORG_ID}", json={"name": "Dev Org"})

        assert response.status_code == 204
        assert response.content == b""
        assert store.deleted_orgs == [_ORG_ID]

    def test_a_mismatched_name_is_refused(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.request("DELETE", f"/organisations/{_ORG_ID}", json={"name": "Wrong"})

        assert response.status_code == 400
        assert response.json()["detail"] == "Organisation name does not match"
        assert store.deleted_orgs == []

    def test_a_missing_organisation_is_a_404(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.request("DELETE", f"/organisations/{uuid4()}", json={"name": "Dev Org"})

        assert response.status_code == 404
        assert store.deleted_orgs == []

    def test_an_org_admin_who_is_not_a_super_admin_is_refused(self, client: TestClient, store: FakeStore) -> None:
        response = client.request("DELETE", f"/organisations/{_ORG_ID}", json={"name": "Dev Org"})

        assert response.status_code == 403
        assert store.deleted_orgs == []


# -- Members -------------------------------------------------------------------


class TestListMembers:
    """``GET /organisations/{org_id}/members``: each member with the role they hold."""

    def test_lists_members_with_their_roles(self, client: TestClient, store: FakeStore) -> None:
        other_id = uuid4()
        store.profiles[other_id] = SimpleNamespace(id=other_id, email="bob@example.com", name=None, avatar_url="u")
        store.roles[(_ORG_ID, other_id)] = "viewer"

        response = client.get(f"/organisations/{_ORG_ID}/members")

        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {
                    "id": str(_USER_ID),
                    "email": "ada@example.com",
                    "name": "Ada",
                    "avatar_url": None,
                    "role": "admin",
                },
                {
                    "id": str(other_id),
                    "email": "bob@example.com",
                    "name": None,
                    "avatar_url": "u",
                    "role": "viewer",
                },
            ],
            "total": 2,
        }
        assert store.listed_members == [(_ORG_ID, PageQuery())]

    def test_any_member_may_list(self, client: TestClient, store: FakeStore) -> None:
        store.roles[(_ORG_ID, _USER_ID)] = "viewer"

        assert client.get(f"/organisations/{_ORG_ID}/members").status_code == 200

    def test_a_non_member_reads_the_organisation_as_missing(self, client: TestClient, store: FakeStore) -> None:
        response = client.get(f"/organisations/{_OTHER_ORG_ID}/members")

        assert response.status_code == 404
        assert store.listed_members == []

    def test_a_super_admin_lists_any_organisations_members(
        self, super_client: TestClient, store: FakeStore
    ) -> None:
        response = super_client.get(f"/organisations/{_ORG_ID}/members", params={"limit": 1, "offset": 0})

        assert response.status_code == 200
        assert [member["email"] for member in response.json()["items"]] == ["ada@example.com"]
        assert store.listed_members == [(_ORG_ID, PageQuery(limit=1, offset=0))]


class TestJoinOrganisation:
    """``POST /organisations/{org_id}/members``: a super-admin joins without an invitation."""

    def test_a_super_admin_joins_as_admin_by_default(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.post(f"/organisations/{_ORG_ID}/members", json={})

        assert response.status_code == 201
        assert response.json() == {
            "id": str(_SUPER_ADMIN_ID),
            "email": "root@example.com",
            "name": "Root",
            "avatar_url": None,
            "role": "admin",
        }
        assert store.added_members == [(_ORG_ID, _SUPER_ADMIN_ID, "admin")]
        assert store.created_invitations == []

    def test_the_role_can_be_chosen(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.post(f"/organisations/{_ORG_ID}/members", json={"role": "viewer"})

        assert response.json()["role"] == "viewer"
        assert store.added_members == [(_ORG_ID, _SUPER_ADMIN_ID, "viewer")]

    def test_an_existing_member_conflicts(self, super_client: TestClient, store: FakeStore) -> None:
        store.roles[(_ORG_ID, _SUPER_ADMIN_ID)] = "viewer"

        response = super_client.post(f"/organisations/{_ORG_ID}/members", json={"role": "admin"})

        assert response.status_code == 409
        assert response.json()["detail"] == "Already a member of this organisation"

    def test_an_invalid_role_is_rejected(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.post(f"/organisations/{_ORG_ID}/members", json={"role": "root"})

        assert response.status_code == 422
        assert store.added_members == []

    def test_a_missing_organisation_is_a_404(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.post(f"/organisations/{uuid4()}/members", json={"role": "admin"})

        assert response.status_code == 404
        assert store.added_members == []

    def test_an_org_admin_cannot_join_another_organisation(self, client: TestClient, store: FakeStore) -> None:
        response = client.post(f"/organisations/{_OTHER_ORG_ID}/members", json={"role": "admin"})

        assert response.status_code == 403
        assert store.added_members == []


class TestUpdateMember:
    """``PATCH /organisations/{org_id}/members/{user_id}``: the org's admin or a super-admin."""

    @pytest.fixture
    def target(self, store: FakeStore) -> UUID:
        """A viewer of ``_ORG_ID`` whose role the tests change.

        Args:
            store: The fake store the member is added to.

        Returns:
            The member's id.
        """
        target = uuid4()
        store.profiles[target] = SimpleNamespace(id=target, email="bob@example.com", name="Bob", avatar_url=None)
        store.roles[(_ORG_ID, target)] = "viewer"
        return target

    def test_an_org_admin_changes_a_members_role(self, client: TestClient, store: FakeStore, target: UUID) -> None:
        response = client.patch(f"/organisations/{_ORG_ID}/members/{target}", json={"role": "editor"})

        assert response.status_code == 200
        assert response.json() == {
            "id": str(target),
            "email": "bob@example.com",
            "name": "Bob",
            "avatar_url": None,
            "role": "editor",
        }
        assert store.role_updates == [(_ORG_ID, target, "editor")]

    def test_a_super_admin_changes_a_members_role(
        self, super_client: TestClient, store: FakeStore, target: UUID
    ) -> None:
        response = super_client.patch(f"/organisations/{_ORG_ID}/members/{target}", json={"role": "admin"})

        assert response.status_code == 200
        assert response.json()["role"] == "admin"
        assert store.role_updates == [(_ORG_ID, target, "admin")]

    def test_an_editor_cannot_change_roles(self, client: TestClient, store: FakeStore, target: UUID) -> None:
        store.roles[(_ORG_ID, _USER_ID)] = "editor"

        response = client.patch(f"/organisations/{_ORG_ID}/members/{target}", json={"role": "admin"})

        assert response.status_code == 403
        assert store.role_updates == []

    def test_an_invalid_role_is_rejected(self, client: TestClient, store: FakeStore, target: UUID) -> None:
        response = client.patch(f"/organisations/{_ORG_ID}/members/{target}", json={"role": "root"})

        assert response.status_code == 422
        assert store.role_updates == []

    def test_a_missing_member_is_a_404(self, client: TestClient, store: FakeStore) -> None:
        response = client.patch(f"/organisations/{_ORG_ID}/members/{uuid4()}", json={"role": "editor"})

        assert response.status_code == 404
        assert store.role_updates == []


class TestRemoveMember:
    """``DELETE /organisations/{org_id}/members/{user_id}``: the org's admin or a super-admin."""

    def test_removes_the_member(self, client: TestClient, store: FakeStore) -> None:
        target = uuid4()

        response = client.delete(f"/organisations/{_ORG_ID}/members/{target}")

        assert response.status_code == 204
        assert response.content == b""
        assert store.removed_members == [(_ORG_ID, target)]

    def test_removing_yourself_is_refused(self, client: TestClient, store: FakeStore) -> None:
        # Otherwise an admin can lock the organisation out of its own admin seat.
        response = client.delete(f"/organisations/{_ORG_ID}/members/{_USER_ID}")

        assert response.status_code == 400
        assert response.json()["detail"] == "Cannot remove yourself"
        assert store.removed_members == []

    def test_a_super_admin_removes_a_member_of_any_organisation(
        self, super_client: TestClient, store: FakeStore
    ) -> None:
        response = super_client.delete(f"/organisations/{_ORG_ID}/members/{_USER_ID}")

        assert response.status_code == 204
        assert store.removed_members == [(_ORG_ID, _USER_ID)]

    def test_an_editor_cannot_remove_members(self, client: TestClient, store: FakeStore) -> None:
        store.roles[(_ORG_ID, _USER_ID)] = "editor"

        response = client.delete(f"/organisations/{_ORG_ID}/members/{uuid4()}")

        assert response.status_code == 403
        assert store.removed_members == []


# -- Invitations ---------------------------------------------------------------


class TestListInvitations:
    """``GET /organisations/{org_id}/invitations``: the org's admin or a super-admin."""

    def test_lists_the_outstanding_invitations(self, client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [
            _invitation(invitation_id, email="new@example.com", role="editor"),
            _invitation(uuid4(), org_id=_OTHER_ORG_ID),
        ]

        response = client.get(f"/organisations/{_ORG_ID}/invitations")

        assert response.status_code == 200
        payload = response.json()
        assert payload["total"] == 1
        [row] = payload["items"]
        assert row["id"] == str(invitation_id)
        assert row["email"] == "new@example.com"
        assert row["role"] == "editor"
        # The token is never exposed; only the mailed link carries it.
        assert "token" not in row
        assert store.listed_invitations == [(_ORG_ID, PageQuery())]

    def test_none_outstanding_is_an_empty_page(self, client: TestClient) -> None:
        assert client.get(f"/organisations/{_ORG_ID}/invitations").json() == {"items": [], "total": 0}

    def test_a_viewer_cannot_see_invitations(self, client: TestClient, store: FakeStore) -> None:
        store.roles[(_ORG_ID, _USER_ID)] = "viewer"

        assert client.get(f"/organisations/{_ORG_ID}/invitations").status_code == 403
        assert store.listed_invitations == []

    def test_a_super_admin_sees_any_organisations_invitations(
        self, super_client: TestClient, store: FakeStore
    ) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id, org_id=_OTHER_ORG_ID)]

        response = super_client.get(f"/organisations/{_OTHER_ORG_ID}/invitations")

        assert response.status_code == 200
        assert [row["id"] for row in response.json()["items"]] == [str(invitation_id)]

    def test_a_missing_organisation_404s_before_touching_anything(
        self, super_client: TestClient, store: FakeStore
    ) -> None:
        response = super_client.get(f"/organisations/{uuid4()}/invitations")

        assert response.status_code == 404
        assert store.listed_invitations == []


class TestInviteMember:
    """``POST /organisations/{org_id}/invitations``: the invitation outlives a missing mailer."""

    def test_creates_the_invitation(self, client: TestClient, store: FakeStore) -> None:
        response = client.post(
            f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com", "role": "editor"}
        )

        assert response.status_code == 201
        assert response.json()["email"] == "new@example.com"
        assert response.json()["role"] == "editor"
        assert "token" not in response.json()
        assert store.created_invitations == [
            {"org_id": _ORG_ID, "email": "new@example.com", "role": "editor", "invited_by": _USER_ID}
        ]

    def test_the_role_defaults_to_viewer(self, client: TestClient, store: FakeStore) -> None:
        client.post(f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com"})

        assert store.created_invitations[0]["role"] == "viewer"

    def test_the_address_is_trimmed(self, client: TestClient, store: FakeStore) -> None:
        client.post(f"/organisations/{_ORG_ID}/invitations", json={"email": "  new@example.com  "})

        assert store.created_invitations[0]["email"] == "new@example.com"

    def test_a_super_admin_invites_into_any_organisation(self, super_client: TestClient, store: FakeStore) -> None:
        response = super_client.post(
            f"/organisations/{_OTHER_ORG_ID}/invitations", json={"email": "x@acme.test", "role": "viewer"}
        )

        assert response.status_code == 201
        assert store.created_invitations == [
            {"org_id": _OTHER_ORG_ID, "email": "x@acme.test", "role": "viewer", "invited_by": _SUPER_ADMIN_ID}
        ]

    def test_an_editor_cannot_invite(self, client: TestClient, store: FakeStore) -> None:
        store.roles[(_ORG_ID, _USER_ID)] = "editor"

        response = client.post(f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com"})

        assert response.status_code == 403
        assert store.created_invitations == []

    def test_an_unconfigured_mailer_still_creates_the_invitation(
        self, client: TestClient, store: FakeStore, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING", logger="interloper_api.notifications.invitations"):
            response = client.post(f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com"})

        assert response.status_code == 201
        assert "SMTP not configured" in caplog.text

    def test_a_disabled_mailer_is_treated_as_unconfigured(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(organisations_module, "get_smtp_config", lambda: SimpleNamespace(enabled=False))

        with caplog.at_level("WARNING", logger="interloper_api.notifications.invitations"):
            response = client.post(f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com"})

        assert response.status_code == 201
        assert "SMTP not configured" in caplog.text

    def test_a_configured_mailer_is_handed_the_invite_url(
        self, client: TestClient, mailer: list[tuple[InvitationEmail, str]]
    ) -> None:
        client.post(f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com"})

        [(email, to)] = mailer
        assert to == "new@example.com"
        assert email.org_name == "Dev Org"
        assert email.inviter_name == "Ada"
        assert email.invite_url.startswith("http://testserver/invite/token-")

    def test_a_nameless_inviter_is_identified_by_email(
        self, app: FastAPI, client: TestClient, mailer: list[tuple[InvitationEmail, str]]
    ) -> None:
        app.dependency_overrides[get_current_user] = lambda: _profile(name=None)

        client.post(f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com"})

        assert [email.inviter_name for email, _ in mailer] == ["ada@example.com"]

    def test_an_unknown_role_is_rejected(self, client: TestClient, store: FakeStore) -> None:
        response = client.post(
            f"/organisations/{_ORG_ID}/invitations", json={"email": "new@example.com", "role": "owner"}
        )

        assert response.status_code == 422
        assert store.created_invitations == []


class TestCancelInvitation:
    """``DELETE /organisations/{org_id}/invitations/{id}``: scoped to the path's organisation."""

    def test_cancels_an_invitation_of_this_org(self, client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id)]

        response = client.delete(f"/organisations/{_ORG_ID}/invitations/{invitation_id}")

        assert response.status_code == 204
        assert response.content == b""
        assert store.deleted_invitations == [invitation_id]

    def test_an_invitation_of_another_org_is_a_404(self, client: TestClient, store: FakeStore) -> None:
        # The id must not act as a cross-org handle.
        theirs = uuid4()
        store.invitation_rows = [_invitation(theirs, org_id=_OTHER_ORG_ID)]

        response = client.delete(f"/organisations/{_ORG_ID}/invitations/{theirs}")

        assert response.status_code == 404
        assert response.json()["detail"] == f"Invitation {theirs} not found"
        assert store.deleted_invitations == []

    def test_a_super_admin_cancels_within_the_path_org(self, super_client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id, org_id=_OTHER_ORG_ID)]

        response = super_client.delete(f"/organisations/{_OTHER_ORG_ID}/invitations/{invitation_id}")

        assert response.status_code == 204
        assert store.deleted_invitations == [invitation_id]


class TestResendInvitation:
    """``POST /organisations/{org_id}/invitations/{id}/resend``: reissues with a fresh token and expiry."""

    def test_returns_the_reissued_invitation(self, client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id, email="new@example.com", role="editor")]

        response = client.post(f"/organisations/{_ORG_ID}/invitations/{invitation_id}/resend")

        assert response.status_code == 201
        [replacement] = store.invitation_rows
        assert response.json() == {
            "id": str(replacement.id),
            "email": "new@example.com",
            "role": "editor",
            "created_at": "2026-06-01T00:00:00Z",
            "expires_at": "2026-06-11T00:00:00Z",
        }
        assert replacement.id != invitation_id
        assert store.reissued == [(invitation_id, _ORG_ID, _USER_ID)]

    def test_a_super_admin_resends_an_invitation_of_any_organisation(
        self, super_client: TestClient, store: FakeStore
    ) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id, org_id=_OTHER_ORG_ID)]

        response = super_client.post(f"/organisations/{_OTHER_ORG_ID}/invitations/{invitation_id}/resend")

        assert response.status_code == 201
        assert response.json()["id"] != str(invitation_id)
        assert store.reissued == [(invitation_id, _OTHER_ORG_ID, _SUPER_ADMIN_ID)]

    def test_an_invitation_of_another_org_is_a_404(self, client: TestClient, store: FakeStore) -> None:
        theirs = uuid4()
        store.invitation_rows = [_invitation(theirs, org_id=_OTHER_ORG_ID)]

        response = client.post(f"/organisations/{_ORG_ID}/invitations/{theirs}/resend")

        assert response.status_code == 404
        assert store.reissued == []

    def test_a_viewer_cannot_resend(self, client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id)]
        store.roles[(_ORG_ID, _USER_ID)] = "viewer"

        response = client.post(f"/organisations/{_ORG_ID}/invitations/{invitation_id}/resend")

        assert response.status_code == 403
        assert store.reissued == []

    def test_an_unconfigured_mailer_still_reissues(
        self, client: TestClient, store: FakeStore, caplog: pytest.LogCaptureFixture
    ) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id)]

        with caplog.at_level("WARNING", logger="interloper_api.notifications.invitations"):
            response = client.post(f"/organisations/{_ORG_ID}/invitations/{invitation_id}/resend")

        assert response.status_code == 201
        assert "SMTP not configured" in caplog.text

    def test_a_configured_mailer_gets_the_new_invitation(
        self, client: TestClient, store: FakeStore, mailer: list[tuple[InvitationEmail, str]]
    ) -> None:
        invitation_id = uuid4()
        store.invitation_rows = [_invitation(invitation_id, email="new@example.com")]

        client.post(f"/organisations/{_ORG_ID}/invitations/{invitation_id}/resend")

        # The reissued token, not the one that was just replaced.
        [(email, to)] = mailer
        [replacement] = store.invitation_rows
        assert to == "new@example.com"
        assert email.invite_url == f"http://testserver/invite/token-{replacement.id}"
