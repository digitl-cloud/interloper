"""Tests for ``interloper_api.routes.organisations``.

A lightweight fake store stands in for persistence so these stay pure unit
tests, matching the style of ``test_admin.py`` and ``test_runs.py``.
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

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import (
    get_current_user,
    get_org_id,
    get_store,
    require_admin,
    require_viewer,
)
from interloper_api.notifications import InvitationEmail
from interloper_api.routes import organisations as organisations_module

_ORG_ID = uuid4()
_USER_ID = uuid4()
_NOW = dt.datetime(2026, 6, 1, tzinfo=dt.timezone.utc)


def _invitation(invitation_id: UUID, email: str = "new@example.com", role: str = "viewer") -> SimpleNamespace:
    return SimpleNamespace(
        id=invitation_id,
        email=email,
        role=role,
        token=f"token-{invitation_id}",
        created_at=_NOW,
        expires_at=_NOW + dt.timedelta(days=7),
    )


class FakeStore:
    """In-memory stand-in exposing only the store facets these routes reach for."""

    def __init__(self) -> None:
        """Set up the recorders and the default fixture data."""
        self.created_orgs: list[tuple[str, UUID]] = []
        self.session_org_calls: list[tuple[str, UUID, UUID]] = []
        self.removed_members: list[tuple[UUID, UUID]] = []
        self.deleted_invitations: list[UUID] = []
        self.created_invitations: list[dict[str, Any]] = []
        self.invitations: list[SimpleNamespace] = []
        self.members: list[tuple[SimpleNamespace, str]] = []
        self.user_orgs: list[SimpleNamespace] = []
        self.org_name = "Dev Org"

        self.organisations = SimpleNamespace(
            create=self._create,
            list_for_user=lambda user_id: self.user_orgs,
            list_members=lambda org_id: self.members,
            remove_member=self._remove_member,
            list_invitations=lambda org_id: self.invitations,
            create_invitation=self._create_invitation,
            get_invitation=self._get_invitation,
            delete_invitation=self._delete_invitation,
            get=lambda org_id: SimpleNamespace(id=org_id, name=self.org_name),
            member_role=lambda user_id, org_id: "admin",
        )
        self.auth = SimpleNamespace(set_session_org=self._set_session_org)

    def _create(self, name: str, creator_id: UUID) -> SimpleNamespace:
        self.created_orgs.append((name, creator_id))
        return SimpleNamespace(id=_ORG_ID, name=name, created_at=_NOW)

    def _set_session_org(self, token: str, org_id: UUID, user_id: UUID) -> None:
        self.session_org_calls.append((token, org_id, user_id))

    def _remove_member(self, org_id: UUID, user_id: UUID) -> None:
        self.removed_members.append((org_id, user_id))

    def _get_invitation(self, invitation_id: UUID, *, org_id: UUID) -> SimpleNamespace:
        found = next((invitation for invitation in self.invitations if invitation.id == invitation_id), None)
        if found is None or org_id != _ORG_ID:
            raise NotFoundError(f"Invitation {invitation_id} not found")
        return found

    def _delete_invitation(self, invitation_id: UUID, *, org_id: UUID) -> None:
        self._get_invitation(invitation_id, org_id=org_id)
        self.deleted_invitations.append(invitation_id)

    def _create_invitation(self, org_id: UUID, email: str, role: str, invited_by: UUID) -> SimpleNamespace:
        self.created_invitations.append(
            {"org_id": org_id, "email": email, "role": role, "invited_by": invited_by}
        )
        return _invitation(uuid4(), email=email, role=role)


def _profile(name: str | None = "Ada") -> SimpleNamespace:
    return SimpleNamespace(
        id=_USER_ID,
        email="ada@example.com",
        name=name,
        avatar_url=None,
        is_super_admin=False,
    )


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
    """Mount the organisations router with every gate satisfied.

    The role gates are overridden wholesale; ``test_rbac.py`` owns proving
    that they refuse the wrong role.

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
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
    app.dependency_overrides[get_current_user] = _profile
    app.dependency_overrides[require_viewer] = _profile
    app.dependency_overrides[require_admin] = _profile
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """A client for the probe app.

    Args:
        app: The probe app.

    Returns:
        The client.
    """
    return TestClient(app)


class TestCreateOrganisation:
    """``POST /organisations`` — the creator becomes its admin."""

    def test_creates_and_returns_the_organisation(self, client: TestClient, store: FakeStore) -> None:
        response = client.post("/organisations", json={"name": "Acme"})

        assert response.status_code == 201
        assert response.json()["name"] == "Acme"
        assert store.created_orgs == [("Acme", _USER_ID)]

    def test_the_new_org_becomes_the_sessions_active_one(
        self, client: TestClient, store: FakeStore
    ) -> None:
        client.cookies.set("session_token", "tok")

        client.post("/organisations", json={"name": "Acme"})

        assert store.session_org_calls == [("tok", _ORG_ID, _USER_ID)]

    def test_without_a_session_cookie_no_org_is_selected(
        self, client: TestClient, store: FakeStore
    ) -> None:
        client.post("/organisations", json={"name": "Acme"})

        assert store.session_org_calls == []

    def test_a_missing_name_is_rejected(self, client: TestClient) -> None:
        assert client.post("/organisations", json={}).status_code == 422


class TestListOrganisations:
    """``GET /organisations`` — scoped to the caller's memberships."""

    def test_lists_the_users_organisations(self, client: TestClient, store: FakeStore) -> None:
        store.user_orgs = [
            SimpleNamespace(id=_ORG_ID, name="Dev Org", created_at=_NOW),
            SimpleNamespace(id=uuid4(), name="Other", created_at=None),
        ]

        response = client.get("/organisations")

        assert [org["name"] for org in response.json()] == ["Dev Org", "Other"]

    def test_a_user_with_no_memberships_gets_an_empty_list(self, client: TestClient) -> None:
        assert client.get("/organisations").json() == []


class TestListMembers:
    """``GET /organisations/members`` — each member with the role they hold."""

    def test_lists_members_with_their_roles(self, client: TestClient, store: FakeStore) -> None:
        other_id = uuid4()
        store.members = [
            (_profile(), "admin"),
            (SimpleNamespace(id=other_id, email="bob@example.com", name=None, avatar_url="u"), "viewer"),
        ]

        response = client.get("/organisations/members")

        assert response.json() == [
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
        ]


class TestRemoveMember:
    """``DELETE /organisations/members/{user_id}`` — admin only."""

    def test_removes_the_member(self, client: TestClient, store: FakeStore) -> None:
        target = uuid4()

        response = client.delete(f"/organisations/members/{target}")

        assert response.json() == {"status": "ok"}
        assert store.removed_members == [(_ORG_ID, target)]

    def test_removing_yourself_is_refused(self, client: TestClient, store: FakeStore) -> None:
        # Otherwise an admin can lock the organisation out of its own admin seat.
        response = client.delete(f"/organisations/members/{_USER_ID}")

        assert response.status_code == 400
        assert response.json()["detail"] == "Cannot remove yourself"
        assert store.removed_members == []


class TestListInvitations:
    """``GET /organisations/invitations`` — admin only."""

    def test_lists_the_outstanding_invitations(self, client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitations = [_invitation(invitation_id, email="new@example.com", role="editor")]

        response = client.get("/organisations/invitations")

        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["id"] == str(invitation_id)
        assert payload[0]["email"] == "new@example.com"
        assert payload[0]["role"] == "editor"
        # The token is never exposed; only the mailed link carries it.
        assert "token" not in payload[0]

    def test_none_outstanding_is_an_empty_list(self, client: TestClient) -> None:
        assert client.get("/organisations/invitations").json() == []


class TestInviteMember:
    """``POST /organisations/invite`` — the invitation outlives a missing mailer."""

    def test_creates_the_invitation(self, client: TestClient, store: FakeStore) -> None:
        response = client.post("/organisations/invite", json={"email": "new@example.com", "role": "editor"})

        assert response.status_code == 201
        assert response.json()["email"] == "new@example.com"
        assert store.created_invitations == [
            {"org_id": _ORG_ID, "email": "new@example.com", "role": "editor", "invited_by": _USER_ID}
        ]

    def test_the_role_defaults_to_viewer(self, client: TestClient, store: FakeStore) -> None:
        client.post("/organisations/invite", json={"email": "new@example.com"})

        assert store.created_invitations[0]["role"] == "viewer"

    def test_the_address_is_trimmed(self, client: TestClient, store: FakeStore) -> None:
        client.post("/organisations/invite", json={"email": "  new@example.com  "})

        assert store.created_invitations[0]["email"] == "new@example.com"

    def test_an_unconfigured_mailer_still_creates_the_invitation(
        self, client: TestClient, store: FakeStore, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING", logger="interloper_api.notifications.invitations"):
            response = client.post("/organisations/invite", json={"email": "new@example.com"})

        assert response.status_code == 201
        assert "SMTP not configured" in caplog.text

    def test_a_disabled_mailer_is_treated_as_unconfigured(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(organisations_module, "get_smtp_config", lambda: SimpleNamespace(enabled=False))

        with caplog.at_level("WARNING", logger="interloper_api.notifications.invitations"):
            assert client.post("/organisations/invite", json={"email": "new@example.com"}).status_code == 201

        assert "SMTP not configured" in caplog.text

    def test_a_configured_mailer_is_handed_the_invite_url(
        self, client: TestClient, mailer: list[tuple[InvitationEmail, str]]
    ) -> None:
        client.post("/organisations/invite", json={"email": "new@example.com"})

        [(email, to)] = mailer
        assert to == "new@example.com"
        assert email.org_name == "Dev Org"
        assert email.inviter_name == "Ada"
        assert email.invite_url.startswith("http://testserver/invite/token-")

    def test_a_nameless_inviter_is_identified_by_email(
        self, app: FastAPI, client: TestClient, mailer: list[tuple[InvitationEmail, str]]
    ) -> None:
        app.dependency_overrides[require_admin] = lambda: _profile(name=None)

        client.post("/organisations/invite", json={"email": "new@example.com"})

        assert [email.inviter_name for email, _ in mailer] == ["ada@example.com"]

    def test_an_unknown_role_is_rejected(self, client: TestClient, store: FakeStore) -> None:
        response = client.post("/organisations/invite", json={"email": "new@example.com", "role": "owner"})

        assert response.status_code == 422
        assert store.created_invitations == []


class TestCancelInvitation:
    """``DELETE /organisations/invitations/{id}`` — scoped to the active org."""

    def test_cancels_an_invitation_of_this_org(self, client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitations = [_invitation(invitation_id)]

        response = client.delete(f"/organisations/invitations/{invitation_id}")

        assert response.json() == {"status": "ok"}
        assert store.deleted_invitations == [invitation_id]

    def test_an_invitation_of_another_org_is_a_404(self, client: TestClient, store: FakeStore) -> None:
        # The id must not act as a cross-org handle.
        store.invitations = [_invitation(uuid4())]

        missing = uuid4()

        response = client.delete(f"/organisations/invitations/{missing}")

        assert response.status_code == 404
        assert response.json()["detail"] == f"Invitation {missing} not found"
        assert store.deleted_invitations == []


class TestResendInvitation:
    """``POST /organisations/invitations/{id}/resend`` — reissues with fresh expiry."""

    def test_replaces_the_invitation(self, client: TestClient, store: FakeStore) -> None:
        invitation_id = uuid4()
        store.invitations = [_invitation(invitation_id, email="new@example.com", role="editor")]

        response = client.post(f"/organisations/invitations/{invitation_id}/resend")

        assert response.json() == {"status": "ok"}
        # The old link stops working, and the replacement keeps address and role.
        assert store.deleted_invitations == [invitation_id]
        assert store.created_invitations == [
            {"org_id": _ORG_ID, "email": "new@example.com", "role": "editor", "invited_by": _USER_ID}
        ]

    def test_an_invitation_of_another_org_is_a_404(self, client: TestClient, store: FakeStore) -> None:
        store.invitations = [_invitation(uuid4())]

        response = client.post(f"/organisations/invitations/{uuid4()}/resend")

        assert response.status_code == 404
        assert store.deleted_invitations == []
        assert store.created_invitations == []

    def test_an_unconfigured_mailer_still_reissues(
        self, client: TestClient, store: FakeStore, caplog: pytest.LogCaptureFixture
    ) -> None:
        invitation_id = uuid4()
        store.invitations = [_invitation(invitation_id)]

        with caplog.at_level("WARNING", logger="interloper_api.notifications.invitations"):
            assert client.post(f"/organisations/invitations/{invitation_id}/resend").status_code == 200

        assert "SMTP not configured" in caplog.text

    def test_a_configured_mailer_gets_the_new_invitation(
        self, client: TestClient, store: FakeStore, mailer: list[tuple[InvitationEmail, str]]
    ) -> None:
        invitation_id = uuid4()
        store.invitations = [_invitation(invitation_id, email="new@example.com")]

        client.post(f"/organisations/invitations/{invitation_id}/resend")

        # The reissued token, not the one that was just deleted.
        [(email, to)] = mailer
        assert to == "new@example.com"
        assert not email.invite_url.endswith(f"token-{invitation_id}")
