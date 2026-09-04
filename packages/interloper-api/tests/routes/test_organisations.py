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

from interloper_api.dependencies import (
    get_current_user,
    get_org_id,
    get_store,
    require_admin,
    require_viewer,
)
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
            delete_invitation=self.deleted_invitations.append,
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

    Scoped to ``client`` rather than autouse, so ``TestSmtpLookup`` still
    sees the real lookup it is there to probe.

    Args:
        monkeypatch: Fixture used to stub the SMTP lookup.
    """
    monkeypatch.setattr(organisations_module, "_get_smtp_config", lambda: None)


@pytest.fixture
def client(store: FakeStore, no_smtp: None) -> TestClient:
    """Mount the organisations router with every gate satisfied.

    The role gates are overridden wholesale; ``test_rbac.py`` owns proving
    that they refuse the wrong role.

    Args:
        store: The fake store the routes resolve against.
        no_smtp: Leaves email unconfigured unless a test says otherwise.

    Returns:
        A client for the probe app.
    """
    app = FastAPI()
    app.include_router(organisations_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_org_id] = lambda: _ORG_ID
    app.dependency_overrides[get_current_user] = _profile
    app.dependency_overrides[require_viewer] = _profile
    app.dependency_overrides[require_admin] = _profile
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
        with caplog.at_level("WARNING", logger="interloper_api.routes.organisations"):
            response = client.post("/organisations/invite", json={"email": "new@example.com"})

        assert response.status_code == 201
        assert "SMTP not configured" in caplog.text

    def test_a_disabled_mailer_is_treated_as_unconfigured(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(
            organisations_module, "_get_smtp_config", lambda: SimpleNamespace(enabled=False)
        )

        with caplog.at_level("WARNING", logger="interloper_api.routes.organisations"):
            assert client.post("/organisations/invite", json={"email": "new@example.com"}).status_code == 201

        assert "SMTP not configured" in caplog.text

    def test_a_configured_mailer_is_handed_the_invite_url(
        self, client: TestClient, store: FakeStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sent: list[dict[str, Any]] = []
        smtp = SimpleNamespace(enabled=True, host="smtp.example.com")
        monkeypatch.setattr(organisations_module, "_get_smtp_config", lambda: smtp)
        monkeypatch.setattr(
            organisations_module,
            "_send_invitation_email",
            lambda request, smtp_config, invitation, org_name, inviter_name: sent.append(
                {
                    "smtp": smtp_config,
                    "email": invitation.email,
                    "org_name": org_name,
                    "inviter_name": inviter_name,
                }
            ),
        )

        client.post("/organisations/invite", json={"email": "new@example.com"})

        assert sent == [
            {
                "smtp": smtp,
                "email": "new@example.com",
                "org_name": "Dev Org",
                "inviter_name": "Ada",
            }
        ]

    def test_a_nameless_inviter_is_identified_by_email(
        self, client: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sent: list[str] = []
        monkeypatch.setattr(
            organisations_module, "_get_smtp_config", lambda: SimpleNamespace(enabled=True)
        )
        monkeypatch.setattr(
            organisations_module,
            "_send_invitation_email",
            lambda request, smtp_config, invitation, org_name, inviter_name: sent.append(inviter_name),
        )
        client.app.dependency_overrides[require_admin] = lambda: _profile(name=None)

        client.post("/organisations/invite", json={"email": "new@example.com"})

        assert sent == ["ada@example.com"]


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

        response = client.delete(f"/organisations/invitations/{uuid4()}")

        assert response.status_code == 404
        assert response.json()["detail"] == "Invitation not found"
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

        with caplog.at_level("WARNING", logger="interloper_api.routes.organisations"):
            assert client.post(f"/organisations/invitations/{invitation_id}/resend").status_code == 200

        assert "SMTP not configured" in caplog.text

    def test_a_configured_mailer_gets_the_new_invitation(
        self, client: TestClient, store: FakeStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        invitation_id = uuid4()
        store.invitations = [_invitation(invitation_id, email="new@example.com")]
        sent: list[str] = []
        monkeypatch.setattr(
            organisations_module, "_get_smtp_config", lambda: SimpleNamespace(enabled=True)
        )
        monkeypatch.setattr(
            organisations_module,
            "_send_invitation_email",
            lambda request, smtp_config, invitation, org_name, inviter_name: sent.append(invitation.token),
        )

        client.post(f"/organisations/invitations/{invitation_id}/resend")

        # The reissued token, not the one that was just deleted.
        assert sent and sent[0] != f"token-{invitation_id}"


class TestSmtpLookup:
    """``_get_smtp_config`` degrades instead of raising when email is unset."""

    def test_it_returns_whatever_state_holds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from interloper_api.dependencies import state as state_module

        smtp = SimpleNamespace(enabled=True)
        monkeypatch.setattr(state_module, "_smtp_config", smtp)

        assert organisations_module._get_smtp_config() is smtp

    def test_an_unset_config_is_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from interloper_api.dependencies import state as state_module

        monkeypatch.setattr(state_module, "_smtp_config", None)

        assert organisations_module._get_smtp_config() is None


class TestSendInvitationEmail:
    """The mailer wrapper never fails the request it was called from."""

    def test_builds_the_invite_url_from_the_request(self, monkeypatch: pytest.MonkeyPatch) -> None:
        built: list[dict[str, Any]] = []

        class FakeEmail:
            def __init__(self, **kwargs: Any) -> None:
                built.append(kwargs)

            def send(self, smtp_config: Any, email: str) -> None:
                built[-1]["sent_to"] = email

        monkeypatch.setattr(organisations_module, "InvitationEmail", FakeEmail)
        request = SimpleNamespace(base_url="https://app.example.com/")

        organisations_module._send_invitation_email(
            request,  # ty: ignore[invalid-argument-type]
            SimpleNamespace(enabled=True),
            _invitation(uuid4(), email="new@example.com"),
            "Dev Org",
            "Ada",
        )

        assert built[0]["invite_url"].startswith("https://app.example.com/invite/token-")
        assert built[0]["logo_url"] == "https://app.example.com/logo-email.png"
        assert built[0]["org_name"] == "Dev Org"
        assert built[0]["inviter_name"] == "Ada"
        assert built[0]["sent_to"] == "new@example.com"

    def test_a_mailer_failure_is_logged_not_raised(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        # The invitation row already exists; failing the request here would
        # leave the caller unable to tell what happened.
        class FailingEmail:
            def __init__(self, **kwargs: Any) -> None:
                pass

            def send(self, smtp_config: Any, email: str) -> None:
                raise OSError("smtp unreachable")

        monkeypatch.setattr(organisations_module, "InvitationEmail", FailingEmail)

        with caplog.at_level("ERROR", logger="interloper_api.routes.organisations"):
            organisations_module._send_invitation_email(
                SimpleNamespace(base_url="https://app.example.com/"),  # ty: ignore[invalid-argument-type]
                SimpleNamespace(enabled=True),
                _invitation(uuid4(), email="new@example.com"),
                "Dev Org",
                "Ada",
            )

        assert "Failed to send invitation email to new@example.com" in caplog.text
