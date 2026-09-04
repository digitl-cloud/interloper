"""Tests for ``interloper_api.routes.auth`` — login callback policies.

The Google OAuth exchange is faked at the httpx layer; a lightweight fake store
records the calls. Two properties under test: a user whose email is in
``auth_config.super_admin_emails`` is promoted on login (promote-only — an
existing super-admin is left alone), and ``allowed_domains`` gates
profile creation for first-time logins without touching existing profiles.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper.errors import NotFoundError

from interloper_api.dependencies import get_auth_config, get_current_user, get_store
from interloper_api.routes import auth as auth_module


class FakeStore:
    """In-memory stand-in exposing only the ``auth`` facet methods the callback calls."""

    def __init__(self, *, is_super_admin: bool = False, exists: bool = False, invited: bool = False) -> None:
        self.profile = SimpleNamespace(id=uuid4(), is_super_admin=is_super_admin)
        self.exists = exists
        self.invited = invited
        self.promoted: list[UUID] = []
        self.upserted = False
        self.auth = SimpleNamespace(
            get_profile_by_google_id=self._get_profile_by_google_id,
            upsert_profile=self._upsert_profile,
            set_super_admin=self._set_super_admin,
            create_session=self._create_session,
        )
        self.organisations = SimpleNamespace(has_pending_invitation=self._has_pending_invitation)

    def _get_profile_by_google_id(self, google_id: str) -> SimpleNamespace | None:
        return self.profile if self.exists else None

    def _has_pending_invitation(self, email: str) -> bool:
        return self.invited

    def _upsert_profile(self, **kwargs) -> SimpleNamespace:
        self.upserted = True
        return self.profile

    def _set_super_admin(self, user_id: UUID) -> SimpleNamespace:
        self.promoted.append(user_id)
        self.profile.is_super_admin = True
        return self.profile

    def _create_session(self, user_id: UUID) -> str:
        return "token"


def _auth_config(super_admin_emails: list[str], allowed_domains: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        google_client_id="client-id",
        google_client_secret="client-secret",
        google_redirect_uri="http://localhost/api/auth/google/callback",
        cookie_secure=False,
        session_expiry_days=1,
        super_admin_emails=super_admin_emails,
        allowed_domains=allowed_domains,
    )


def _client(
    store: FakeStore,
    super_admin_emails: list[str] | None = None,
    allowed_domains: list[str] | None = None,
) -> TestClient:
    app = FastAPI()
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_auth_config] = lambda: _auth_config(
        super_admin_emails or [], allowed_domains or []
    )
    return TestClient(app, follow_redirects=False)


@pytest.fixture(autouse=True)
def fake_google(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake the token exchange and userinfo fetch."""
    monkeypatch.setattr(
        auth_module.httpx,
        "post",
        lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"access_token": "at"}),
    )
    monkeypatch.setattr(
        auth_module.httpx,
        "get",
        lambda *a, **k: SimpleNamespace(
            status_code=200,
            json=lambda: {"id": "google-1", "email": "Boss@Example.com", "name": "Boss"},
        ),
    )


def test_listed_email_is_promoted_case_insensitively() -> None:
    store = FakeStore()
    resp = _client(store, ["boss@example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert store.promoted == [store.profile.id]


def test_unlisted_email_is_not_promoted() -> None:
    store = FakeStore()
    resp = _client(store, ["someone-else@example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert store.promoted == []


def test_existing_super_admin_is_not_touched() -> None:
    store = FakeStore(is_super_admin=True)
    resp = _client(store, ["boss@example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert store.promoted == []


def test_signup_blocked_when_domain_not_allowed() -> None:
    store = FakeStore()
    client = _client(store, allowed_domains=["digitlcloud.com"])
    resp = client.get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert resp.headers["location"] == "/login?error=signup_not_allowed"
    assert not store.upserted
    assert "session_token" not in resp.cookies


def test_signup_allowed_for_listed_domain() -> None:
    store = FakeStore()
    resp = _client(store, allowed_domains=["example.com"]).get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted


def test_existing_profile_bypasses_allowlist() -> None:
    store = FakeStore(exists=True)
    resp = _client(store, allowed_domains=["digitlcloud.com"]).get(
        "/auth/google/callback", params={"code": "c"}
    )
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted


def test_invited_email_can_sign_up() -> None:
    store = FakeStore(invited=True)
    resp = _client(store, allowed_domains=["digitlcloud.com"]).get(
        "/auth/google/callback", params={"code": "c"}
    )
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted


def test_super_admin_email_can_sign_up() -> None:
    store = FakeStore()
    client = _client(store, super_admin_emails=["boss@example.com"], allowed_domains=["digitlcloud.com"])
    resp = client.get("/auth/google/callback", params={"code": "c"})
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.promoted == [store.profile.id]


# -- PATCH /auth/me -------------------------------------------------------------


class FakeProfileStore:
    """In-memory stand-in exposing only ``auth.update_profile``."""

    def __init__(self) -> None:
        self.profile = SimpleNamespace(
            id=uuid4(),
            email="user@example.com",
            name="Google Name",
            avatar_url=None,
            timezone=None,
        )
        self.auth = SimpleNamespace(update_profile=self._update_profile)

    def _update_profile(self, user_id: UUID, *, name: str | None = None, timezone: str | None = None):
        if name is not None:
            self.profile.name = name
        if timezone is not None:
            self.profile.timezone = timezone
        return self.profile


def _me_client(store: FakeProfileStore) -> TestClient:
    from interloper_api.dependencies import get_current_user

    app = FastAPI()
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_current_user] = lambda: store.profile
    return TestClient(app)


def test_update_me_sets_name_and_timezone() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"name": "Custom", "timezone": "Europe/Berlin"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Custom"
    assert body["timezone"] == "Europe/Berlin"


def test_update_me_omitted_fields_stay_untouched() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"timezone": "UTC"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Google Name"
    assert store.profile.timezone == "UTC"


def test_update_me_rejects_unknown_timezone() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"timezone": "Mars/Olympus_Mons"})
    assert resp.status_code == 422
    assert store.profile.timezone is None


def test_update_me_rejects_empty_name() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"name": ""})
    assert resp.status_code == 422
    assert store.profile.name == "Google Name"


# -- Login redirect ------------------------------------------------------------


class TestGoogleLogin:
    """``GET /auth/google`` — hands the browser to Google's consent screen."""

    def test_redirects_with_the_configured_client_and_scopes(self) -> None:
        response = _client(FakeStore()).get("/auth/google")

        assert response.status_code == 307
        location = response.headers["location"]
        assert location.startswith(auth_module.GOOGLE_AUTH_URL)
        query = parse_qs(urlparse(location).query)
        assert query["client_id"] == ["client-id"]
        assert query["redirect_uri"] == ["http://localhost/api/auth/google/callback"]
        assert query["response_type"] == ["code"]
        assert query["scope"] == ["openid email profile"]
        assert query["state"] == ["/"]

    def test_the_return_destination_rides_the_state_parameter(self) -> None:
        response = _client(FakeStore()).get("/auth/google?redirect=/jobs/42")

        query = parse_qs(urlparse(response.headers["location"]).query)
        assert query["state"] == ["/jobs/42"]

    def test_an_unconfigured_client_id_is_a_500(self) -> None:
        app = FastAPI()
        app.include_router(auth_module.router)
        app.dependency_overrides[get_store] = lambda: FakeStore()
        config = _auth_config([], [])
        config.google_client_id = ""
        app.dependency_overrides[get_auth_config] = lambda: config

        response = TestClient(app, follow_redirects=False).get("/auth/google")

        assert response.status_code == 500
        assert response.json()["detail"] == "Google OAuth not configured"


class TestGoogleCallbackFailures:
    """Every way the Google exchange can fail becomes a clean status."""

    def test_an_unconfigured_secret_is_a_500(self) -> None:
        app = FastAPI()
        app.include_router(auth_module.router)
        app.dependency_overrides[get_store] = lambda: FakeStore()
        config = _auth_config([], [])
        config.google_client_secret = ""
        app.dependency_overrides[get_auth_config] = lambda: config

        response = TestClient(app, follow_redirects=False).get("/auth/google/callback?code=c")

        assert response.status_code == 500

    def test_a_rejected_code_is_a_401(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            auth_module.httpx, "post", lambda *a, **k: SimpleNamespace(status_code=400, json=dict)
        )

        response = _client(FakeStore()).get("/auth/google/callback?code=bad")

        assert response.status_code == 401
        assert response.json()["detail"] == "Failed to exchange authorization code"

    def test_a_token_response_without_an_access_token_is_a_401(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            auth_module.httpx,
            "post",
            lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"scope": "openid"}),
        )

        response = _client(FakeStore()).get("/auth/google/callback?code=c")

        assert response.status_code == 401
        assert response.json()["detail"] == "No access token in response"

    def test_a_failed_userinfo_lookup_is_a_401(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            auth_module.httpx, "get", lambda *a, **k: SimpleNamespace(status_code=403, json=dict)
        )

        response = _client(FakeStore()).get("/auth/google/callback?code=c")

        assert response.status_code == 401
        assert response.json()["detail"] == "Failed to fetch user info"

    @pytest.mark.parametrize(
        "userinfo",
        [{"email": "a@example.com"}, {"id": "google-1"}, {}],
    )
    def test_incomplete_userinfo_is_a_401(
        self, monkeypatch: pytest.MonkeyPatch, userinfo: dict[str, str]
    ) -> None:
        monkeypatch.setattr(
            auth_module.httpx,
            "get",
            lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: userinfo),
        )

        response = _client(FakeStore()).get("/auth/google/callback?code=c")

        assert response.status_code == 401
        assert response.json()["detail"] == "Incomplete user info from Google"


class TestGoogleCallbackSession:
    """A successful login lands a session cookie and honours the return destination."""

    def test_the_session_cookie_is_set_httponly(self) -> None:
        response = _client(FakeStore()).get("/auth/google/callback?code=c")

        assert response.status_code == 302
        cookie = response.headers["set-cookie"]
        assert "session_token=token" in cookie
        assert "HttpOnly" in cookie
        assert "Max-Age=86400" in cookie

    def test_an_app_relative_state_is_honoured(self) -> None:
        response = _client(FakeStore()).get("/auth/google/callback?code=c&state=/jobs/42")

        assert response.headers["location"] == "/jobs/42"

    @pytest.mark.parametrize("state", ["https://evil.example.com/", "evil.example.com"])
    def test_an_absolute_state_is_refused_in_favour_of_the_root(self, state: str) -> None:
        # Otherwise ``state`` is an open-redirect vector.
        response = _client(FakeStore()).get(f"/auth/google/callback?code=c&state={state}")

        assert response.headers["location"] == "/"

    def test_no_state_lands_at_the_root(self) -> None:
        response = _client(FakeStore()).get("/auth/google/callback?code=c")

        assert response.headers["location"] == "/"


# -- Session-backed endpoints --------------------------------------------------


class SessionStore:
    """Fake store for the endpoints that read an existing session."""

    def __init__(
        self,
        *,
        session: tuple[Any, Any] | None = None,
        org: Any = None,
        role: str | None = "editor",
        org_missing: bool = False,
        accepted: Any = None,
    ) -> None:
        """Set up the fake.

        Args:
            session: What ``resolve_session`` returns.
            org: What ``organisations.get`` returns.
            role: The caller's role in the organisation asked about.
            org_missing: Whether ``organisations.get`` raises ``NotFoundError``.
            accepted: What ``accept_invitation`` returns; ``None`` means the
                token was unknown or expired.
        """
        self.deleted_sessions: list[UUID] = []
        self.session_org_calls: list[tuple[str, UUID, UUID]] = []
        self._accepted = accepted
        self.auth = SimpleNamespace(
            resolve_session=lambda token: session,
            delete_user_sessions=self.deleted_sessions.append,
            set_session_org=self._set_session_org,
        )
        self.organisations = SimpleNamespace(
            get=self._get_org if not org_missing else self._missing_org,
            member_role=lambda user_id, org_id: role,
            accept_invitation=self._accept,
            _org=org,
        )

    def _get_org(self, org_id: UUID) -> Any:
        return self.organisations._org

    def _missing_org(self, org_id: UUID) -> Any:
        raise NotFoundError(f"Organisation {org_id} not found")

    def _set_session_org(self, token: str, org_id: UUID, user_id: UUID) -> None:
        self.session_org_calls.append((token, org_id, user_id))

    def _accept(self, token: str, user_id: UUID) -> Any:
        return self._accepted


def _session_client(store: Any, user: Any = None) -> TestClient:
    app = FastAPI()
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_auth_config] = lambda: _auth_config([], [])
    if user is not None:
        app.dependency_overrides[get_current_user] = lambda: user
    return TestClient(app, follow_redirects=False)


def _me_profile(**overrides: Any) -> SimpleNamespace:
    base = {
        "id": uuid4(),
        "email": "ada@example.com",
        "name": "Ada",
        "avatar_url": None,
        "timezone": None,
        "is_super_admin": False,
        "last_organisation_id": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


class TestGetMe:
    """``GET /auth/me`` — identity plus the active organisation, when there is one."""

    def test_no_cookie_is_a_401(self) -> None:
        response = _session_client(SessionStore()).get("/auth/me")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    def test_an_unresolvable_token_is_a_401(self) -> None:
        client = _session_client(SessionStore(session=None))
        client.cookies.set("session_token", "stale")

        response = client.get("/auth/me")

        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid or expired session"

    def test_a_session_without_an_organisation_defaults_to_viewer(self) -> None:
        profile = _me_profile()
        client = _session_client(
            SessionStore(session=(profile, SimpleNamespace(organisation_id=None)))
        )
        client.cookies.set("session_token", "tok")

        payload = client.get("/auth/me").json()

        assert payload["organisation"] is None
        assert payload["role"] == "viewer"
        assert payload["email"] == "ada@example.com"

    def test_the_active_organisation_and_role_are_reported(self) -> None:
        profile = _me_profile()
        org_id = uuid4()
        org = SimpleNamespace(id=org_id, name="Dev Org", created_at=None)
        client = _session_client(
            SessionStore(session=(profile, SimpleNamespace(organisation_id=org_id)), org=org, role="admin")
        )
        client.cookies.set("session_token", "tok")

        payload = client.get("/auth/me").json()

        assert payload["organisation"]["name"] == "Dev Org"
        assert payload["role"] == "admin"

    def test_a_session_outliving_its_organisation_still_resolves(self) -> None:
        # The caller is authenticated, just no longer scoped anywhere.
        profile = _me_profile()
        client = _session_client(
            SessionStore(session=(profile, SimpleNamespace(organisation_id=uuid4())), org_missing=True)
        )
        client.cookies.set("session_token", "tok")

        payload = client.get("/auth/me").json()

        assert payload["organisation"] is None
        assert payload["role"] == "viewer"

    def test_a_non_member_of_the_active_org_falls_back_to_viewer(self) -> None:
        profile = _me_profile()
        org_id = uuid4()
        client = _session_client(
            SessionStore(
                session=(profile, SimpleNamespace(organisation_id=org_id)),
                org=SimpleNamespace(id=org_id, name="Dev Org", created_at=None),
                role=None,
            )
        )
        client.cookies.set("session_token", "tok")

        assert client.get("/auth/me").json()["role"] == "viewer"


class TestLogout:
    """``POST /auth/logout`` — every session, not just this browser's."""

    def test_drops_all_sessions_and_clears_the_cookie(self) -> None:
        profile = _me_profile()
        store = SessionStore()
        client = _session_client(store, user=profile)

        response = client.post("/auth/logout")

        assert response.json() == {"status": "ok"}
        assert store.deleted_sessions == [profile.id]
        assert 'session_token=""' in response.headers["set-cookie"]


class TestSwitchOrg:
    """``POST /auth/switch-org`` — membership is required."""

    def test_records_the_new_active_org(self) -> None:
        profile = _me_profile()
        store = SessionStore(role="viewer")
        client = _session_client(store, user=profile)
        client.cookies.set("session_token", "tok")
        org_id = uuid4()

        response = client.post("/auth/switch-org", json={"organisation_id": str(org_id)})

        assert response.json() == {"status": "ok"}
        assert store.session_org_calls == [("tok", org_id, profile.id)]

    def test_a_non_member_is_refused(self) -> None:
        store = SessionStore(role=None)
        client = _session_client(store, user=_me_profile())
        client.cookies.set("session_token", "tok")

        response = client.post("/auth/switch-org", json={"organisation_id": str(uuid4())})

        assert response.status_code == 403
        assert response.json()["detail"] == "Not a member of this organisation"
        assert store.session_org_calls == []

    def test_without_a_cookie_there_is_nothing_to_record(self) -> None:
        store = SessionStore(role="admin")
        client = _session_client(store, user=_me_profile())

        response = client.post("/auth/switch-org", json={"organisation_id": str(uuid4())})

        assert response.json() == {"status": "ok"}
        assert store.session_org_calls == []


class TestAcceptInvite:
    """``POST /auth/accept-invite`` — redeeming a token joins the organisation."""

    def test_joining_makes_the_org_active(self) -> None:
        profile = _me_profile()
        org_id = uuid4()
        store = SessionStore(accepted=SimpleNamespace(id=org_id, name="Dev Org"))
        client = _session_client(store, user=profile)
        client.cookies.set("session_token", "tok")

        response = client.post("/auth/accept-invite", json={"token": "invite-token"})

        assert response.json() == {"status": "ok"}
        assert store.session_org_calls == [("tok", org_id, profile.id)]

    def test_an_unknown_or_expired_token_is_a_400(self) -> None:
        store = SessionStore(accepted=None)
        client = _session_client(store, user=_me_profile())
        client.cookies.set("session_token", "tok")

        response = client.post("/auth/accept-invite", json={"token": "stale"})

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid or expired invitation"
        assert store.session_org_calls == []

    def test_without_a_cookie_the_membership_still_lands(self) -> None:
        store = SessionStore(accepted=SimpleNamespace(id=uuid4(), name="Dev Org"))
        client = _session_client(store, user=_me_profile())

        assert client.post("/auth/accept-invite", json={"token": "invite-token"}).json() == {"status": "ok"}
        assert store.session_org_calls == []
