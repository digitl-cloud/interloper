"""Tests for ``interloper_api.routes.auth`` — login callback policies.

The Google OAuth exchange is faked at the httpx2 layer; a lightweight fake store
records the calls. Two properties under test: a user whose email is in
``auth_config.super_admin_emails`` is promoted on login (promote-only — an
existing super-admin is left alone), and ``allowed_domains`` gates
profile creation for first-time logins without touching existing profiles.
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import NotFoundError, Page

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import get_caller, get_settings, get_store
from interloper_api.routes import auth as auth_module


class FakeStore:
    """In-memory stand-in exposing only the store facet methods the callback calls."""

    def __init__(self, *, is_super_admin: bool = False, exists: bool = False, invited: bool = False) -> None:
        self.profile = SimpleNamespace(id=uuid4(), is_super_admin=is_super_admin)
        self.exists = exists
        self.invited = invited
        self.promoted: list[UUID] = []
        self.upserted: dict[str, Any] | None = None
        self.sessions_created: list[UUID] = []
        self.profiles = SimpleNamespace(
            get_by_google_id=self._get_profile_by_google_id,
            upsert=self._upsert_profile,
            set_super_admin=self._set_super_admin,
        )
        self.tokens = SimpleNamespace(create_session=self._create_session)
        self.invitations = SimpleNamespace(has_pending=self._has_pending_invitation)

    def _get_profile_by_google_id(self, google_id: str) -> SimpleNamespace | None:
        return self.profile if self.exists else None

    def _has_pending_invitation(self, email: str) -> bool:
        return self.invited

    def _upsert_profile(self, **kwargs) -> SimpleNamespace:
        self.upserted = kwargs
        return self.profile

    def _set_super_admin(self, user_id: UUID, *, value: bool) -> SimpleNamespace:
        self.promoted.append(user_id)
        self.profile.is_super_admin = value
        return self.profile

    def _create_session(self, user_id: UUID) -> str:
        self.sessions_created.append(user_id)
        return "token"


NONCE = "nonce"


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
    install_error_handlers(app)
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(
        auth=_auth_config(super_admin_emails or [], allowed_domains or [])
    )
    client = TestClient(app, follow_redirects=False)
    client.cookies.set(auth_module.LOGIN_STATE_COOKIE, NONCE)
    return client


def _callback_params(destination: str = "/", nonce: str = NONCE) -> dict[str, str]:
    """Build the query Google hands back for a flow :func:`_client`'s browser started.

    Args:
        destination: The post-login destination recorded in ``state``.
        nonce: The nonce recorded in ``state``; anything but ``NONCE`` mismatches the cookie.

    Returns:
        The callback's query parameters.
    """
    return {"code": "c", "state": f"{nonce}:{destination}"}


@pytest.fixture(autouse=True)
def fake_google(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake the token exchange and userinfo fetch."""
    monkeypatch.setattr(
        auth_module.httpx2,
        "post",
        lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"access_token": "at"}),
    )
    monkeypatch.setattr(
        auth_module.httpx2,
        "get",
        lambda *a, **k: SimpleNamespace(
            status_code=200,
            json=lambda: {"id": "google-1", "email": "Boss@Example.com", "name": "Boss"},
        ),
    )


def test_listed_email_is_promoted_case_insensitively() -> None:
    store = FakeStore()
    resp = _client(store, ["boss@example.com"]).get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert store.promoted == [store.profile.id]
    assert store.profile.is_super_admin is True


def test_unlisted_email_is_not_promoted() -> None:
    store = FakeStore()
    resp = _client(store, ["someone-else@example.com"]).get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert store.promoted == []


def test_existing_super_admin_is_not_touched() -> None:
    store = FakeStore(is_super_admin=True)
    resp = _client(store, ["boss@example.com"]).get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert store.promoted == []


def test_signup_blocked_when_domain_not_allowed() -> None:
    store = FakeStore()
    client = _client(store, allowed_domains=["digitlcloud.com"])
    resp = client.get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert resp.headers["location"] == "/login?error=signup_not_allowed"
    assert store.upserted is None
    assert store.sessions_created == []
    assert "session_token" not in resp.cookies


def test_signup_allowed_for_listed_domain() -> None:
    store = FakeStore()
    resp = _client(store, allowed_domains=["example.com"]).get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted == {
        "google_id": "google-1",
        "email": "Boss@Example.com",
        "name": "Boss",
        "avatar_url": None,
    }
    assert store.sessions_created == [store.profile.id]


def test_existing_profile_bypasses_allowlist() -> None:
    store = FakeStore(exists=True)
    resp = _client(store, allowed_domains=["digitlcloud.com"]).get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted is not None


def test_invited_email_can_sign_up() -> None:
    store = FakeStore(invited=True)
    resp = _client(store, allowed_domains=["digitlcloud.com"]).get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.upserted is not None


def test_super_admin_email_can_sign_up() -> None:
    store = FakeStore()
    client = _client(store, super_admin_emails=["boss@example.com"], allowed_domains=["digitlcloud.com"])
    resp = client.get("/auth/google/callback", params=_callback_params())
    assert resp.status_code == 302
    assert resp.headers["location"] == "/"
    assert store.promoted == [store.profile.id]


# -- PATCH /auth/me -------------------------------------------------------------


class FakeProfileStore:
    """In-memory stand-in for ``PATCH /auth/me``: the profile update and what ``GET /auth/me`` reads."""

    def __init__(self) -> None:
        self.org = SimpleNamespace(id=uuid4(), name="Dev Org", slug="dev-org", created_at=None)
        self.profile = SimpleNamespace(
            id=uuid4(),
            email="user@example.com",
            name="Google Name",
            avatar_url=None,
            timezone=None,
            is_super_admin=False,
            last_organisation_id=None,
        )
        self.updates: list[tuple[UUID, str | None, str | None]] = []
        self.profiles = SimpleNamespace(update=self._update_profile)
        self.organisations = SimpleNamespace(list=lambda query: Page(items=[self.org], total=1))
        self.members = SimpleNamespace(role_of=lambda org_id, user: "admin" if user.is_super_admin else "editor")

    def _update_profile(self, user_id: UUID, *, name: str | None = None, timezone: str | None = None):
        self.updates.append((user_id, name, timezone))
        if name is not None:
            self.profile.name = name
        if timezone is not None:
            self.profile.timezone = timezone
        return self.profile


def _me_client(store: FakeProfileStore) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_caller] = lambda: store.profile
    return TestClient(app)


def test_update_me_sets_name_and_timezone() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"name": "Custom", "timezone": "Europe/Berlin"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Custom"
    assert body["timezone"] == "Europe/Berlin"
    assert store.updates == [(store.profile.id, "Custom", "Europe/Berlin")]


def test_update_me_answers_the_same_shape_as_get_me() -> None:
    store = FakeProfileStore()
    client = _me_client(store)

    patched = client.patch("/auth/me", json={"name": "Custom"})
    fetched = client.get("/auth/me")

    assert patched.status_code == 200
    assert patched.json() == fetched.json()
    assert patched.json()["memberships"] == [
        {"id": str(store.org.id), "name": "Dev Org", "slug": "dev-org", "role": "editor"}
    ]
    assert set(patched.json()) == set(auth_module.AuthUserResponse.model_fields)


def test_update_me_omitted_fields_stay_untouched() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"timezone": "UTC"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "Google Name"
    assert store.profile.timezone == "UTC"
    assert store.updates == [(store.profile.id, None, "UTC")]


def test_update_me_rejects_unknown_timezone() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"timezone": "Mars/Olympus_Mons"})
    assert resp.status_code == 422
    assert store.profile.timezone is None
    assert store.updates == []


def test_update_me_rejects_empty_name() -> None:
    store = FakeProfileStore()
    resp = _me_client(store).patch("/auth/me", json={"name": ""})
    assert resp.status_code == 422
    assert store.profile.name == "Google Name"
    assert store.updates == []


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
        nonce, _, destination = query["state"][0].partition(":")
        assert destination == "/"
        assert nonce == response.cookies[auth_module.LOGIN_STATE_COOKIE]

    def test_the_nonce_cookie_is_short_lived_and_httponly(self) -> None:
        response = _client(FakeStore()).get("/auth/google")

        cookie = response.headers["set-cookie"]
        assert cookie.startswith(f"{auth_module.LOGIN_STATE_COOKIE}=")
        assert "HttpOnly" in cookie
        assert "SameSite=lax" in cookie
        assert f"Max-Age={auth_module.LOGIN_STATE_MAX_AGE}" in cookie

    def test_every_login_draws_a_fresh_nonce(self) -> None:
        client = _client(FakeStore())

        first = client.get("/auth/google").cookies[auth_module.LOGIN_STATE_COOKIE]
        second = client.get("/auth/google").cookies[auth_module.LOGIN_STATE_COOKIE]

        assert first != second

    def test_the_return_destination_rides_the_state_parameter(self) -> None:
        response = _client(FakeStore()).get("/auth/google?redirect=/jobs/42")

        query = parse_qs(urlparse(response.headers["location"]).query)
        assert query["state"][0].endswith(":/jobs/42")

    def test_an_unconfigured_client_id_is_a_500(self) -> None:
        app = FastAPI()
        install_error_handlers(app)
        app.include_router(auth_module.router)
        app.dependency_overrides[get_store] = lambda: FakeStore()
        config = _auth_config([], [])
        config.google_client_id = ""
        app.dependency_overrides[get_settings] = lambda: SimpleNamespace(auth=config)

        response = TestClient(app, follow_redirects=False).get("/auth/google")

        assert response.status_code == 500
        assert response.json()["detail"] == "Google OAuth not configured"


class TestGoogleCallbackFailures:
    """Every way the Google exchange can fail becomes a clean status."""

    def test_an_unconfigured_secret_is_a_500(self) -> None:
        app = FastAPI()
        install_error_handlers(app)
        app.include_router(auth_module.router)
        app.dependency_overrides[get_store] = lambda: FakeStore()
        config = _auth_config([], [])
        config.google_client_secret = ""
        app.dependency_overrides[get_settings] = lambda: SimpleNamespace(auth=config)

        response = TestClient(app, follow_redirects=False).get("/auth/google/callback", params=_callback_params())

        assert response.status_code == 500

    def test_a_rejected_code_is_a_401(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(auth_module.httpx2, "post", lambda *a, **k: SimpleNamespace(status_code=400, json=dict))

        response = _client(FakeStore()).get("/auth/google/callback", params={**_callback_params(), "code": "bad"})

        assert response.status_code == 401
        assert response.json()["detail"] == "Failed to exchange authorization code"

    def test_a_token_response_without_an_access_token_is_a_401(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            auth_module.httpx2,
            "post",
            lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"scope": "openid"}),
        )

        response = _client(FakeStore()).get("/auth/google/callback", params=_callback_params())

        assert response.status_code == 401
        assert response.json()["detail"] == "No access token in response"

    def test_a_failed_userinfo_lookup_is_a_401(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(auth_module.httpx2, "get", lambda *a, **k: SimpleNamespace(status_code=403, json=dict))

        response = _client(FakeStore()).get("/auth/google/callback", params=_callback_params())

        assert response.status_code == 401
        assert response.json()["detail"] == "Failed to fetch user info"

    @pytest.mark.parametrize(
        "userinfo",
        [{"email": "a@example.com"}, {"id": "google-1"}, {}],
    )
    def test_incomplete_userinfo_is_a_401(self, monkeypatch: pytest.MonkeyPatch, userinfo: dict[str, str]) -> None:
        monkeypatch.setattr(
            auth_module.httpx2,
            "get",
            lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: userinfo),
        )

        response = _client(FakeStore()).get("/auth/google/callback", params=_callback_params())

        assert response.status_code == 401
        assert response.json()["detail"] == "Incomplete user info from Google"


class TestGoogleCallbackSession:
    """A successful login lands a session cookie and honours the return destination."""

    def test_the_session_cookie_is_set_httponly(self) -> None:
        response = _client(FakeStore()).get("/auth/google/callback", params=_callback_params())

        assert response.status_code == 302
        cookie = response.headers["set-cookie"]
        assert "session_token=token" in cookie
        assert "HttpOnly" in cookie
        assert "Max-Age=86400" in cookie

    def test_an_app_relative_destination_is_honoured(self) -> None:
        response = _client(FakeStore()).get("/auth/google/callback", params=_callback_params("/jobs/42?tab=runs"))

        assert response.headers["location"] == "/jobs/42?tab=runs"

    @pytest.mark.parametrize(
        "destination",
        ["https://evil.example.com/", "evil.example.com", "//evil.example.com", "/\\evil.example.com", ""],
    )
    def test_an_off_site_destination_is_refused_in_favour_of_the_root(self, destination: str) -> None:
        response = _client(FakeStore()).get("/auth/google/callback", params=_callback_params(destination))

        assert response.headers["location"] == "/"

    def test_the_nonce_cookie_is_cleared(self) -> None:
        response = _client(FakeStore()).get("/auth/google/callback", params=_callback_params())

        assert f'{auth_module.LOGIN_STATE_COOKIE}=""' in response.headers["set-cookie"]

    def test_a_login_round_trip_lands_at_the_requested_destination(self) -> None:
        client = _client(FakeStore())
        client.cookies.clear()

        login = client.get("/auth/google?redirect=/jobs/42")
        state = parse_qs(urlparse(login.headers["location"]).query)["state"][0]
        response = client.get("/auth/google/callback", params={"code": "c", "state": state})

        assert response.headers["location"] == "/jobs/42"
        assert "session_token=token" in response.headers["set-cookie"]


class TestGoogleCallbackNonce:
    """A callback is honoured only in the browser that started the login."""

    def test_a_callback_without_the_nonce_cookie_is_refused(self) -> None:
        store = FakeStore()
        client = _client(store)
        client.cookies.clear()

        response = client.get("/auth/google/callback", params=_callback_params())

        assert response.status_code == 302
        assert response.headers["location"] == "/login?error=login_expired"
        assert store.sessions_created == []
        assert "session_token" not in response.cookies

    @pytest.mark.parametrize("state", ["other-nonce:/", "/", ""])
    def test_a_mismatched_nonce_is_refused(self, state: str) -> None:
        store = FakeStore()

        response = _client(store).get("/auth/google/callback", params={"code": "c", "state": state})

        assert response.headers["location"] == "/login?error=login_expired"
        assert store.sessions_created == []
        assert f'{auth_module.LOGIN_STATE_COOKIE}=""' in response.headers["set-cookie"]

    def test_a_refused_callback_never_exchanges_the_code(self, monkeypatch: pytest.MonkeyPatch) -> None:
        exchanged: list[Any] = []
        monkeypatch.setattr(auth_module.httpx2, "post", lambda *a, **k: exchanged.append(a))

        _client(FakeStore()).get("/auth/google/callback", params=_callback_params(nonce="other-nonce"))

        assert exchanged == []


# -- Endpoints about the caller ------------------------------------------------


class CallerStore:
    """Fake store for the endpoints that read the caller."""

    def __init__(
        self,
        *,
        authenticated: bool = True,
        organisations: list[Any] | None = None,
        roles: dict[UUID, str] | None = None,
        accepted: Any = None,
    ) -> None:
        """Set up the fake.

        Args:
            authenticated: Whether the session cookie resolves.
            organisations: The organisations the caller belongs to.
            roles: The caller's role per organisation; absent means not a member.
            accepted: What ``invitations.accept`` returns; ``None`` means the
                token was unknown or expired.
        """
        self.profile = _me_profile()
        self.revoked_sessions: list[UUID] = []
        self.last_organisations: list[tuple[UUID, UUID | None]] = []
        self.accept_calls: list[tuple[str, UUID]] = []
        self._accepted = accepted
        self._organisations = organisations or []
        self._roles = roles or {}
        resolved = (self.profile, SimpleNamespace(kind="session")) if authenticated else None
        self.tokens = SimpleNamespace(
            resolve=lambda raw, audience: resolved, revoke_sessions=self.revoked_sessions.append
        )
        self.profiles = SimpleNamespace(update=self._update_profile)
        self.organisations = SimpleNamespace(
            list=lambda query: Page(items=self._organisations, total=len(self._organisations)),
            get=self._get_organisation,
        )
        self.members = SimpleNamespace(
            role_of=lambda org_id, user: "admin" if user.is_super_admin else self._roles.get(org_id)
        )
        self.invitations = SimpleNamespace(accept=self._accept)

    def _get_organisation(self, org_id: UUID) -> Any:
        for organisation in self._organisations:
            if organisation.id == org_id:
                return organisation
        raise NotFoundError(f"Organisation {org_id} not found")

    def _update_profile(self, user_id: UUID, *, last_organisation_id: UUID | None = None) -> Any:
        self.last_organisations.append((user_id, last_organisation_id))
        return self.profile

    def _accept(self, token: str, user_id: UUID) -> Any:
        self.accept_calls.append((token, user_id))
        return self._accepted


def _session_client(store: CallerStore) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(auth=_auth_config([], []))
    client = TestClient(app, follow_redirects=False)
    client.cookies.set("session_token", "tok")
    return client


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


def _organisation(name: str, slug: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), name=name, slug=slug, created_at=None)


class TestGetMe:
    """``GET /auth/me``: identity plus every organisation the caller belongs to, by slug."""

    def test_no_cookie_is_a_401(self) -> None:
        client = _session_client(CallerStore())
        client.cookies.clear()

        response = client.get("/auth/me")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    def test_an_unresolvable_cookie_is_a_401(self) -> None:
        response = _session_client(CallerStore(authenticated=False)).get("/auth/me")

        assert response.status_code == 401

    def test_memberships_carry_the_slug_and_the_role(self) -> None:
        dev, acme = _organisation("Dev Org", "dev-org"), _organisation("Acme", "acme")
        store = CallerStore(organisations=[dev, acme], roles={dev.id: "admin", acme.id: "viewer"})

        payload = _session_client(store).get("/auth/me").json()

        assert payload["email"] == "ada@example.com"
        assert payload["memberships"] == [
            {"id": str(dev.id), "name": "Dev Org", "slug": "dev-org", "role": "admin"},
            {"id": str(acme.id), "name": "Acme", "slug": "acme", "role": "viewer"},
        ]
        assert "organisation" not in payload
        assert "role" not in payload

    def test_a_super_admins_memberships_carry_admin(self) -> None:
        dev = _organisation("Dev Org", "dev-org")
        store = CallerStore(organisations=[dev], roles={dev.id: "viewer"})
        store.profile.is_super_admin = True

        payload = _session_client(store).get("/auth/me").json()

        assert [membership["role"] for membership in payload["memberships"]] == ["admin"]

    def test_the_last_organisation_is_reported(self) -> None:
        store = CallerStore()
        store.profile.last_organisation_id = last = uuid4()

        assert _session_client(store).get("/auth/me").json()["last_organisation_id"] == str(last)

    def test_no_membership_is_an_empty_list(self) -> None:
        assert _session_client(CallerStore()).get("/auth/me").json()["memberships"] == []


class TestLogout:
    """``POST /auth/logout``: every session, not just this browser's."""

    def test_revokes_all_sessions_and_clears_the_cookie(self) -> None:
        store = CallerStore()

        response = _session_client(store).post("/auth/logout")

        assert response.status_code == 204
        assert response.content == b""
        assert store.revoked_sessions == [store.profile.id]
        assert 'session_token=""' in response.headers["set-cookie"]

    def test_a_bearer_cannot_log_out(self) -> None:
        store = CallerStore()

        response = _session_client(store).post("/auth/logout", headers={"Authorization": "Bearer ilp_leaked"})

        assert response.status_code == 401
        assert store.revoked_sessions == []


class TestCookieOnlyRoutes:
    """Membership and session management refuse a bearer token; reading the caller does not."""

    @pytest.mark.parametrize(
        ("method", "path", "body"),
        [
            ("PUT", "/auth/me/last-organisation", {"org_id": "00000000-0000-0000-0000-000000000000"}),
            ("POST", "/auth/accept-invite", {"token": "invite-token"}),
        ],
    )
    def test_a_bearer_is_refused(self, method: str, path: str, body: dict[str, str]) -> None:
        store = CallerStore()

        response = _session_client(store).request(
            method, path, json=body, headers={"Authorization": "Bearer ilp_leaked"}
        )

        assert response.status_code == 401
        assert store.last_organisations == []
        assert store.accept_calls == []

    def test_get_me_accepts_a_bearer(self) -> None:
        client = _session_client(CallerStore())
        client.cookies.clear()

        response = client.get("/auth/me", headers={"Authorization": "Bearer ilp_pat"})

        assert response.status_code == 200


def test_switch_org_gone() -> None:
    response = _session_client(CallerStore()).post("/auth/switch-org", json={"organisation_id": str(uuid4())})

    assert response.status_code == 404


class TestLastOrganisation:
    """``PUT /auth/me/last-organisation``: where the app lands the caller next time."""

    def test_a_member_records_the_organisation(self) -> None:
        dev = _organisation("Dev Org", "dev-org")
        store = CallerStore(organisations=[dev], roles={dev.id: "viewer"})

        response = _session_client(store).put("/auth/me/last-organisation", json={"org_id": str(dev.id)})

        assert response.status_code == 204
        assert store.last_organisations == [(store.profile.id, dev.id)]

    def test_a_non_member_is_refused(self) -> None:
        other = _organisation("Other", "other")
        store = CallerStore(organisations=[other])

        response = _session_client(store).put("/auth/me/last-organisation", json={"org_id": str(other.id)})

        assert response.status_code == 404
        assert store.last_organisations == []

    def test_a_super_admin_records_any_live_organisation(self) -> None:
        other = _organisation("Other", "other")
        store = CallerStore(organisations=[other])
        store.profile.is_super_admin = True

        response = _session_client(store).put("/auth/me/last-organisation", json={"org_id": str(other.id)})

        assert response.status_code == 204
        assert store.last_organisations == [(store.profile.id, other.id)]


class TestAcceptInvite:
    """``POST /auth/accept-invite``: redeeming a token joins the organisation."""

    def test_the_joined_organisation_is_returned(self) -> None:
        org_id = uuid4()
        created_at = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)
        store = CallerStore(accepted=SimpleNamespace(id=org_id, name="Dev Org", slug="dev-org", created_at=created_at))

        response = _session_client(store).post("/auth/accept-invite", json={"token": "invite-token"})

        assert response.status_code == 200
        assert store.accept_calls == [("invite-token", store.profile.id)]
        assert response.json() == {
            "id": str(org_id),
            "name": "Dev Org",
            "slug": "dev-org",
            "created_at": "2026-01-02T00:00:00Z",
        }

    def test_an_unknown_or_expired_token_is_a_400(self) -> None:
        response = _session_client(CallerStore(accepted=None)).post("/auth/accept-invite", json={"token": "stale"})

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid or expired invitation"
