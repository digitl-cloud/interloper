"""Tests for ``interloper_api.routes.auth``: Google login, the login session, and the caller's own profile.

Google is faked at the HTTP transport (its discovery document, its keys and a
token endpoint that signs real ID tokens), so the login runs Authlib's own
checks end to end; a lightweight fake store records what the callback does.
"""

from __future__ import annotations

import datetime as dt
import time
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

import httpx2
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import NotFoundError, Page
from joserfc import jwt
from joserfc.jwk import KeySet, RSAKey

from interloper_api.app import install_error_handlers
from interloper_api.dependencies import get_caller, get_google_login, get_settings, get_store
from interloper_api.login import SESSION_COOKIE, SESSION_MAX_AGE, SESSION_PATH, GoogleLogin
from interloper_api.routes import auth as auth_module

ISSUER = "https://accounts.google.com"
AUTHORIZATION_ENDPOINT = f"{ISSUER}/o/oauth2/v2/auth"
TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
JWKS_URI = "https://www.googleapis.com/oauth2/v3/certs"
CLIENT_ID = "client-id"
REDIRECT_URI = "http://testserver/api/auth/google/callback"
ENCRYPTION_KEY = "test-encryption-key"
GOOGLE_KEY = RSAKey.generate_key(2048, parameters={"kid": "google"})
FORGED_KEY = RSAKey.generate_key(2048, parameters={"kid": "google"})


class FakeGoogle:
    """Google's OpenID endpoints, served from memory.

    Attributes:
        claims: The ID token's claims, over the defaults; a ``None`` value
            drops the claim.
        signing_key: The key the ID token is signed with.
        token_error: An OAuth error the token endpoint answers instead.
        issues_id_token: Whether the token response carries an ID token.
        discovery_status: The discovery document's HTTP status.
        nonce: The nonce of the login in flight, read from the authorization
            redirect, which the next ID token carries.
        token_requests: The forms the token endpoint received.
    """

    def __init__(self) -> None:
        """Serve a working Google."""
        self.claims: dict[str, Any] = {}
        self.signing_key = GOOGLE_KEY
        self.token_error: str | None = None
        self.issues_id_token = True
        self.discovery_status = 200
        self.nonce: str | None = None
        self.token_requests: list[dict[str, list[str]]] = []

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request to Google.

        Args:
            request: The request Authlib sent.

        Returns:
            Google's answer.
        """
        url = str(request.url)
        if url.endswith("/.well-known/openid-configuration"):
            return httpx2.Response(
                self.discovery_status,
                json={
                    "issuer": ISSUER,
                    "authorization_endpoint": AUTHORIZATION_ENDPOINT,
                    "token_endpoint": TOKEN_ENDPOINT,
                    "jwks_uri": JWKS_URI,
                    "id_token_signing_alg_values_supported": ["RS256"],
                },
            )
        if url == JWKS_URI:
            return httpx2.Response(200, json=KeySet([GOOGLE_KEY]).as_dict(private=False))
        if url == TOKEN_ENDPOINT:
            self.token_requests.append(parse_qs(request.content.decode()))
            if self.token_error:
                return httpx2.Response(400, json={"error": self.token_error})
            token = {"access_token": "at", "token_type": "Bearer", "expires_in": 3600}
            if self.issues_id_token:
                token["id_token"] = self.id_token()
            return httpx2.Response(200, json=token)
        return httpx2.Response(404)

    def id_token(self) -> str:
        """Sign the ID token for the login in flight.

        Returns:
            The compact JWT.
        """
        now = int(time.time())
        claims = {
            "iss": ISSUER,
            "aud": CLIENT_ID,
            "sub": "google-1",
            "email": "Boss@Example.com",
            "email_verified": True,
            "name": "Boss",
            "iat": now,
            "exp": now + 3600,
            "nonce": self.nonce,
            **self.claims,
        }
        claims = {name: value for name, value in claims.items() if value is not None}
        return jwt.encode({"alg": "RS256", "kid": "google"}, claims, self.signing_key)


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


def _auth_config(super_admin_emails: list[str], allowed_domains: list[str]) -> SimpleNamespace:
    return SimpleNamespace(
        cookie_secure=False,
        session_expiry_days=1,
        super_admin_emails=super_admin_emails,
        allowed_domains=allowed_domains,
    )


def _client(
    store: FakeStore,
    google: FakeGoogle,
    super_admin_emails: list[str] | None = None,
    allowed_domains: list[str] | None = None,
    *,
    encryption_key: str = ENCRYPTION_KEY,
) -> TestClient:
    """Serve the auth routes at their real path, with a login talking to ``google``.

    Args:
        store: The fake store.
        google: The fake Google.
        super_admin_emails: The addresses promoted on login.
        allowed_domains: The domains signup is open to; empty for everyone.
        encryption_key: The secret the login's session cookie is signed from.

    Returns:
        A client that does not follow redirects.
    """
    login = GoogleLogin(
        client_id=CLIENT_ID,
        client_secret="client-secret",
        redirect_uri=REDIRECT_URI,
        encryption_key=encryption_key,
        cookie_secure=False,
        transport=httpx2.MockTransport(google.handle),
    )
    app = FastAPI()
    install_error_handlers(app)
    login.install(app)
    app.include_router(auth_module.router, prefix="/api")
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_google_login] = lambda: login
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(
        auth=_auth_config(super_admin_emails or [], allowed_domains or [])
    )
    return TestClient(app, follow_redirects=False)


def _start(client: TestClient, google: FakeGoogle, destination: str | None = None) -> dict[str, str]:
    """Start a login in ``client``'s browser and return what Google hands back.

    Args:
        client: The browser.
        google: The fake Google, told the nonce the login asked for.
        destination: The post-login destination to ask for.

    Returns:
        The callback's query parameters.
    """
    response = client.get("/api/auth/google", params={"redirect": destination} if destination else None)
    query = parse_qs(urlsplit(response.headers["location"]).query)
    google.nonce = query["nonce"][0]
    return {"code": "c", "state": query["state"][0]}


def _sign_in(client: TestClient, google: FakeGoogle, destination: str | None = None) -> httpx2.Response:
    """Run a whole login in ``client``'s browser.

    Args:
        client: The browser.
        google: The fake Google.
        destination: The post-login destination to ask for.

    Returns:
        The callback's response.
    """
    return client.get("/api/auth/google/callback", params=_start(client, google, destination))


@pytest.fixture
def google() -> FakeGoogle:
    """A working Google.

    Returns:
        The fake.
    """
    return FakeGoogle()


def test_listed_email_is_promoted_case_insensitively(google: FakeGoogle) -> None:
    store = FakeStore()
    response = _sign_in(_client(store, google, ["boss@example.com"]), google)
    assert response.status_code == 302
    assert store.promoted == [store.profile.id]
    assert store.profile.is_super_admin is True


def test_unlisted_email_is_not_promoted(google: FakeGoogle) -> None:
    store = FakeStore()
    response = _sign_in(_client(store, google, ["someone-else@example.com"]), google)
    assert response.status_code == 302
    assert store.promoted == []


def test_existing_super_admin_is_not_touched(google: FakeGoogle) -> None:
    store = FakeStore(is_super_admin=True)
    response = _sign_in(_client(store, google, ["boss@example.com"]), google)
    assert response.status_code == 302
    assert store.promoted == []


def test_signup_blocked_when_domain_not_allowed(google: FakeGoogle) -> None:
    store = FakeStore()
    response = _sign_in(_client(store, google, allowed_domains=["digitlcloud.com"]), google)
    assert response.status_code == 302
    assert response.headers["location"] == "/login?error=signup_not_allowed"
    assert store.upserted is None
    assert store.sessions_created == []
    assert "session_token" not in response.cookies


def test_signup_allowed_for_listed_domain(google: FakeGoogle) -> None:
    store = FakeStore()
    response = _sign_in(_client(store, google, allowed_domains=["example.com"]), google)
    assert response.status_code == 302
    assert response.headers["location"] == "/"
    assert store.upserted == {
        "google_id": "google-1",
        "email": "Boss@Example.com",
        "name": "Boss",
        "avatar_url": None,
    }
    assert store.sessions_created == [store.profile.id]


def test_existing_profile_bypasses_allowlist(google: FakeGoogle) -> None:
    store = FakeStore(exists=True)
    response = _sign_in(_client(store, google, allowed_domains=["digitlcloud.com"]), google)
    assert response.status_code == 302
    assert response.headers["location"] == "/"
    assert store.upserted is not None


def test_invited_email_can_sign_up(google: FakeGoogle) -> None:
    store = FakeStore(invited=True)
    response = _sign_in(_client(store, google, allowed_domains=["digitlcloud.com"]), google)
    assert response.status_code == 302
    assert response.headers["location"] == "/"
    assert store.upserted is not None


def test_super_admin_email_can_sign_up(google: FakeGoogle) -> None:
    store = FakeStore()
    response = _sign_in(_client(store, google, ["boss@example.com"], ["digitlcloud.com"]), google)
    assert response.status_code == 302
    assert response.headers["location"] == "/"
    assert store.promoted == [store.profile.id]


# -- PATCH /auth/me -------------------------------------------------------------


class FakeProfileStore:
    """In-memory stand-in for ``PATCH /auth/me``: the profile update and what ``GET /auth/me`` reads."""

    def __init__(self) -> None:
        self.org = SimpleNamespace(id=uuid4(), name="Dev Org", key="dev-org", created_at=None)
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
        {"id": str(store.org.id), "name": "Dev Org", "key": "dev-org", "role": "editor"}
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
    """``GET /auth/google``: hands the browser to Google's account picker."""

    def test_redirects_with_the_configured_client_scopes_and_pkce(self, google: FakeGoogle) -> None:
        response = _client(FakeStore(), google).get("/api/auth/google")

        assert response.status_code == 302
        location = response.headers["location"]
        assert location.startswith(AUTHORIZATION_ENDPOINT)
        query = parse_qs(urlsplit(location).query)
        assert query["client_id"] == [CLIENT_ID]
        assert query["redirect_uri"] == [REDIRECT_URI]
        assert query["response_type"] == ["code"]
        assert query["scope"] == ["openid email profile"]
        assert query["prompt"] == ["select_account"]
        assert query["code_challenge_method"] == ["S256"]
        assert query["code_challenge"] and query["nonce"] and query["state"]
        assert "access_type" not in query

    def test_the_session_cookie_is_short_lived_httponly_and_scoped_to_the_login(self, google: FakeGoogle) -> None:
        response = _client(FakeStore(), google).get("/api/auth/google")

        cookie = response.headers["set-cookie"]
        assert cookie.startswith(f"{SESSION_COOKIE}=")
        assert "httponly" in cookie
        assert "samesite=lax" in cookie
        assert f"path={SESSION_PATH}" in cookie
        assert f"Max-Age={SESSION_MAX_AGE}" in cookie

    def test_every_login_draws_a_fresh_state(self, google: FakeGoogle) -> None:
        client = _client(FakeStore(), google)

        assert _start(client, google)["state"] != _start(client, google)["state"]

    def test_an_unreachable_google_lands_on_the_login_page(self, google: FakeGoogle) -> None:
        google.discovery_status = 503

        response = _client(FakeStore(), google).get("/api/auth/google")

        assert response.status_code == 302
        assert response.headers["location"] == "/login?error=login_failed"

    def test_a_deployment_without_a_login_answers_503(self) -> None:
        app = FastAPI()
        install_error_handlers(app)
        app.include_router(auth_module.router, prefix="/api")

        response = TestClient(app, follow_redirects=False).get("/api/auth/google")

        assert response.status_code == 503
        assert response.json()["detail"] == "Google login not configured"


class TestGoogleCallbackFailures:
    """Every way a login can fail lands on the login page, with no session."""

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param({"aud": "another-client"}, id="another audience"),
            pytest.param({"iss": "https://evil.example.com"}, id="another issuer"),
            pytest.param({"exp": int(time.time()) - 3600, "iat": int(time.time()) - 7200}, id="expired"),
            pytest.param({"nonce": "another-nonce"}, id="another nonce"),
            pytest.param({"email_verified": False}, id="unverified email"),
            pytest.param({"email_verified": None}, id="no verification claim"),
            pytest.param({"email": None}, id="no email"),
        ],
    )
    def test_an_id_token_that_does_not_verify_is_refused(self, google: FakeGoogle, claims: dict[str, Any]) -> None:
        google.claims = claims
        store = FakeStore()

        response = _sign_in(_client(store, google), google)

        assert response.headers["location"] == "/login?error=login_failed"
        assert store.sessions_created == []

    def test_an_id_token_signed_by_another_key_is_refused(self, google: FakeGoogle) -> None:
        google.signing_key = FORGED_KEY
        store = FakeStore()

        response = _sign_in(_client(store, google), google)

        assert response.headers["location"] == "/login?error=login_failed"
        assert store.sessions_created == []

    def test_a_rejected_code_is_refused(self, google: FakeGoogle) -> None:
        google.token_error = "invalid_grant"
        store = FakeStore()

        response = _sign_in(_client(store, google), google)

        assert response.headers["location"] == "/login?error=login_failed"
        assert store.sessions_created == []

    def test_a_token_response_without_an_id_token_is_refused(self, google: FakeGoogle) -> None:
        google.issues_id_token = False
        store = FakeStore()

        response = _sign_in(_client(store, google), google)

        assert response.headers["location"] == "/login?error=login_failed"
        assert store.sessions_created == []

    def test_a_login_declined_at_google_is_refused(self, google: FakeGoogle) -> None:
        client = _client(FakeStore(), google)
        state = _start(client, google)["state"]

        response = client.get("/api/auth/google/callback", params={"error": "access_denied", "state": state})

        assert response.headers["location"] == "/login?error=login_failed"
        assert google.token_requests == []


class TestGoogleCallbackSession:
    """A successful login lands a session cookie and honours the return destination."""

    def test_the_session_cookie_is_set_httponly(self, google: FakeGoogle) -> None:
        response = _sign_in(_client(FakeStore(), google), google)

        assert response.status_code == 302
        assert response.cookies["session_token"] == "token"
        session_cookie = next(c for c in response.headers.get_list("set-cookie") if c.startswith("session_token="))
        assert "HttpOnly" in session_cookie
        assert "Max-Age=86400" in session_cookie

    def test_the_code_is_exchanged_with_its_pkce_verifier(self, google: FakeGoogle) -> None:
        _sign_in(_client(FakeStore(), google), google)

        ((form),) = google.token_requests
        assert form["code"] == ["c"]
        assert form["redirect_uri"] == [REDIRECT_URI]
        assert form["code_verifier"]

    def test_an_app_relative_destination_is_honoured(self, google: FakeGoogle) -> None:
        response = _sign_in(_client(FakeStore(), google), google, "/jobs/42?tab=runs")

        assert response.headers["location"] == "/jobs/42?tab=runs"

    @pytest.mark.parametrize(
        "destination",
        ["https://evil.example.com/", "evil.example.com", "//evil.example.com", "/\\evil.example.com"],
    )
    def test_an_off_site_destination_is_refused_in_favour_of_the_root(
        self, google: FakeGoogle, destination: str
    ) -> None:
        response = _sign_in(_client(FakeStore(), google), google, destination)

        assert response.headers["location"] == "/"

    def test_the_login_session_cookie_is_cleared(self, google: FakeGoogle) -> None:
        response = _sign_in(_client(FakeStore(), google), google)

        login_cookie = next(c for c in response.headers.get_list("set-cookie") if c.startswith(f"{SESSION_COOKIE}="))
        assert "expires=Thu, 01 Jan 1970" in login_cookie


class TestGoogleCallbackState:
    """A callback is honoured only in the browser that started the login, on any replica."""

    def test_a_callback_without_a_login_in_flight_is_refused(self, google: FakeGoogle) -> None:
        store = FakeStore()
        other_browser = _client(store, google)
        params = _start(_client(store, google), google)

        response = other_browser.get("/api/auth/google/callback", params=params)

        assert response.headers["location"] == "/login?error=login_expired"
        assert store.sessions_created == []
        assert google.token_requests == []

    @pytest.mark.parametrize("state", ["another-state", ""])
    def test_a_mismatched_state_is_refused(self, google: FakeGoogle, state: str) -> None:
        store = FakeStore()
        client = _client(store, google)
        _start(client, google)

        response = client.get("/api/auth/google/callback", params={"code": "c", "state": state})

        assert response.headers["location"] == "/login?error=login_expired"
        assert store.sessions_created == []
        assert google.token_requests == []

    def test_another_replica_completes_the_login(self, google: FakeGoogle) -> None:
        store = FakeStore()
        first, second = _client(store, google), _client(store, google)
        params = _start(first, google)
        second.cookies = first.cookies

        response = second.get("/api/auth/google/callback", params=params)

        assert response.headers["location"] == "/"
        assert store.sessions_created == [store.profile.id]

    def test_an_instance_with_another_encryption_key_refuses_the_login(self, google: FakeGoogle) -> None:
        store = FakeStore()
        first, second = _client(store, google), _client(store, google, encryption_key="another-key")
        params = _start(first, google)
        second.cookies = first.cookies

        response = second.get("/api/auth/google/callback", params=params)

        assert response.headers["location"] == "/login?error=login_expired"
        assert store.sessions_created == []


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


def _organisation(name: str, key: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), name=name, key=key, created_at=None)


class TestGetMe:
    """``GET /auth/me``: identity plus every organisation the caller belongs to, by key."""

    def test_no_cookie_is_a_401(self) -> None:
        client = _session_client(CallerStore())
        client.cookies.clear()

        response = client.get("/auth/me")

        assert response.status_code == 401
        assert response.json()["detail"] == "Not authenticated"

    def test_an_unresolvable_cookie_is_a_401(self) -> None:
        response = _session_client(CallerStore(authenticated=False)).get("/auth/me")

        assert response.status_code == 401

    def test_memberships_carry_the_key_and_the_role(self) -> None:
        dev, acme = _organisation("Dev Org", "dev-org"), _organisation("Acme", "acme")
        store = CallerStore(organisations=[dev, acme], roles={dev.id: "admin", acme.id: "viewer"})

        payload = _session_client(store).get("/auth/me").json()

        assert payload["email"] == "ada@example.com"
        assert payload["memberships"] == [
            {"id": str(dev.id), "name": "Dev Org", "key": "dev-org", "role": "admin"},
            {"id": str(acme.id), "name": "Acme", "key": "acme", "role": "viewer"},
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
        store = CallerStore(accepted=SimpleNamespace(id=org_id, name="Dev Org", key="dev-org", created_at=created_at))

        response = _session_client(store).post("/auth/accept-invite", json={"token": "invite-token"})

        assert response.status_code == 200
        assert store.accept_calls == [("invite-token", store.profile.id)]
        assert response.json() == {
            "id": str(org_id),
            "name": "Dev Org",
            "key": "dev-org",
            "created_at": "2026-01-02T00:00:00Z",
        }

    def test_an_unknown_or_expired_token_is_a_400(self) -> None:
        response = _session_client(CallerStore(accepted=None)).post("/auth/accept-invite", json={"token": "stale"})

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid or expired invitation"
