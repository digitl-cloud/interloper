"""Tests for ``interloper_api.routes.auth``: signing in, the login session, and the caller's own profile.

Logins run against Google and Microsoft, faked at the HTTP transport (their discovery documents,
their keys and token endpoints that sign real ID tokens), so the login runs Authlib's own
checks end to end; a lightweight fake store records what the callback does.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import time
from types import SimpleNamespace
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

import httpx2
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from interloper_db import ConflictError, NotFoundError, Page, ProfileQuery
from interloper_db.models import Identity
from joserfc import jwt
from joserfc.jwk import KeySet, RSAKey

from interloper_api.app import install_error_handlers
from interloper_api.auth import (
    SESSION_COOKIE,
    SESSION_MAX_AGE,
    SESSION_PATH,
    GoogleProvider,
    Login,
    LoginProvider,
    MicrosoftProvider,
)
from interloper_api.dependencies import (
    State,
    get_caller,
    get_features,
    get_login,
    get_login_provider,
    get_settings,
    get_store,
)
from interloper_api.notifications import LinkConfirmationEmail
from interloper_api.routes import auth as auth_module
from interloper_api.settings import AuthSettings, SmtpSettings

MAIL = SmtpSettings(host="smtp.example.com", user="mailer", password="placeholder")

ISSUER = "https://accounts.google.com"
AUTHORIZATION_ENDPOINT = f"{ISSUER}/o/oauth2/v2/auth"
TENANT = "0c1ea3a1-36e3-4a64-8a3a-2b3e8b0e2a51"
MICROSOFT = "https://login.microsoftonline.com"
MICROSOFT_ISSUER = f"{MICROSOFT}/{TENANT}/v2.0"
MICROSOFT_AUTHORIZATION_ENDPOINT = f"{MICROSOFT}/organizations/oauth2/v2.0/authorize"
CLIENT_ID = "client-id"
PUBLIC_URL = "http://testserver"
REDIRECT_URI = f"{PUBLIC_URL}/api/auth/google/callback"
ENCRYPTION_KEY = "test-encryption-key"
SIGNING_KEY = RSAKey.generate_key(2048, parameters={"kid": "signing"})
FORGED_KEY = RSAKey.generate_key(2048, parameters={"kid": "signing"})


class FakeIdentityProvider:
    """An OpenID provider's endpoints, served from memory.

    Attributes:
        provider_type: The login provider that talks to it.
        issuer: The issuer its discovery document names.
        authorization_endpoint: Where it sends the browser.
        token_endpoint: Where the code is exchanged.
        jwks_uri: Where its keys are published.
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

    provider_type: ClassVar[type[LoginProvider]]
    issuer: ClassVar[str]
    authorization_endpoint: ClassVar[str]
    token_endpoint: ClassVar[str]
    jwks_uri: ClassVar[str]

    def __init__(self) -> None:
        """Serve a working provider."""
        self.claims: dict[str, Any] = {}
        self.signing_key = SIGNING_KEY
        self.token_error: str | None = None
        self.issues_id_token = True
        self.discovery_status = 200
        self.nonce: str | None = None
        self.token_requests: list[dict[str, list[str]]] = []

    @property
    def name(self) -> str:
        """Name the provider in the login routes.

        Returns:
            The login provider's name.
        """
        return self.provider_type.name

    def account_claims(self) -> dict[str, Any]:
        """Name the account the ID token proves by default.

        Returns:
            The provider's own claims, before the token's time, audience and nonce.
        """
        raise NotImplementedError

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request to the provider.

        Args:
            request: The request Authlib sent.

        Returns:
            The provider's answer.
        """
        url = str(request.url)
        if url.endswith("/.well-known/openid-configuration"):
            return httpx2.Response(
                self.discovery_status,
                json={
                    "issuer": self.issuer,
                    "authorization_endpoint": self.authorization_endpoint,
                    "token_endpoint": self.token_endpoint,
                    "jwks_uri": self.jwks_uri,
                    "id_token_signing_alg_values_supported": ["RS256"],
                },
            )
        if url == self.jwks_uri:
            return httpx2.Response(200, json=KeySet([SIGNING_KEY]).as_dict(private=False))
        if url == self.token_endpoint:
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
            **self.account_claims(),
            "aud": CLIENT_ID,
            "iat": now,
            "exp": now + 3600,
            "nonce": self.nonce,
            **self.claims,
        }
        claims = {name: value for name, value in claims.items() if value is not None}
        return jwt.encode({"alg": "RS256", "kid": "signing"}, claims, self.signing_key)


class FakeGoogle(FakeIdentityProvider):
    """Google, whose issuer is its own."""

    provider_type = GoogleProvider
    issuer = ISSUER
    authorization_endpoint = AUTHORIZATION_ENDPOINT
    token_endpoint = "https://oauth2.googleapis.com/token"
    jwks_uri = "https://www.googleapis.com/oauth2/v3/certs"

    def account_claims(self) -> dict[str, Any]:
        """Name a Google account with a verified address.

        Returns:
            The account's claims.
        """
        return {
            "iss": ISSUER,
            "sub": "google-1",
            "email": "Boss@Example.com",
            "email_verified": True,
            "name": "Boss",
        }


class FakeMicrosoft(FakeIdentityProvider):
    """Microsoft's multi-tenant endpoint, whose discovery document names a templated issuer."""

    provider_type = MicrosoftProvider
    issuer = f"{MICROSOFT}/{{tenantid}}/v2.0"
    authorization_endpoint = MICROSOFT_AUTHORIZATION_ENDPOINT
    token_endpoint = f"{MICROSOFT}/organizations/oauth2/v2.0/token"
    jwks_uri = f"{MICROSOFT}/organizations/discovery/v2.0/keys"

    def account_claims(self) -> dict[str, Any]:
        """Name a work account whose domain owner verified its address.

        Returns:
            The account's claims.
        """
        return {
            "iss": MICROSOFT_ISSUER,
            "tid": TENANT,
            "oid": "oid-1",
            "sub": "pairwise-1",
            "email": "ada@contoso.com",
            "xms_edov": True,
            "name": "Ada",
        }


class FakeStore:
    """In-memory stand-in exposing only the store facet methods the callback calls.

    Attributes:
        holders: The profiles holding the address signing in.
        holder_issuers: The issuers the holder already signs in through.
        queried: The addresses the callback looked profiles up by.
        linked: The identities linked, as ``(profile id, issuer, subject)``;
            each then signs in to its profile.
    """

    def __init__(
        self, *, is_super_admin: bool = False, exists: bool = False, invited: bool = False, email: str | None = None
    ) -> None:
        self.profile = SimpleNamespace(id=uuid4(), is_super_admin=is_super_admin, email=email)
        self.exists = exists
        self.known_issuers: set[str] = set()
        self.invited = invited
        self.holders: list[SimpleNamespace] = []
        self.holder_issuers: list[str] = []
        self.queried: list[str | None] = []
        self.linked: list[tuple[UUID, str, str]] = []
        self.promoted: list[UUID] = []
        self.upserted: dict[str, Any] | None = None
        self.sessions_created: list[UUID] = []
        self.profiles = SimpleNamespace(
            get=self._get_profile,
            get_by_identity=self._get_profile_by_identity,
            list=self._list_profiles,
            issuers=lambda profile_id: self.holder_issuers,
            identities=self._identities,
            link=self._link,
            upsert=self._upsert_profile,
            set_super_admin=self._set_super_admin,
        )
        self.tokens = SimpleNamespace(create_session=self._create_session)
        self.invitations = SimpleNamespace(has_pending=self._has_pending_invitation)

    def _get_profile(self, profile_id: UUID) -> SimpleNamespace:
        holder = next((holder for holder in self.holders if holder.id == profile_id), None)
        if holder is None:
            raise NotFoundError(f"Profile {profile_id} not found")
        return holder

    def _get_profile_by_identity(self, issuer: str, subject: str) -> SimpleNamespace | None:
        for linked_profile, linked_issuer, linked_subject in self.linked:
            if (linked_issuer, linked_subject) == (issuer, subject):
                return SimpleNamespace(id=linked_profile)
        return self.profile if self.exists or issuer in self.known_issuers else None

    def _identities(self, profile_id: UUID) -> list[Identity]:
        return [
            Identity(issuer=linked_issuer, subject=linked_subject, profile_id=linked_profile)
            for linked_profile, linked_issuer, linked_subject in self.linked
            if linked_profile == profile_id
        ]

    def _list_profiles(self, query: ProfileQuery) -> Page:
        self.queried.append(query.email)
        return Page(items=self.holders, total=len(self.holders))

    def _link(self, profile_id: UUID, *, issuer: str, subject: str) -> None:
        self.linked.append((profile_id, issuer, subject))

    def _has_pending_invitation(self, email: str) -> bool:
        return self.invited

    def _upsert_profile(self, **kwargs) -> SimpleNamespace:
        self.upserted = kwargs
        if getattr(self.profile, "email", None) is None:
            self.profile.email = kwargs["email"]
        return self.profile

    def _set_super_admin(self, user_id: UUID, *, value: bool) -> SimpleNamespace:
        self.promoted.append(user_id)
        self.profile.is_super_admin = value
        return self.profile

    def _create_session(self, user_id: UUID) -> str:
        self.sessions_created.append(user_id)
        return "token"


def _auth_config(super_admin_emails: list[str], allowed_domains: list[str]) -> AuthSettings:
    return AuthSettings(
        cookie_secure=False,
        session_expiry_days=1,
        super_admin_emails=super_admin_emails,
        allowed_domains=allowed_domains,
    )


def _client(
    store: FakeStore,
    fake: FakeIdentityProvider,
    super_admin_emails: list[str] | None = None,
    allowed_domains: list[str] | None = None,
    *,
    also_offered: tuple[FakeIdentityProvider, ...] = (),
    smtp: SmtpSettings | None = None,
    encryption_key: str = ENCRYPTION_KEY,
) -> TestClient:
    """Serve the auth routes at their real path, offering a provider talking to each fake.

    Args:
        store: The fake store.
        fake: The fake provider the login runs against.
        super_admin_emails: The addresses promoted on login.
        allowed_domains: The domains signup is open to; empty for everyone.
        also_offered: Fakes of the other providers the deployment offers.
        smtp: The mail server; ``None`` for a deployment sending none.
        encryption_key: The secret the login's session cookie is signed from.

    Returns:
        A client that does not follow redirects.
    """
    providers = [
        offered.provider_type(
            client_id=CLIENT_ID,
            client_secret="client-secret",
            public_url=PUBLIC_URL,
            transport=httpx2.MockTransport(offered.handle),
        )
        for offered in (fake, *also_offered)
    ]
    login = Login(providers, encryption_key=encryption_key, cookie_secure=False)
    app = FastAPI()
    install_error_handlers(app)
    login.install(app)
    app.include_router(auth_module.router, prefix="/api")
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_login] = lambda: login
    app.dependency_overrides[get_login_provider] = lambda provider: login.providers[provider]
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(
        auth=_auth_config(super_admin_emails or [], allowed_domains or []),
        smtp=smtp or SmtpSettings(),
        server=SimpleNamespace(public_url=PUBLIC_URL),
    )
    return TestClient(app, follow_redirects=False)


def _state_client(state: State) -> TestClient:
    """Serve the auth routes over ``state``, with no dependency overridden.

    Args:
        state: The application's state.

    Returns:
        A client that does not follow redirects.
    """
    app = FastAPI()
    app.state.interloper = state
    install_error_handlers(app)
    app.include_router(auth_module.router, prefix="/api")
    return TestClient(app, follow_redirects=False)


def _start(client: TestClient, fake: FakeIdentityProvider, destination: str | None = None) -> dict[str, str]:
    """Start a login in ``client``'s browser and return what the provider hands back.

    Args:
        client: The browser.
        fake: The fake provider, told the nonce the login asked for.
        destination: The post-login destination to ask for.

    Returns:
        The callback's query parameters.
    """
    response = client.get(f"/api/auth/{fake.name}/login", params={"redirect": destination} if destination else None)
    query = parse_qs(urlsplit(response.headers["location"]).query)
    fake.nonce = query["nonce"][0]
    return {"code": "c", "state": query["state"][0]}


def _sign_in(client: TestClient, fake: FakeIdentityProvider, destination: str | None = None) -> httpx2.Response:
    """Run a whole login in ``client``'s browser.

    Args:
        client: The browser.
        fake: The fake provider.
        destination: The post-login destination to ask for.

    Returns:
        The callback's response.
    """
    return client.get(f"/api/auth/{fake.name}/callback", params=_start(client, fake, destination))


@pytest.fixture
def google() -> FakeGoogle:
    """A working Google.

    Returns:
        The fake.
    """
    return FakeGoogle()


@pytest.fixture
def microsoft() -> FakeMicrosoft:
    """A working Microsoft.

    Returns:
        The fake.
    """
    return FakeMicrosoft()


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


def test_promotion_reads_the_profiles_address_not_the_logins(google: FakeGoogle) -> None:
    store = FakeStore(exists=True, email="ada@example.com")
    response = _sign_in(_client(store, google, ["boss@example.com"]), google)
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
        "issuer": ISSUER,
        "subject": "google-1",
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
    app.dependency_overrides[get_features] = lambda: {"agent": False}
    app.dependency_overrides[get_login] = lambda: None
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


class TestListLoginProviders:
    """``GET /auth/providers``: what the login page offers."""

    def test_lists_the_providers_the_deployment_offers(self, app_state: State) -> None:
        provider = GoogleProvider(client_id=CLIENT_ID, client_secret="client-secret", public_url=PUBLIC_URL)
        login = Login([provider], encryption_key=ENCRYPTION_KEY, cookie_secure=False)

        response = _state_client(dataclasses.replace(app_state, login=login)).get("/api/auth/providers")

        assert response.status_code == 200
        assert response.json() == [{"name": "google", "label": "Google"}]

    def test_a_deployment_without_a_login_offers_none(self, app_state: State) -> None:
        response = _state_client(app_state).get("/api/auth/providers")

        assert response.status_code == 200
        assert response.json() == []


class TestLogin:
    """``GET /auth/{provider}/login``: hands the browser to the provider's account picker."""

    def test_redirects_with_the_configured_client_scopes_and_pkce(self, google: FakeGoogle) -> None:
        response = _client(FakeStore(), google).get("/api/auth/google/login")

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
        response = _client(FakeStore(), google).get("/api/auth/google/login")

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

        response = _client(FakeStore(), google).get("/api/auth/google/login")

        assert response.status_code == 302
        assert response.headers["location"] == "/login?error=login_failed"

    @pytest.mark.parametrize("route", ["login", "callback"])
    def test_a_provider_the_deployment_does_not_offer_answers_404(self, app_state: State, route: str) -> None:
        provider = GoogleProvider(client_id=CLIENT_ID, client_secret="client-secret", public_url=PUBLIC_URL)
        login = Login([provider], encryption_key=ENCRYPTION_KEY, cookie_secure=False)
        client = _state_client(dataclasses.replace(app_state, login=login))

        response = client.get(f"/api/auth/okta/{route}")

        assert response.status_code == 404
        assert response.json()["detail"] == "Unknown sign-in provider 'okta'"

    def test_a_deployment_without_a_login_answers_404(self, app_state: State) -> None:
        response = _state_client(app_state).get("/api/auth/google/login")

        assert response.status_code == 404
        assert response.json()["detail"] == "Unknown sign-in provider 'google'"


class TestCallbackFailures:
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


class TestCallbackSession:
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


class TestCallbackState:
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


class TestMicrosoftLogin:
    """A Microsoft work or school account signs in as the object in its own tenant."""

    def test_redirects_to_the_work_and_school_account_picker(self, microsoft: FakeMicrosoft) -> None:
        response = _client(FakeStore(), microsoft).get("/api/auth/microsoft/login")

        location = response.headers["location"]
        assert location.startswith(MICROSOFT_AUTHORIZATION_ENDPOINT)
        query = parse_qs(urlsplit(location).query)
        assert query["redirect_uri"] == [f"{PUBLIC_URL}/api/auth/microsoft/callback"]
        assert query["prompt"] == ["select_account"]
        assert query["code_challenge_method"] == ["S256"]

    def test_signs_in_the_object_in_its_tenant(self, microsoft: FakeMicrosoft) -> None:
        store = FakeStore()

        response = _sign_in(_client(store, microsoft), microsoft)

        assert response.headers["location"] == "/"
        assert store.upserted == {
            "issuer": MICROSOFT_ISSUER,
            "subject": "oid-1",
            "email": "ada@contoso.com",
            "name": "Ada",
            "avatar_url": None,
        }
        assert store.sessions_created == [store.profile.id]

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param({"iss": f"{MICROSOFT}/another-tenant/v2.0"}, id="issued by another tenant"),
            pytest.param({"tid": "another-tenant"}, id="naming another tenant"),
            pytest.param({"iss": FakeMicrosoft.issuer}, id="the templated issuer"),
            pytest.param({"tid": None}, id="no tenant"),
            pytest.param({"xms_edov": False}, id="unverified domain"),
            pytest.param({"xms_edov": None}, id="no domain verification claim"),
            pytest.param({"aud": "another-client"}, id="another audience"),
        ],
    )
    def test_an_id_token_that_does_not_verify_is_refused(
        self, microsoft: FakeMicrosoft, claims: dict[str, Any]
    ) -> None:
        microsoft.claims = claims
        store = FakeStore()

        response = _sign_in(_client(store, microsoft), microsoft)

        assert response.headers["location"] == "/login?error=login_failed"
        assert store.upserted is None
        assert store.sessions_created == []


@pytest.fixture
def delivered(monkeypatch: pytest.MonkeyPatch) -> list[tuple[LinkConfirmationEmail, str]]:
    """Record the link confirmation emails delivered, instead of sending them.

    Args:
        monkeypatch: Pytest's patcher.

    Returns:
        The emails delivered, each with its recipient.
    """
    sent: list[tuple[LinkConfirmationEmail, str]] = []
    monkeypatch.setattr(LinkConfirmationEmail, "deliver", lambda email, smtp, to: sent.append((email, to)))
    return sent


class TestCallbackLinking:
    """The address is the trust anchor: a first login joins the profile holding its address."""

    def test_a_login_claims_the_profile_holding_its_address_that_nothing_signs_in_to(
        self, microsoft: FakeMicrosoft, delivered: list
    ) -> None:
        store = FakeStore()
        holder = SimpleNamespace(id=uuid4(), email="ada@contoso.com")
        store.holders = [holder]

        response = _sign_in(_client(store, microsoft), microsoft)

        assert response.headers["location"] == "/"
        assert store.queried == ["ada@contoso.com"]
        assert store.linked == [(holder.id, MICROSOFT_ISSUER, "oid-1")]
        assert store.upserted is not None
        assert store.sessions_created == [store.profile.id]
        assert delivered == []

    def test_joining_a_profile_skips_the_signup_allowlist(self, microsoft: FakeMicrosoft) -> None:
        store = FakeStore()
        store.holders = [SimpleNamespace(id=uuid4(), email="ada@contoso.com")]

        response = _sign_in(_client(store, microsoft, allowed_domains=["digitlcloud.com"]), microsoft)

        assert response.headers["location"] == "/"
        assert store.sessions_created == [store.profile.id]

    def test_an_address_held_by_no_profile_signs_up(self, microsoft: FakeMicrosoft) -> None:
        store = FakeStore()

        response = _sign_in(_client(store, microsoft), microsoft)

        assert response.headers["location"] == "/"
        assert store.linked == []
        assert store.upserted is not None

    def test_an_address_several_profiles_hold_is_refused(self, microsoft: FakeMicrosoft) -> None:
        store = FakeStore()
        store.holders = [SimpleNamespace(id=uuid4()), SimpleNamespace(id=uuid4())]

        response = _sign_in(_client(store, microsoft), microsoft)

        assert response.headers["location"] == "/login?error=address_shared"
        assert store.linked == []
        assert store.upserted is None
        assert store.sessions_created == []
        assert "session_token" not in response.cookies

    def test_a_known_identity_signs_in_without_looking_up_its_address(self, microsoft: FakeMicrosoft) -> None:
        store = FakeStore(exists=True)
        store.holders = [SimpleNamespace(id=uuid4()), SimpleNamespace(id=uuid4())]

        response = _sign_in(_client(store, microsoft), microsoft)

        assert response.headers["location"] == "/"
        assert store.queried == []
        assert store.linked == []
        assert store.sessions_created == [store.profile.id]


class TestLinkProposal:
    """A login whose address a profile that signs in holds proposes a link, which the person proves.

    The proposal waits in the login session; signing in through one of the
    profile's own ways links it, or the link emailed to the address does.
    """

    @staticmethod
    def _propose(
        microsoft: FakeMicrosoft, google: FakeGoogle, *, smtp: SmtpSettings | None = None
    ) -> tuple[FakeStore, TestClient, httpx2.Response]:
        """Run a Microsoft login whose address a profile signing in with Google holds.

        Args:
            microsoft: The fake Microsoft the login runs against.
            google: The fake Google the profile signs in with.
            smtp: The mail server; ``None`` for a deployment sending none.

        Returns:
            The store, the browser, and the callback's response.
        """
        store = FakeStore()
        store.holders = [SimpleNamespace(id=store.profile.id, email="Ada@Contoso.com")]
        store.holder_issuers = [ISSUER]
        client = _client(store, microsoft, also_offered=(google,), smtp=smtp)
        return store, client, _sign_in(client, microsoft, "/jobs")

    @staticmethod
    def _emailed_token(client: TestClient, delivered: list) -> str:
        """Email the proposed link and read its token back from the email.

        Args:
            client: The browser holding the proposal.
            delivered: The emails delivered.

        Returns:
            The token the emailed link carries.
        """
        client.post("/api/auth/link/email")
        ((email, _),) = delivered
        return email.confirm_url.rsplit("/", 1)[1]

    def test_the_login_proposes_a_link_and_signs_nobody_in(
        self, microsoft: FakeMicrosoft, google: FakeGoogle, delivered: list
    ) -> None:
        store, client, response = self._propose(microsoft, google)

        assert response.headers["location"] == "/auth/link"
        assert "session_token" not in response.cookies
        assert store.sessions_created == []
        assert store.linked == []
        assert delivered == []
        proposal = client.get("/api/auth/link")
        assert proposal.status_code == 200
        assert proposal.json() == {
            "email": "Ada@Contoso.com",
            "provider": {"name": "microsoft", "label": "Microsoft"},
            "methods": [{"name": "google", "label": "Google"}],
            "email_available": False,
            "destination": "/jobs",
        }

    def test_signing_in_the_way_the_profile_does_links_the_account(
        self, microsoft: FakeMicrosoft, google: FakeGoogle
    ) -> None:
        store, client, _ = self._propose(microsoft, google)
        store.known_issuers = {ISSUER}

        response = _sign_in(client, google, "/jobs")

        assert response.headers["location"] == "/jobs"
        assert store.linked == [(store.profile.id, MICROSOFT_ISSUER, "oid-1")]
        assert store.sessions_created == [store.profile.id]
        assert client.get("/api/auth/link").status_code == 404

    def test_signing_in_to_another_profile_links_nothing(self, microsoft: FakeMicrosoft, google: FakeGoogle) -> None:
        store, client, _ = self._propose(microsoft, google)
        store.profile = SimpleNamespace(id=uuid4(), is_super_admin=False)
        store.known_issuers = {ISSUER}

        _sign_in(client, google)

        assert store.linked == []
        assert store.sessions_created == [store.profile.id]
        assert client.get("/api/auth/link").status_code == 404

    def test_the_link_can_be_emailed_instead(
        self, microsoft: FakeMicrosoft, google: FakeGoogle, delivered: list
    ) -> None:
        store, client, _ = self._propose(microsoft, google, smtp=MAIL)
        assert client.get("/api/auth/link").json()["email_available"] is True

        response = client.post("/api/auth/link/email")

        assert response.status_code == 204
        ((email, recipient),) = delivered
        assert recipient == "Ada@Contoso.com"
        assert email.provider_label == "Microsoft"
        assert email.confirm_url.startswith(f"{PUBLIC_URL}/auth/link/")
        assert store.linked == []
        assert client.get("/api/auth/link").status_code == 404

    def test_following_the_emailed_link_links_the_account_from_any_browser(
        self, microsoft: FakeMicrosoft, google: FakeGoogle, delivered: list
    ) -> None:
        store, client, _ = self._propose(microsoft, google, smtp=MAIL)
        token = self._emailed_token(client, delivered)

        response = _client(store, microsoft, also_offered=(google,)).post(
            "/api/auth/link/confirm", json={"token": token}
        )

        assert response.status_code == 200
        assert response.json()["provider"] == {"name": "microsoft", "label": "Microsoft"}
        assert store.linked == [(store.profile.id, MICROSOFT_ISSUER, "oid-1")]

    def test_following_it_again_changes_nothing(
        self, microsoft: FakeMicrosoft, google: FakeGoogle, delivered: list
    ) -> None:
        store, client, _ = self._propose(microsoft, google, smtp=MAIL)
        token = self._emailed_token(client, delivered)

        first = client.post("/api/auth/link/confirm", json={"token": token})
        again = client.post("/api/auth/link/confirm", json={"token": token})

        assert first.status_code == again.status_code == 200
        assert first.json() == again.json()
        assert store.linked == [(store.profile.id, MICROSOFT_ISSUER, "oid-1")]

    @pytest.mark.parametrize("change", ["altered", "address changed", "profile gone"])
    def test_a_link_that_no_longer_holds_links_nothing(
        self, microsoft: FakeMicrosoft, google: FakeGoogle, delivered: list, change: str
    ) -> None:
        store, client, _ = self._propose(microsoft, google, smtp=MAIL)
        token = self._emailed_token(client, delivered)
        if change == "altered":
            token = token[:-2] + ("aa" if not token.endswith("aa") else "bb")
        elif change == "address changed":
            store.holders[0].email = "ada.new@contoso.com"
        else:
            store.holders = []

        response = client.post("/api/auth/link/confirm", json={"token": token})

        assert response.status_code == 400
        assert response.json()["detail"] == "This link is invalid or has expired"
        assert store.linked == []

    def test_an_account_signing_in_elsewhere_since_is_refused(
        self, microsoft: FakeMicrosoft, google: FakeGoogle, delivered: list
    ) -> None:
        store, client, _ = self._propose(microsoft, google, smtp=MAIL)
        token = self._emailed_token(client, delivered)
        elsewhere = (uuid4(), MICROSOFT_ISSUER, "oid-1")
        store.linked.append(elsewhere)

        response = client.post("/api/auth/link/confirm", json={"token": token})

        assert response.status_code == 409
        assert store.linked == [elsewhere]

    def test_without_mail_the_link_cannot_be_emailed(
        self, microsoft: FakeMicrosoft, google: FakeGoogle, delivered: list
    ) -> None:
        _, client, _ = self._propose(microsoft, google)

        response = client.post("/api/auth/link/email")

        assert response.status_code == 409
        assert delivered == []
        assert client.get("/api/auth/link").status_code == 200

    def test_cancelling_drops_the_proposal(self, microsoft: FakeMicrosoft, google: FakeGoogle) -> None:
        store, client, _ = self._propose(microsoft, google)

        response = client.delete("/api/auth/link")

        assert response.status_code == 204
        assert client.get("/api/auth/link").status_code == 404
        store.known_issuers = {ISSUER}
        _sign_in(client, google)
        assert store.linked == []

    @pytest.mark.parametrize(("method", "path"), [("GET", "/api/auth/link"), ("POST", "/api/auth/link/email")])
    def test_without_a_proposal_there_is_nothing_to_act_on(
        self, microsoft: FakeMicrosoft, method: str, path: str
    ) -> None:
        response = _client(FakeStore(), microsoft).request(method, path)

        assert response.status_code == 404


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
        self.issuers: list[str] = []
        self.identities: list[SimpleNamespace] = []
        self.unlink_error: Exception | None = None
        self.unlinked: list[tuple[UUID, UUID]] = []
        self.revoked_sessions: list[UUID] = []
        self.kept_sessions: list[str | None] = []
        self.last_organisations: list[tuple[UUID, UUID | None]] = []
        self.accept_calls: list[tuple[str, UUID]] = []
        self._accepted = accepted
        self._organisations = organisations or []
        self._roles = roles or {}
        self._authenticated = authenticated
        self.tokens = SimpleNamespace(
            resolve=self._resolve, resolve_in=self._resolve_in, revoke_sessions=self._revoke_sessions
        )
        self.profiles = SimpleNamespace(
            update=self._update_profile,
            issuers=lambda profile_id: self.issuers,
            identities=lambda profile_id: self.identities,
            unlink=self._unlink,
        )
        self.organisations = SimpleNamespace(
            list=lambda query: Page(items=self._organisations, total=len(self._organisations)),
        )
        self.members = SimpleNamespace(
            role_of=lambda org_id, user: "admin" if user.is_super_admin else self._roles.get(org_id)
        )
        self.invitations = SimpleNamespace(accept=self._accept)

    def _resolve(self, raw: str, audience: Any) -> tuple[Any, Any] | None:
        return (self.profile, SimpleNamespace(kind="session")) if self._authenticated else None

    def _resolve_in(self, raw: str, audience: Any, org_id: UUID) -> tuple[Any, Any, Any, str | None] | None:
        resolved = self._resolve(raw, audience)
        if resolved is None:
            return None
        organisation = next((row for row in self._organisations if row.id == org_id), None)
        role = self.members.role_of(org_id, self.profile) if organisation is not None else None
        return *resolved, organisation, role

    def _update_profile(self, user_id: UUID, *, last_organisation_id: UUID | None = None) -> Any:
        self.last_organisations.append((user_id, last_organisation_id))
        return self.profile

    def _accept(self, token: str, user_id: UUID) -> Any:
        self.accept_calls.append((token, user_id))
        return self._accepted

    def _revoke_sessions(self, user_id: UUID, *, keep: str | None = None) -> None:
        self.revoked_sessions.append(user_id)
        self.kept_sessions.append(keep)

    def _unlink(self, profile_id: UUID, reference: UUID) -> None:
        if self.unlink_error is not None:
            raise self.unlink_error
        self.unlinked.append((profile_id, reference))


def _identity(issuer: str) -> SimpleNamespace:
    return SimpleNamespace(reference=uuid4(), issuer=issuer, created_at=dt.datetime(2026, 11, 17, 9, 30, tzinfo=dt.UTC))


def _both_providers() -> Login:
    providers = [
        GoogleProvider(client_id=CLIENT_ID, client_secret="client-secret", public_url=PUBLIC_URL),
        MicrosoftProvider(client_id=CLIENT_ID, client_secret="client-secret", public_url=PUBLIC_URL),
    ]
    return Login(providers, encryption_key=ENCRYPTION_KEY, cookie_secure=False)


def _session_client(store: CallerStore, login: Login | None = None) -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.include_router(auth_module.router)
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_settings] = lambda: SimpleNamespace(auth=_auth_config([], []))
    app.dependency_overrides[get_features] = lambda: {"agent": False}
    app.dependency_overrides[get_login] = lambda: login
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

    def test_reports_the_providers_the_caller_signs_in_through(self) -> None:
        providers = [
            GoogleProvider(client_id=CLIENT_ID, client_secret="client-secret", public_url=PUBLIC_URL),
            MicrosoftProvider(client_id=CLIENT_ID, client_secret="client-secret", public_url=PUBLIC_URL),
        ]
        login = Login(providers, encryption_key=ENCRYPTION_KEY, cookie_secure=False)
        store = CallerStore()
        store.issuers = [MICROSOFT_ISSUER]

        payload = _session_client(store, login).get("/auth/me").json()

        assert payload["login_providers"] == [{"name": "microsoft", "label": "Microsoft"}]

    def test_a_deployment_without_a_login_reports_no_provider(self) -> None:
        store = CallerStore()
        store.issuers = [ISSUER]

        assert _session_client(store).get("/auth/me").json()["login_providers"] == []


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
            ("GET", "/auth/identities", None),
            ("DELETE", "/auth/identities/00000000-0000-0000-0000-000000000000", None),
        ],
    )
    def test_a_bearer_is_refused(self, method: str, path: str, body: dict[str, str] | None) -> None:
        store = CallerStore()

        response = _session_client(store).request(
            method, path, json=body, headers={"Authorization": "Bearer ilp_leaked"}
        )

        assert response.status_code == 401
        assert store.last_organisations == []
        assert store.accept_calls == []
        assert store.unlinked == []

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
        created_at = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)
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


# -- Sign-in methods -----------------------------------------------------------


class TestListIdentities:
    """``GET /auth/identities``: the ways the caller signs in."""

    def test_names_the_provider_of_each(self) -> None:
        store = CallerStore()
        google, microsoft = _identity(ISSUER), _identity(MICROSOFT_ISSUER)
        store.identities = [google, microsoft]

        response = _session_client(store, _both_providers()).get("/auth/identities")

        assert response.status_code == 200
        assert [(item["id"], item["provider"]["name"]) for item in response.json()] == [
            (str(google.reference), "google"),
            (str(microsoft.reference), "microsoft"),
        ]

    def test_a_provider_the_deployment_no_longer_offers_is_named_by_none(self) -> None:
        store = CallerStore()
        store.identities = [_identity("https://idp.example.com")]

        response = _session_client(store, _both_providers()).get("/auth/identities")

        assert response.json()[0]["provider"] is None


class TestRemoveIdentity:
    """``DELETE /auth/identities/{id}``: a way to sign in goes, and so do the caller's other sessions."""

    def test_removes_it_and_ends_every_other_session(self) -> None:
        store = CallerStore()
        microsoft = _identity(MICROSOFT_ISSUER)
        store.identities = [_identity(ISSUER), microsoft]

        response = _session_client(store).delete(f"/auth/identities/{microsoft.reference}")

        assert response.status_code == 204
        assert store.unlinked == [(store.profile.id, microsoft.reference)]
        assert store.revoked_sessions == [store.profile.id]
        assert store.kept_sessions == ["tok"]

    @pytest.mark.parametrize(
        ("error", "status"),
        [
            pytest.param(NotFoundError("Identity not found"), 404, id="not the caller's"),
            pytest.param(ConflictError("A profile keeps at least one way to sign in"), 409, id="the last one"),
        ],
    )
    def test_a_refused_removal_ends_no_session(self, error: Exception, status: int) -> None:
        store = CallerStore()
        store.unlink_error = error

        response = _session_client(store).delete(f"/auth/identities/{uuid4()}")

        assert response.status_code == status
        assert store.revoked_sessions == []
