"""Tests for ``interloper_api.routes.authorization``: the OAuth 2.1 flow through the API.

Every test drives the real routes over a real store (in-memory SQLite). The
client talks plain http to an issuer configured as https, as the API does
behind the gateway, so the request URL Authlib sees is the issuer's.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from authlib.oauth2.rfc7636 import create_s256_code_challenge
from fastapi.testclient import TestClient
from interloper_db import Audience, Profile, Store

from interloper_api.app import create_app

ISSUER = "https://testserver"
API_RESOURCE = f"{ISSUER}/api"
MCP_RESOURCE = f"{ISSUER}/mcp"
REDIRECT_URI = "http://127.0.0.1:8765/callback"
VERIFIER = "a-code-verifier-long-enough-to-satisfy-rfc-7636"
PREFIX = "/api/auth/oauth"


@pytest.fixture
def client(oauth_store: Store, fake_settings: SimpleNamespace) -> TestClient:
    fake_settings.agent.enabled = False
    fake_settings.server.external_url = ISSUER
    fake_settings.mcp.external_url = MCP_RESOURCE
    return TestClient(create_app(store=oauth_store, settings=fake_settings), follow_redirects=False)


@pytest.fixture
def user(oauth_store: Store, client: TestClient) -> Profile:
    """A user logged in to the app: the client carries their session cookie.

    Returns:
        The profile.
    """
    profile = oauth_store.profiles.upsert(google_id="g-user", email="user@example.com", name="User")
    client.cookies.set("session_token", oauth_store.tokens.create_session(profile.id))
    return profile


def _register(client: TestClient, **metadata: Any) -> dict[str, Any]:
    body = {
        "client_name": "Claude Code",
        "redirect_uris": [REDIRECT_URI],
        "token_endpoint_auth_method": "none",
        "grant_types": ["authorization_code", "refresh_token"],
        **metadata,
    }
    response = client.post(f"{PREFIX}/register", json=body)
    assert response.status_code == 201, response.text
    return response.json()


def _authorize_params(client_id: str, **overrides: str | None) -> dict[str, str]:
    params: dict[str, str | None] = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "state": "xyz",
        "code_challenge": create_s256_code_challenge(VERIFIER),
        "code_challenge_method": "S256",
        "resource": MCP_RESOURCE,
        **overrides,
    }
    return {key: value for key, value in params.items() if value is not None}


def _query(location: str) -> dict[str, str]:
    return {key: values[0] for key, values in parse_qs(urlsplit(location).query).items()}


def _sealed(client: TestClient, client_id: str, **overrides: str | None) -> str:
    response = client.get(f"{PREFIX}/authorize", params=_authorize_params(client_id, **overrides))
    assert response.status_code == 302, response.text
    location = response.headers["location"]
    assert location.startswith("/authorize?request=")
    return _query(location)["request"]


def _decide(client: TestClient, sealed: str, *, approve: bool) -> str:
    response = client.post(f"{PREFIX}/consent", json={"request": sealed, "approve": approve})
    assert response.status_code == 200, response.text
    return response.json()["redirect_to"]


def _code(client: TestClient, client_id: str, **overrides: str | None) -> str:
    redirect_to = _decide(client, _sealed(client, client_id, **overrides), approve=True)
    assert redirect_to.startswith(REDIRECT_URI)
    return _query(redirect_to)["code"]


def _exchange(client: TestClient, client_id: str, code: str, **overrides: str) -> Any:
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": client_id,
        "code_verifier": VERIFIER,
        **overrides,
    }
    return client.post(f"{PREFIX}/token", data=form)


def _refresh(client: TestClient, client_id: str, refresh_token: str, **overrides: str) -> Any:
    form = {"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": client_id, **overrides}
    return client.post(f"{PREFIX}/token", data=form)


def _tokens(client: TestClient, client_id: str) -> dict[str, Any]:
    response = _exchange(client, client_id, _code(client, client_id))
    assert response.status_code == 200, response.text
    return response.json()


# -- Metadata & registration ---------------------------------------------------


def test_metadata(client: TestClient):
    response = client.get("/.well-known/oauth-authorization-server")

    assert response.status_code == 200
    assert response.json() == {
        "issuer": ISSUER,
        "authorization_endpoint": f"{ISSUER}{PREFIX}/authorize",
        "token_endpoint": f"{ISSUER}{PREFIX}/token",
        "registration_endpoint": f"{ISSUER}{PREFIX}/register",
        "revocation_endpoint": f"{ISSUER}{PREFIX}/revoke",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
        "revocation_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
        "authorization_response_iss_parameter_supported": True,
    }


def test_register_dynamic_client(client: TestClient, oauth_store: Store):
    registered = _register(client, client_uri="https://claude.ai")

    assert "client_secret" not in registered
    assert registered["client_name"] == "Claude Code"
    assert registered["redirect_uris"] == [REDIRECT_URI]
    assert registered["token_endpoint_auth_method"] == "none"
    assert registered["grant_types"] == ["authorization_code", "refresh_token"]
    stored = oauth_store.oauth.get_client(registered["client_id"])
    assert stored is not None
    assert stored.secret_hash is None
    assert stored.registered_by == "dynamic"


def test_register_confidential_client_gets_a_secret(client: TestClient, oauth_store: Store):
    registered = _register(client, token_endpoint_auth_method="client_secret_post")

    assert registered["client_secret"].startswith("ilc_")
    assert registered["client_secret_expires_at"] == 0
    stored = oauth_store.oauth.get_client(registered["client_id"])
    assert stored is not None
    assert stored.secret_hash is not None


@pytest.mark.parametrize(
    "redirect_uri",
    [
        "javascript://x/%0Aalert(document.domain)//",
        "JavaScript://x/%0Aalert(document.domain)//",
        " javascript://x/%0Aalert(document.domain)//",
        "java\tscript://x/%0Aalert(document.domain)//",
        "data://text/html,hi",
        "vbscript://x/msgbox",
        "file:///etc/passwd",
        "blob://x/y",
        "http://evil.example/cb",
        "http://127.0.0.1.evil.example/cb",
        "view-source:https://testserver/",
        "intent://scan/#Intent;scheme=zxing;end",
        "ms-msdt:/id PCWDiagnostic",
        "search-ms:query=x",
        "ms-officecmd:{}",
        "com.example.app:/oauth/callback",
    ],
)
def test_register_refuses_an_unsafe_redirect_uri(client: TestClient, oauth_store: Store, redirect_uri: str):
    response = client.post(f"{PREFIX}/register", json={"redirect_uris": [REDIRECT_URI, redirect_uri]})

    assert response.status_code == 400
    assert response.json()["error"] in {"invalid_redirect_uri", "invalid_client_metadata"}


@pytest.mark.parametrize(
    "redirect_uri",
    [
        "https://claude.ai/api/mcp/auth_callback",
        "http://127.0.0.1:8765/callback",
        "http://localhost:3334/callback",
        "http://[::1]:9000/callback",
        "cursor://anysphere.cursor-retrieval/oauth/user-interloper/callback",
        "vscode://vscode.github-authentication/did-authenticate",
    ],
)
def test_register_accepts_a_safe_redirect_uri(client: TestClient, redirect_uri: str):
    assert _register(client, redirect_uris=[redirect_uri])["redirect_uris"] == [redirect_uri]


@pytest.mark.parametrize("body", ["not json", "[1, 2]"])
def test_register_refuses_a_body_that_is_not_a_json_object(client: TestClient, body: str):
    response = client.post(f"{PREFIX}/register", content=body, headers={"Content-Type": "application/json"})

    assert response.status_code == 400


def test_register_cuts_a_long_client_name(client: TestClient, oauth_store: Store):
    registered = _register(client, client_name="x" * 500)

    assert registered["client_name"] == "x" * 100
    stored = oauth_store.oauth.get_client(registered["client_id"])
    assert stored is not None and stored.name == "x" * 100


def test_an_unsafe_redirect_uri_already_stored_is_never_used(client: TestClient, oauth_store: Store):
    unsafe = "javascript://x/%0Aalert(document.domain)//"
    row, _ = oauth_store.oauth.register_client("Stored", [unsafe], registered_by="operator")

    response = client.get(f"{PREFIX}/authorize", params=_authorize_params(row.client_id, redirect_uri=unsafe))

    assert response.status_code == 400
    assert "location" not in response.headers


@pytest.mark.parametrize("redirect_uris", [None, []])
def test_register_requires_redirect_uris(client: TestClient, redirect_uris: list[str] | None):
    body: dict[str, Any] = {"client_name": "No redirect", "token_endpoint_auth_method": "none"}
    if redirect_uris is not None:
        body["redirect_uris"] = redirect_uris

    response = client.post(f"{PREFIX}/register", json=body)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_redirect_uri"


def test_register_refuses_an_unsupported_grant_type(client: TestClient):
    response = client.post(
        f"{PREFIX}/register", json={"redirect_uris": [REDIRECT_URI], "grant_types": ["client_credentials"]}
    )

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_client_metadata"


# -- Authorization -------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"code_challenge": None, "code_challenge_method": None},
        {"code_challenge_method": "plain"},
        {"code_challenge_method": None},
    ],
)
def test_authorize_without_pkce_refused(client: TestClient, overrides: dict[str, str | None]):
    client_id = _register(client)["client_id"]

    response = client.get(f"{PREFIX}/authorize", params=_authorize_params(client_id, **overrides))

    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith(REDIRECT_URI)
    assert _query(location) == {
        "error": "invalid_request",
        "error_description": _query(location)["error_description"],
        "state": "xyz",
        "iss": ISSUER,
    }


@pytest.mark.parametrize("resource", ["https://evil.example.com/api", None])
def test_authorize_unknown_resource_invalid_target(client: TestClient, resource: str | None):
    client_id = _register(client)["client_id"]

    response = client.get(f"{PREFIX}/authorize", params=_authorize_params(client_id, resource=resource))

    assert response.status_code == 302
    query = _query(response.headers["location"])
    assert query["error"] == "invalid_target"
    assert query["state"] == "xyz"
    assert query["iss"] == ISSUER


def test_authorize_unknown_redirect_uri_never_redirects(client: TestClient):
    client_id = _register(client)["client_id"]

    response = client.get(
        f"{PREFIX}/authorize", params=_authorize_params(client_id, redirect_uri="https://evil.example.com/cb")
    )

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"


def test_authorize_accepts_a_loopback_redirect_uri_on_another_port(client: TestClient, user: Profile):
    client_id = _register(client)["client_id"]
    redirect_uri = "http://127.0.0.1:51234/callback"

    redirect_to = _decide(client, _sealed(client, client_id, redirect_uri=redirect_uri), approve=True)

    assert redirect_to.startswith(redirect_uri)
    code = _query(redirect_to)["code"]
    assert _exchange(client, client_id, code, redirect_uri=redirect_uri).status_code == 200


def test_authorize_without_a_session_logs_in_first(client: TestClient):
    client_id = _register(client)["client_id"]

    response = client.get(f"{PREFIX}/authorize", params=_authorize_params(client_id))

    assert response.status_code == 302
    login = urlsplit(response.headers["location"])
    assert login.path == "/api/auth/google"
    return_path = _query(response.headers["location"])["redirect"]
    assert urlsplit(return_path).path == f"{PREFIX}/authorize"
    assert _query(return_path) == _authorize_params(client_id)


def test_authorize_with_a_session_goes_to_consent(client: TestClient, user: Profile):
    client_id = _register(client)["client_id"]

    assert _sealed(client, client_id)


def test_consent_describes_the_request(client: TestClient, user: Profile):
    client_id = _register(client)["client_id"]

    response = client.get(f"{PREFIX}/consent", params={"request": _sealed(client, client_id)})

    assert response.status_code == 200
    assert response.json() == {"client_name": "Claude Code", "redirect_host": "127.0.0.1:8765", "resource": "mcp"}


def test_consent_answers_the_session_cookie_only(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    sealed = _sealed(client, client_id)
    _, personal = oauth_store.tokens.create_personal(user.id, "laptop")
    client.cookies.clear()
    bearer = {"Authorization": f"Bearer {personal}"}

    assert client.get(f"{PREFIX}/consent", params={"request": sealed}, headers=bearer).status_code == 401
    posted = client.post(f"{PREFIX}/consent", json={"request": sealed, "approve": True}, headers=bearer)
    assert posted.status_code == 401


def test_consent_refuses_a_request_sealed_for_another_user(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    sealed = _sealed(client, client_id)
    other = oauth_store.profiles.upsert(google_id="g-other", email="other@example.com", name="Other")
    client.cookies.set("session_token", oauth_store.tokens.create_session(other.id))

    assert client.get(f"{PREFIX}/consent", params={"request": sealed}).status_code == 400
    posted = client.post(f"{PREFIX}/consent", json={"request": sealed, "approve": True})
    assert posted.status_code == 400
    assert oauth_store.oauth.authorised_clients(other.id) == []


def test_consent_refuses_a_tampered_request(client: TestClient, user: Profile):
    client_id = _register(client)["client_id"]
    sealed = _sealed(client, client_id)
    tampered = sealed[:-4] + ("AAAA" if not sealed.endswith("AAAA") else "BBBB")

    assert client.get(f"{PREFIX}/consent", params={"request": tampered}).status_code == 400
    posted = client.post(f"{PREFIX}/consent", json={"request": tampered, "approve": True})
    assert posted.status_code == 400


def test_consent_refuses_a_request_whose_client_was_removed(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    sealed = _sealed(client, client_id)
    oauth_store.oauth.delete_client(client_id)

    assert client.get(f"{PREFIX}/consent", params={"request": sealed}).status_code == 400
    posted = client.post(f"{PREFIX}/consent", json={"request": sealed, "approve": True})
    assert posted.status_code == 400
    assert "location" not in posted.headers
    assert oauth_store.oauth.authorised_clients(user.id) == []


def test_consent_deny_returns_access_denied(client: TestClient, user: Profile):
    client_id = _register(client)["client_id"]

    redirect_to = _decide(client, _sealed(client, client_id), approve=False)

    assert redirect_to.startswith(REDIRECT_URI)
    query = _query(redirect_to)
    assert "code" not in query
    assert (query["error"], query["state"], query["iss"]) == ("access_denied", "xyz", ISSUER)


# -- Token ---------------------------------------------------------------------


def test_full_flow_issues_access_and_refresh(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    redirect_to = _decide(client, _sealed(client, client_id), approve=True)
    query = _query(redirect_to)
    assert query["state"] == "xyz"
    assert query["iss"] == ISSUER

    response = _exchange(client, client_id, query["code"])

    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    token = response.json()
    assert token["token_type"] == "Bearer"
    assert token["expires_in"] == 3600
    assert token["access_token"].startswith("ila_")
    assert token["refresh_token"].startswith("ilr_")
    resolved = oauth_store.tokens.resolve(token["access_token"], Audience.MCP)
    assert resolved is not None
    assert resolved[0].id == user.id
    assert resolved[1].client_id == client_id
    assert oauth_store.tokens.resolve(token["access_token"], Audience.API) is None


def test_full_flow_for_the_api_resource(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]

    response = _exchange(client, client_id, _code(client, client_id, resource=API_RESOURCE))

    assert response.status_code == 200, response.text
    access = response.json()["access_token"]
    assert oauth_store.tokens.resolve(access, Audience.API) is not None
    assert oauth_store.tokens.resolve(access, Audience.MCP) is None


def test_code_bound_to_client_redirect_and_verifier(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    other_id = _register(client, client_name="Other")["client_id"]

    wrong_verifier = _exchange(client, client_id, _code(client, client_id), code_verifier="b" * 43)
    other_client = _exchange(client, other_id, _code(client, client_id))
    wrong_redirect = _exchange(client, client_id, _code(client, client_id), redirect_uri="http://127.0.0.1:9/cb")
    wrong_resource = _exchange(client, client_id, _code(client, client_id), resource=API_RESOURCE)

    assert wrong_verifier.json()["error"] == "invalid_grant"
    assert other_client.json()["error"] == "invalid_grant"
    assert wrong_redirect.json()["error"] == "invalid_grant"
    assert wrong_resource.json()["error"] == "invalid_target"
    assert oauth_store.oauth.authorised_clients(user.id) == []


def test_code_single_use_revokes_what_it_issued(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    code = _code(client, client_id)
    token = _exchange(client, client_id, code).json()

    replay = _exchange(client, client_id, code)

    assert replay.status_code == 400
    assert replay.json()["error"] == "invalid_grant"
    assert oauth_store.tokens.resolve(token["access_token"], Audience.MCP) is None
    assert _refresh(client, client_id, token["refresh_token"]).json()["error"] == "invalid_grant"


def test_a_grant_revoked_while_issuing_is_refused(
    client: TestClient, oauth_store: Store, user: Profile, monkeypatch: pytest.MonkeyPatch
):
    client_id = _register(client)["client_id"]
    code = _code(client, client_id)
    monkeypatch.setattr(oauth_store.oauth, "issue_tokens", lambda *args, **kwargs: None)

    response = _exchange(client, client_id, code)

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_grant"


def test_confidential_client_cannot_pass_as_public(client: TestClient, user: Profile):
    registered = _register(client, token_endpoint_auth_method="client_secret_post")
    client_id = registered["client_id"]
    code = _code(client, client_id)

    without_secret = _exchange(client, client_id, code)
    with_secret = _exchange(client, client_id, _code(client, client_id), client_secret=registered["client_secret"])

    assert without_secret.json()["error"] == "invalid_client"
    assert with_secret.status_code == 200, with_secret.text


def test_refresh_rotates(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    first = _tokens(client, client_id)

    response = _refresh(client, client_id, first["refresh_token"], resource=MCP_RESOURCE, scope="anything")

    assert response.status_code == 200, response.text
    second = response.json()
    assert second["refresh_token"] != first["refresh_token"]
    assert oauth_store.tokens.resolve(second["access_token"], Audience.MCP) is not None


def test_refresh_bound_to_its_resource(client: TestClient, user: Profile):
    client_id = _register(client)["client_id"]
    first = _tokens(client, client_id)

    mismatch = _refresh(client, client_id, first["refresh_token"], resource=API_RESOURCE)
    retry = _refresh(client, client_id, first["refresh_token"])

    assert mismatch.json()["error"] == "invalid_target"
    assert retry.status_code == 200, retry.text


def test_refresh_reuse_invalid_grant(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    first = _tokens(client, client_id)
    second = _refresh(client, client_id, first["refresh_token"]).json()

    reuse = _refresh(client, client_id, first["refresh_token"])

    assert reuse.status_code == 400
    assert reuse.json()["error"] == "invalid_grant"
    assert _refresh(client, client_id, second["refresh_token"]).json()["error"] == "invalid_grant"
    assert oauth_store.tokens.resolve(second["access_token"], Audience.MCP) is None
    assert oauth_store.oauth.authorised_clients(user.id) == []


# -- Revocation ----------------------------------------------------------------


def test_revoke(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    first = _tokens(client, client_id)
    second = _tokens(client, client_id)

    revoked_access = client.post(f"{PREFIX}/revoke", data={"token": first["access_token"], "client_id": client_id})
    revoked_refresh = client.post(
        f"{PREFIX}/revoke",
        data={"token": second["refresh_token"], "token_type_hint": "refresh_token", "client_id": client_id},
    )
    unknown = client.post(f"{PREFIX}/revoke", data={"token": "ila_no-such-token", "client_id": client_id})

    assert revoked_access.status_code == 200
    assert oauth_store.tokens.resolve(first["access_token"], Audience.MCP) is None
    assert revoked_refresh.status_code == 200
    assert _refresh(client, client_id, second["refresh_token"]).json()["error"] == "invalid_grant"
    assert oauth_store.tokens.resolve(second["access_token"], Audience.MCP) is None
    assert unknown.status_code == 200
    assert unknown.json() == {}


def test_revoke_leaves_personal_tokens_alone(client: TestClient, oauth_store: Store, user: Profile):
    client_id = _register(client)["client_id"]
    _, personal = oauth_store.tokens.create_personal(user.id, "laptop")

    response = client.post(f"{PREFIX}/revoke", data={"token": personal, "client_id": client_id})

    assert response.status_code == 200
    assert oauth_store.tokens.resolve(personal, Audience.API) is not None


def test_an_unconfigured_server_answers_503(fake_settings: SimpleNamespace, oauth_store: Store):
    fake_settings.agent.enabled = False
    fake_settings.secrets.encryption_key = ""
    client = TestClient(create_app(store=oauth_store, settings=fake_settings))

    assert client.get("/.well-known/oauth-authorization-server").status_code == 503
    assert client.post(f"{PREFIX}/token", data={"grant_type": "refresh_token"}).status_code == 503


# -- Cross-origin access -------------------------------------------------------


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/.well-known/oauth-authorization-server"),
        ("POST", f"{PREFIX}/register"),
        ("POST", f"{PREFIX}/token"),
        ("POST", f"{PREFIX}/revoke"),
    ],
)
def test_a_client_facing_endpoint_answers_any_origin(client: TestClient, method: str, path: str):
    preflight = client.options(
        path,
        headers={
            "Origin": "https://inspector.example",
            "Access-Control-Request-Method": method,
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "*"
    assert "access-control-allow-credentials" not in preflight.headers


def test_the_metadata_carries_the_cors_header(client: TestClient):
    response = client.get("/.well-known/oauth-authorization-server", headers={"Origin": "https://inspector.example"})

    assert response.headers["access-control-allow-origin"] == "*"


@pytest.mark.parametrize("path", [f"{PREFIX}/authorize", f"{PREFIX}/consent"])
def test_the_browser_steps_stay_same_origin(client: TestClient, path: str):
    response = client.options(
        path, headers={"Origin": "https://inspector.example", "Access-Control-Request-Method": "GET"}
    )

    assert "access-control-allow-origin" not in response.headers


def test_the_app_cors_policy_does_not_refuse_a_client_facing_endpoint(
    oauth_store: Store, fake_settings: SimpleNamespace
):
    fake_settings.agent.enabled = False
    app = create_app(store=oauth_store, settings=fake_settings, cors_origins=["http://localhost:3000"])

    preflight = TestClient(app).options(
        f"{PREFIX}/token", headers={"Origin": "https://inspector.example", "Access-Control-Request-Method": "POST"}
    )

    assert preflight.status_code == 200
    assert preflight.headers["access-control-allow-origin"] == "*"
