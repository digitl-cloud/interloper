"""Tests for ``interloper_api.authorization``: the server's pieces, below the routes."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from authlib.oauth2 import OAuth2Error
from cryptography.fernet import Fernet
from interloper_db import Audience, Store

from interloper_api.authorization import AUTHORIZE_PATH, AuthorizationServer, Client, InvalidTargetError

ISSUER = "https://app.example.com"
MCP_URL = "https://app.example.com/mcp"
USER_ID = uuid4()


@pytest.fixture
def server(oauth_store: Store) -> AuthorizationServer:
    return AuthorizationServer(
        oauth_store,
        issuer=ISSUER,
        mcp_url=MCP_URL,
        encryption_key="key-material",
        access_token_expiry=timedelta(minutes=60),
    )


class TestResources:
    def test_resources_name_the_api_and_the_mcp_endpoint(self, server: AuthorizationServer):
        assert server.resources == {f"{ISSUER}/api": Audience.API, MCP_URL: Audience.MCP}

    @pytest.mark.parametrize("url", [f"{ISSUER}/api", MCP_URL])
    def test_a_known_resource_resolves_to_its_audience(self, server: AuthorizationServer, url: str):
        assert server.resolve_resource([url]) is server.resources[url]

    @pytest.mark.parametrize("url", [f"{ISSUER}/api/", f"{MCP_URL}/"])
    def test_a_trailing_slash_names_the_same_resource(self, server: AuthorizationServer, url: str):
        assert server.resolve_resource([url]) is server.resources[url.rstrip("/")]

    @pytest.mark.parametrize(
        "values", [[], [f"{ISSUER}/api", MCP_URL], [f"{ISSUER}/apis"], ["https://evil.example.com/mcp"]]
    )
    def test_anything_else_is_invalid_target(self, server: AuthorizationServer, values: list[str]):
        with pytest.raises(InvalidTargetError) as raised:
            server.resolve_resource(values, redirect_uri="http://127.0.0.1/cb")

        assert raised.value.error == "invalid_target"
        assert raised.value.redirect_uri == "http://127.0.0.1/cb"


class TestFromSettings:
    def test_the_issuer_is_the_external_url(self, oauth_store: Store, fake_settings: SimpleNamespace):
        fake_settings.server.external_url = "https://app.example.com/"
        fake_settings.mcp.external_url = "https://mcp.example.com/mcp"

        server = AuthorizationServer.from_settings(oauth_store, fake_settings)

        assert server.issuer == "https://app.example.com"
        assert server.resources == {
            "https://app.example.com/api": Audience.API,
            "https://mcp.example.com/mcp": Audience.MCP,
        }
        assert server.access_token_expires_in == 3600

    def test_without_an_external_url_the_issuer_is_the_local_server(
        self, oauth_store: Store, fake_settings: SimpleNamespace
    ):
        server = AuthorizationServer.from_settings(oauth_store, fake_settings)

        assert server.issuer == "http://localhost:3000"
        assert server.resources["http://localhost:3000/mcp"] is Audience.MCP


class TestRequests:
    def test_the_request_uri_is_built_from_the_issuer(self, server: AuthorizationServer):
        request = server.authorization_request([("client_id", "c"), ("resource", "a"), ("resource", "b")])

        assert request.uri == f"{ISSUER}{AUTHORIZE_PATH}?client_id=c&resource=a&resource=b"
        assert request.payload.data == {"client_id": "c", "resource": "a"}
        assert request.payload.datalist["resource"] == ["a", "b"]

    def test_a_sealed_request_round_trips(self, server: AuthorizationServer):
        query = [("client_id", "c"), ("state", "xyz")]

        unsealed = server.unseal(server.seal(server.authorization_request(query), USER_ID), USER_ID)

        assert unsealed is not None
        assert unsealed.query == query

    def test_a_sealed_request_is_opaque(self, server: AuthorizationServer):
        sealed = server.seal(server.authorization_request([("client_id", "a-recognisable-client")]), USER_ID)

        assert "a-recognisable-client" not in sealed

    def test_a_sealed_request_lives_ten_minutes(self, server: AuthorizationServer, monkeypatch: pytest.MonkeyPatch):
        sealed = server.seal(server.authorization_request([("client_id", "c")]), USER_ID)
        now = __import__("time").time()

        monkeypatch.setattr("cryptography.fernet.time.time", lambda: now + 599)
        assert server.unseal(sealed, USER_ID) is not None
        monkeypatch.setattr("cryptography.fernet.time.time", lambda: now + 601)
        assert server.unseal(sealed, USER_ID) is None

    def test_a_request_sealed_for_another_user_is_refused(self, server: AuthorizationServer):
        sealed = server.seal(server.authorization_request([("client_id", "c")]), USER_ID)

        assert server.unseal(sealed, uuid4()) is None

    def test_a_request_sealed_under_another_key_is_refused(self, server: AuthorizationServer):
        foreign = Fernet(Fernet.generate_key()).encrypt(b'[["client_id", "c"]]').decode()

        assert server.unseal(foreign, USER_ID) is None
        assert server.unseal("not-a-token", USER_ID) is None


class TestResponses:
    def test_every_redirect_carries_the_issuer(self, server: AuthorizationServer):
        response = server.handle_response(302, "", [("Location", "http://127.0.0.1/cb?error=access_denied")])

        assert (
            response.headers["location"]
            == f"http://127.0.0.1/cb?error=access_denied&iss={ISSUER.replace(':', '%3A').replace('/', '%2F')}"
        )

    def test_an_error_redirect_carries_the_issuer(self, server: AuthorizationServer):
        error = OAuth2Error("Denied", redirect_uri="http://127.0.0.1/cb", state="xyz", error="access_denied")

        response = server.handle_error_response(server.authorization_request([]), error)

        assert response.status_code == 302
        assert "iss=https%3A%2F%2Fapp.example.com" in response.headers["location"]

    def test_a_json_body_is_encoded(self, server: AuthorizationServer):
        response = server.handle_response(400, {"error": "invalid_grant"}, [("Content-Type", "application/json")])

        assert response.status_code == 400
        assert response.body == b'{"error": "invalid_grant"}'
        assert "location" not in response.headers


class TestClient:
    def test_a_public_client_authenticates_with_none_only(self, oauth_store: Store):
        row, _ = oauth_store.oauth.register_client("Public", ["http://127.0.0.1/cb"], registered_by="dynamic")
        client = Client(row)

        assert client.check_endpoint_auth_method("none", "token")
        assert not client.check_endpoint_auth_method("client_secret_post", "token")
        assert not client.check_client_secret("anything")

    def test_a_confidential_client_never_passes_as_public(self, oauth_store: Store):
        row, secret = oauth_store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )
        assert secret is not None
        client = Client(row)

        assert not client.check_endpoint_auth_method("none", "token")
        assert client.check_endpoint_auth_method("client_secret_basic", "token")
        assert client.check_endpoint_auth_method("client_secret_post", "revocation")
        assert client.check_client_secret(secret)
        assert not client.check_client_secret(secret + "x")

    def test_a_client_redirects_only_to_its_registered_uris(self, oauth_store: Store):
        row, _ = oauth_store.oauth.register_client("Public", ["http://127.0.0.1/cb"], registered_by="dynamic")
        client = Client(row)

        assert client.check_redirect_uri("http://127.0.0.1/cb")
        assert not client.check_redirect_uri("http://127.0.0.1/cb/")
        assert client.get_default_redirect_uri() is None
        assert client.check_grant_type("refresh_token")
        assert not client.check_grant_type("client_credentials")
        assert client.check_response_type("code")
        assert not client.check_response_type("token")


class TestRedirectUris:
    @pytest.mark.parametrize(
        "uri",
        [
            "https://claude.ai/api/mcp/auth_callback",
            "http://127.0.0.1:8765/cb",
            "http://localhost/cb",
            "http://[::1]:9000/cb",
            "cursor://anysphere.cursor-retrieval/oauth/callback",
            "vscode://anthropic.claude-code/callback",
            "vscode-insiders://ms-vscode.vscode-insiders/callback",
            "windsurf://codeium.windsurf/callback",
        ],
    )
    def test_https_loopback_http_and_known_private_use_schemes_are_safe(self, uri: str):
        assert Client.is_safe_redirect_uri(uri)

    @pytest.mark.parametrize(
        "uri",
        [
            "javascript://x/%0Aalert(document.domain)//",
            "JAVASCRIPT:alert(1)",
            "\x00javascript:alert(1)",
            " javascript:alert(1)",
            "java\nscript:alert(1)",
            "data:text/html,hi",
            "vbscript:msgbox",
            "file:///etc/passwd",
            "blob:https://app.example.com/x",
            "about:blank",
            "filesystem:https://app.example.com/x",
            "view-source:https://app.example.com/",
            "intent://scan/#Intent;scheme=zxing;end",
            "ms-msdt:/id PCWDiagnostic",
            "search-ms:query=x",
            "ms-officecmd:{}",
            "com.example.app:/oauth/callback",
            "http://evil.example/cb",
            "http://localhost.evil.example/cb",
            "https:///no-host",
            "https://[::1/cb",
            "no-scheme/cb",
            "",
        ],
    )
    def test_unknown_schemes_remote_http_and_hostless_https_are_unsafe(self, uri: str):
        assert not Client.is_safe_redirect_uri(uri)

    def test_a_registered_unsafe_uri_is_refused_anyway(self, oauth_store: Store):
        unsafe = "javascript://x/%0Aalert(document.domain)//"
        row, _ = oauth_store.oauth.register_client("Stored", [unsafe], registered_by="operator")

        assert not Client(row).check_redirect_uri(unsafe)

    @pytest.mark.parametrize("uri", ["http://127.0.0.1:51234/cb", "http://127.0.0.1/cb", "http://127.0.0.1:9/cb"])
    def test_a_loopback_uri_matches_its_registration_on_any_port(self, oauth_store: Store, uri: str):
        row, _ = oauth_store.oauth.register_client("Native", ["http://127.0.0.1:8765/cb"], registered_by="dynamic")

        assert Client(row).check_redirect_uri(uri)

    @pytest.mark.parametrize(
        "uri", ["http://127.0.0.1:8765/other", "http://localhost:8765/cb", "https://127.0.0.1:8765/cb"]
    )
    def test_a_loopback_uri_still_matches_its_scheme_host_and_path(self, oauth_store: Store, uri: str):
        row, _ = oauth_store.oauth.register_client("Native", ["http://127.0.0.1:8765/cb"], registered_by="dynamic")

        assert not Client(row).check_redirect_uri(uri)

    def test_an_https_uri_matches_its_registration_exactly(self, oauth_store: Store):
        row, _ = oauth_store.oauth.register_client("Web", ["https://app.example.com:8443/cb"], registered_by="dynamic")

        assert not Client(row).check_redirect_uri("https://app.example.com/cb")
        assert Client(row).check_redirect_uri("https://app.example.com:8443/cb")

    def test_ipv6_loopback_keeps_its_brackets(self):
        assert Client.without_loopback_port("http://[::1]:9000/cb") == "http://[::1]/cb"

    def test_a_loopback_uri_with_an_invalid_port_is_left_as_is(self):
        assert Client.without_loopback_port("http://127.0.0.1:99999/cb") == "http://127.0.0.1:99999/cb"
