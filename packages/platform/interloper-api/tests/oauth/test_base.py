"""Tests for ``interloper_api.oauth``: the server's pieces, below the routes."""

from __future__ import annotations

import json
import time
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx2
import pytest
from authlib.oauth2 import OAuth2Error
from authlib.oauth2.rfc6749 import InvalidClientError
from cryptography import fernet
from cryptography.fernet import Fernet
from interloper_db import Audience, Store

from interloper_api.oauth import (
    AUTHORIZE_PATH,
    AuthorizationServer,
    Client,
    ClientMetadataDocument,
    ClientMetadataFetcher,
    InvalidTargetError,
)
from interloper_api.settings import ApiSettings

ISSUER = "https://app.example.com"
MCP_URL = "https://app.example.com/mcp"
USER_ID = uuid4()


@pytest.fixture
def server(oauth_store: Store) -> AuthorizationServer:
    return AuthorizationServer(
        oauth_store,
        issuer=ISSUER,
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
    def test_the_issuer_is_the_external_url_and_the_resources_live_under_it(
        self, oauth_store: Store, fake_settings: ApiSettings
    ):
        fake_settings.server.external_url = "https://app.example.com/"

        server = AuthorizationServer.from_settings(oauth_store, fake_settings)

        assert server.issuer == "https://app.example.com"
        assert server.resources == {
            "https://app.example.com/api": Audience.API,
            "https://app.example.com/mcp": Audience.MCP,
        }
        assert server.access_token_expires_in == 3600

    def test_without_an_external_url_the_issuer_is_the_local_server(
        self, oauth_store: Store, fake_settings: ApiSettings
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

        monkeypatch.setattr(fernet, "time", SimpleNamespace(time=lambda: now + 599))
        assert server.unseal(sealed, USER_ID) is not None
        monkeypatch.setattr(fernet, "time", SimpleNamespace(time=lambda: now + 601))
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

    def test_a_rotated_client_accepts_only_its_new_secret(self, oauth_store: Store):
        row, old = oauth_store.oauth.register_client(
            "Slack", ["https://slack.example.com/cb"], registered_by="operator", confidential=True
        )
        assert old is not None

        rotated = oauth_store.oauth.rotate_secret(row.client_id)

        assert rotated is not None
        rotated_row, new = rotated
        assert Client(rotated_row).check_client_secret(new)
        assert not Client(rotated_row).check_client_secret(old)

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


CLIENT_ID = "https://claude.ai/oauth/claude-code-client-metadata"
PUBLIC_ADDRESS = "160.79.104.10"


def _document(**overrides: Any) -> dict[str, Any]:
    document = {
        "client_id": CLIENT_ID,
        "client_name": "Claude Code",
        "client_uri": "https://claude.ai",
        "redirect_uris": ["http://localhost/callback", "http://127.0.0.1/callback"],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
        **overrides,
    }
    return {key: value for key, value in document.items() if value is not None}


class TestClientMetadataDocument:
    def test_a_valid_document_describes_the_client(self):
        document = ClientMetadataDocument.from_json(CLIENT_ID, _document())

        assert document == ClientMetadataDocument(
            client_id=CLIENT_ID,
            name="Claude Code",
            redirect_uris=["http://localhost/callback", "http://127.0.0.1/callback"],
        )

    def test_a_document_without_a_name_is_named_after_its_host(self):
        assert ClientMetadataDocument.from_json(CLIENT_ID, _document(client_name=None)).name == "claude.ai"

    def test_a_long_name_is_cut(self):
        assert len(ClientMetadataDocument.from_json(CLIENT_ID, _document(client_name="x" * 500)).name) == 100

    @pytest.mark.parametrize(
        "overrides",
        [
            {"token_endpoint_auth_method": None},
            {"token_endpoint_auth_method": None, "token_endpoint_auth_methods_supported": ["private_key_jwt", "none"]},
        ],
    )
    def test_a_public_client_is_accepted(self, overrides: dict[str, Any]):
        assert ClientMetadataDocument.from_json(CLIENT_ID, _document(**overrides)).client_id == CLIENT_ID

    @pytest.mark.parametrize(
        "data",
        [
            [],
            "a string",
            _document(client_id="https://claude.ai/oauth/another-client"),
            _document(client_id=CLIENT_ID + "/"),
            _document(client_id=None),
            _document(client_secret="shared"),
            _document(client_secret_expires_at=0),
            _document(token_endpoint_auth_method="client_secret_basic"),
            _document(token_endpoint_auth_method="private_key_jwt"),
            _document(token_endpoint_auth_method=None, token_endpoint_auth_methods_supported=["private_key_jwt"]),
            _document(grant_types=["client_credentials"]),
            _document(response_types=["token"]),
            _document(redirect_uris=None),
            _document(redirect_uris=[]),
            _document(redirect_uris="http://localhost/callback"),
            _document(redirect_uris=[42]),
            _document(redirect_uris=["http://evil.example/callback"]),
            _document(redirect_uris=["javascript://x/%0Aalert(document.domain)//"]),
        ],
    )
    def test_an_invalid_document_is_refused(self, data: Any):
        with pytest.raises(InvalidClientError) as raised:
            ClientMetadataDocument.from_json(CLIENT_ID, data)

        assert raised.value.redirect_uri is None

    @pytest.mark.parametrize(
        "client_id",
        [CLIENT_ID, "https://client.example.com:8443/metadata.json", "https://client.example.com/metadata?v=1"],
    )
    def test_an_https_url_with_a_path_is_a_client_id(self, client_id: str):
        assert ClientMetadataDocument.check_client_id(client_id).scheme == "https"

    @pytest.mark.parametrize(
        "client_id",
        [
            "http://client.example.com/metadata.json",
            "https://client.example.com",
            "https://client.example.com/",
            "https://user:pass@client.example.com/metadata.json",
            "https://client.example.com@evil.example/metadata.json",
            "https://client.example.com/metadata.json#fragment",
            "https://client.example.com/metadata.json#",
            "https://client.example.com/./metadata.json",
            "https://client.example.com/a/../metadata.json",
            "https:///metadata.json",
            "https://client.example.com:99999/metadata.json",
            "https://client.example.com/meta data.json",
            "https://client.example.com/metadata.json\n",
        ],
    )
    def test_anything_else_is_refused(self, client_id: str):
        with pytest.raises(InvalidClientError):
            ClientMetadataDocument.check_client_id(client_id)


@pytest.fixture
def host(document_host: Any) -> Any:
    document_host.documents[CLIENT_ID] = _document()
    return document_host


@pytest.fixture
def fetcher(host: Any) -> ClientMetadataFetcher:
    return host.fetcher()


class TestClientMetadataFetcher:
    def test_the_document_is_read_from_the_vetted_address_under_its_host_name(
        self, fetcher: ClientMetadataFetcher, host: Any
    ):
        document = fetcher.fetch(CLIENT_ID)

        assert document.name == "Claude Code"
        [request] = host.requests
        assert str(request.url) == f"https://{PUBLIC_ADDRESS}/oauth/claude-code-client-metadata"
        assert request.headers["host"] == "claude.ai"
        assert request.headers["accept"] == "application/json"
        assert "cookie" not in request.headers
        assert request.extensions["sni_hostname"] == "claude.ai"

    def test_an_ipv6_address_is_bracketed_and_the_port_kept(self, fetcher: ClientMetadataFetcher, host: Any):
        client_id = "https://client.example.com:8443/metadata.json"
        host.addresses["client.example.com"] = ["2606:4700::6810:84e5"]
        host.respond = lambda request: httpx2.Response(200, json=_document(client_id=client_id))

        fetcher.fetch(client_id)

        [request] = host.requests
        assert str(request.url) == "https://[2606:4700::6810:84e5]:8443/metadata.json"
        assert request.headers["host"] == "client.example.com:8443"

    @pytest.mark.parametrize(
        "addresses",
        [
            ["127.0.0.1"],
            ["10.0.0.7"],
            ["192.168.1.1"],
            ["169.254.169.254"],
            ["100.64.0.1"],
            ["0.0.0.0"],
            ["224.0.0.1"],
            ["::1"],
            ["fd00::1"],
            ["fe80::1%en0"],
            ["::ffff:10.0.0.1"],
            [PUBLIC_ADDRESS, "10.0.0.7"],
            [],
        ],
    )
    def test_a_host_resolving_to_any_address_that_is_not_public_is_never_fetched(
        self, fetcher: ClientMetadataFetcher, host: Any, addresses: list[str]
    ):
        host.addresses["claude.ai"] = addresses

        with pytest.raises(InvalidClientError):
            fetcher.fetch(CLIENT_ID)

        assert host.requests == []

    def test_an_unknown_host_is_refused(self, fetcher: ClientMetadataFetcher, host: Any):
        del host.addresses["claude.ai"]

        with pytest.raises(InvalidClientError):
            fetcher.fetch(CLIENT_ID)

    @pytest.mark.parametrize(
        "response",
        [
            httpx2.Response(302, headers={"Location": "https://claude.ai/elsewhere"}),
            httpx2.Response(404),
            httpx2.Response(500),
            httpx2.Response(201, json=_document()),
            httpx2.Response(200, content=b"not json"),
            httpx2.Response(200, content=json.dumps(_document(client_name="x" * 6000)).encode()),
            httpx2.Response(200, json=_document(client_id="https://claude.ai/oauth/another-client")),
        ],
    )
    def test_anything_but_a_valid_document_under_200_is_refused(
        self, fetcher: ClientMetadataFetcher, host: Any, response: httpx2.Response
    ):
        host.respond = lambda request: response

        with pytest.raises(InvalidClientError):
            fetcher.fetch(CLIENT_ID)

        assert len(host.requests) == 1

    def test_a_document_still_arriving_at_the_deadline_is_refused(self):
        response = httpx2.Response(200, json=_document())

        with pytest.raises(InvalidClientError) as raised:
            ClientMetadataFetcher._read(response, deadline=time.monotonic() - 1)

        assert raised.value.description == "The client metadata document took too long to read."

    def test_a_network_failure_is_refused(self, fetcher: ClientMetadataFetcher, host: Any):
        def fail(request: httpx2.Request) -> httpx2.Response:
            raise httpx2.ConnectTimeout("timed out", request=request)

        host.respond = fail

        with pytest.raises(InvalidClientError):
            fetcher.fetch(CLIENT_ID)

    def test_an_invalid_client_id_is_refused_before_any_lookup(self, fetcher: ClientMetadataFetcher, host: Any):
        with pytest.raises(InvalidClientError):
            fetcher.fetch("https://claude.ai/")

        assert host.requests == []


class TestDiscoverClient:
    @pytest.fixture
    def server(self, oauth_store: Store, host: Any) -> AuthorizationServer:
        server = AuthorizationServer(
            oauth_store, issuer=ISSUER, encryption_key="key-material", access_token_expiry=timedelta(minutes=60)
        )
        server.client_metadata = host.fetcher()
        return server

    def test_a_url_client_is_read_and_kept(self, server: AuthorizationServer, oauth_store: Store):
        server.discover_client(CLIENT_ID)

        stored = oauth_store.oauth.get_client(CLIENT_ID)
        assert stored is not None
        assert stored.registered_by == "metadata"
        assert stored.name == "Claude Code"
        assert stored.redirect_uris == ["http://localhost/callback", "http://127.0.0.1/callback"]

    @pytest.mark.parametrize("client_id", [None, "0123456789abcdef0123456789abcdef", "http://claude.ai/oauth/x"])
    def test_any_other_client_id_reads_nothing(self, server: AuthorizationServer, host: Any, client_id: Any):
        server.discover_client(client_id)

        assert host.requests == []

    @pytest.mark.parametrize("client_id", [f"{ISSUER}/api/auth/apps", ISSUER])
    def test_a_url_on_the_servers_own_origin_is_refused_unread(
        self, server: AuthorizationServer, host: Any, client_id: str
    ):
        with pytest.raises(InvalidClientError):
            server.discover_client(client_id)

        assert host.requests == []
