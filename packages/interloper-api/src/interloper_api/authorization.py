"""interloper's OAuth 2.1 authorization server, on Authlib's framework-agnostic core.

Authlib ships server integrations for Flask and Django only. This module is
the Starlette one: the four framework methods (``create_oauth2_request``,
``create_json_request``, ``handle_response``, ``send_signal``), request
snapshots Authlib can read synchronously, and the storage hooks over
``store.oauth`` and ``store.tokens``. The routes read the body once, on the
event loop, and call the server from the threadpool, where every hook runs.

What Authlib leaves to the server, and this one decides:

- A redirect URI is ``https``, ``http`` on a loopback host, or the
  private-use scheme of a known native client (RFC 8252), never any other
  scheme: the consent page navigates to it from the app's origin, and a
  browser or the operating system runs or reads many schemes locally.
  Authlib only checks that it parses. A loopback redirect URI matches its
  registration on any port (RFC 8252 section 7.3): a native app binds an
  ephemeral one.
- PKCE is required on every authorization request, ``S256`` only. Authlib's
  ``CodeChallenge(required=True)`` accepts a request with no challenge and
  defaults a missing method to ``plain``.
- RFC 8707 ``resource`` binds every grant to one surface. Authlib ignores the
  parameter; here an authorization request names exactly one known resource,
  or gets ``invalid_target``. Omitting it is refused too: OAuth 2.1 allows the
  omission, but a default would bind a token to a surface the client did not
  name, and the MCP specification requires clients to send it. A trailing
  slash on it is not part of the resource's name.
- The URL Authlib sees is built from the issuer, never from the request, so
  plain http behind a TLS-terminating gateway is not refused as insecure.
- Every redirect to a client carries the RFC 9207 ``iss`` parameter, errors
  included.
- The store generates every secret. Codes, access and refresh tokens are
  issued by the store calls, so Authlib's generators and ``save_*`` hooks
  have nothing left to do.
- The authorization request waits for the user's consent sealed with the
  instance's encryption key (Fernet: encrypted and authenticated), for ten
  minutes, with the user who started it, so the consent page can neither
  read nor alter it, and no other user's session can decide on it.
"""

from __future__ import annotations

import hmac
import json
import time
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import urlencode, urlsplit
from uuid import UUID

from authlib.common.urls import add_params_to_uri
from authlib.consts import default_json_headers
from authlib.oauth2 import OAuth2Error, rfc6749
from authlib.oauth2.rfc6749 import (
    AuthorizationCodeMixin,
    ClientMixin,
    InvalidGrantError,
    InvalidRequestError,
    JsonPayload,
    JsonRequest,
    OAuth2Payload,
    OAuth2Request,
    TokenMixin,
    grants,
)
from authlib.oauth2.rfc7009 import RevocationEndpoint
from authlib.oauth2.rfc7591 import ClientRegistrationEndpoint, InvalidRedirectURIError
from authlib.oauth2.rfc7636 import CodeChallenge
from authlib.oauth2.rfc8414 import AuthorizationServerMetadata
from cryptography.fernet import Fernet, InvalidToken
from interloper_db import (
    Audience,
    AuthorizationCode,
    NotFoundError,
    OAuthClient,
    Profile,
    RefreshToken,
    Store,
    Token,
)
from interloper_db.crypto import derive_fernet_key, hash_token
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

PREFIX = "/api/auth/oauth"

AUTHORIZE_PATH = f"{PREFIX}/authorize"

TOKEN_PATH = f"{PREFIX}/token"

REGISTRATION_PATH = f"{PREFIX}/register"

REVOCATION_PATH = f"{PREFIX}/revoke"

METADATA_PATH = "/.well-known/oauth-authorization-server"

#: The endpoints a client calls itself, cross-origin when it runs in a browser
#: (MCP Inspector); authorization and consent run on the app's own origin.
CROSS_ORIGIN_PATHS = frozenset({METADATA_PATH, REGISTRATION_PATH, TOKEN_PATH, REVOCATION_PATH})

CONSENT_REQUEST_TTL = timedelta(minutes=10)

CLIENT_AUTH_METHODS = ["none", "client_secret_post", "client_secret_basic"]

CONFIDENTIAL_AUTH_METHODS = ("client_secret_post", "client_secret_basic")

GRANT_TYPES = ["authorization_code", "refresh_token"]

PRIVATE_USE_SCHEMES = frozenset({"cursor", "vscode", "vscode-insiders", "windsurf"})

CLIENT_NAME_MAX_LENGTH = 100

LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


# -- Requests ------------------------------------------------------------------


class RequestPayload(OAuth2Payload):
    """Request parameters as Authlib reads them: first value by name, and every value."""

    def __init__(self, items: list[tuple[str, str]]) -> None:
        """Index the parameters.

        Args:
            items: The parameters in request order, repeated names kept.
        """
        self._data: dict[str, str] = {}
        self._datalist: defaultdict[str, list[str]] = defaultdict(list)
        for name, value in items:
            self._data.setdefault(name, value)
            self._datalist[name].append(value)

    @property
    def data(self) -> dict[str, str]:
        """The first value of each parameter.

        Returns:
            Parameter name to value.
        """
        return self._data

    @property
    def datalist(self) -> defaultdict[str, list[str]]:
        """Every value of each parameter, so a repeated one can be refused.

        Returns:
            Parameter name to its values, in request order.
        """
        return self._datalist


class OAuthRequest(OAuth2Request):
    """A snapshot of an authorization, token or revocation request.

    Authlib reads parameters synchronously, so the route reads the body
    first and hands over its parameters.

    Attributes:
        query: The query parameters, in order; what a sealed authorization
            request carries.
        payload: Every parameter, query and body.
        client: The client, once Authlib has validated or authenticated it.
        user: The user, once known.
        authorization_code: The spent code, during a code exchange.
        refresh_token: The rotated refresh token, during a refresh.
    """

    payload: RequestPayload
    client: Any
    user: Any
    authorization_code: Any
    refresh_token: Any

    def __init__(
        self,
        method: str,
        uri: str,
        headers: Any,
        *,
        query: list[tuple[str, str]] | None = None,
        form: list[tuple[str, str]] | None = None,
    ) -> None:
        """Snapshot a request.

        Args:
            method: The HTTP method.
            uri: The URL Authlib sees, built from the issuer.
            headers: The request headers; a case-insensitive mapping.
            query: The query parameters, in order.
            form: The urlencoded body's parameters, in order.
        """
        super().__init__(method, uri, headers=headers)
        self.query = query or []
        self._form = form or []
        self.payload = RequestPayload(self.query + self._form)

    @property
    def args(self) -> dict[str, str]:
        """The first value of each query parameter.

        Returns:
            Parameter name to value.
        """
        return dict(reversed(self.query))

    @property
    def form(self) -> dict[str, str]:
        """The first value of each body parameter.

        Returns:
            Parameter name to value.
        """
        return dict(reversed(self._form))


class RegistrationPayload(JsonPayload):
    """A registration request's JSON body."""

    def __init__(self, data: dict[str, Any]) -> None:
        """Hold the body.

        Args:
            data: The decoded JSON object.
        """
        self._data = data

    @property
    def data(self) -> dict[str, Any]:
        """The client metadata the request carries.

        Returns:
            The decoded JSON object.
        """
        return self._data


class RegistrationRequest(JsonRequest):
    """A snapshot of a dynamic client registration request (RFC 7591)."""

    def __init__(self, uri: str, headers: Any, data: dict[str, Any]) -> None:
        """Snapshot a request.

        Args:
            uri: The URL Authlib sees, built from the issuer.
            headers: The request headers.
            data: The decoded JSON body.
        """
        super().__init__("POST", uri, headers)
        self.payload = RegistrationPayload(data)


# -- Errors --------------------------------------------------------------------


class InvalidTargetError(OAuth2Error):
    """The requested ``resource`` is missing, repeated or unknown (RFC 8707)."""

    error = "invalid_target"


# -- Models --------------------------------------------------------------------


@dataclass(frozen=True)
class Client(ClientMixin):
    """A registered client, as Authlib asks about it.

    A client with no secret is public and authenticates with ``none`` only;
    one with a secret may never pass as public, since ``none`` accepts any
    known client id. Every client may use both grants.

    Attributes:
        row: The stored client.
    """

    row: OAuthClient

    def get_client_id(self) -> str:
        """Name the client.

        Returns:
            Its client id.
        """
        return self.row.client_id

    def get_default_redirect_uri(self) -> None:
        """Pick no redirect URI: every authorization request names its own, which its code is bound to."""

    def get_allowed_scope(self, scope: str) -> str:
        """Grant no scope: a client acts as the user, with their role in each organisation.

        Args:
            scope: The scope the client asked for, ignored.

        Returns:
            The empty scope.
        """
        return ""

    @staticmethod
    def is_safe_redirect_uri(uri: str) -> bool:
        """Whether a URI may receive codes: ``https``, loopback ``http``, or a known private-use scheme.

        A URI carrying whitespace or a control character is refused outright,
        since browsers strip them and could read another scheme than the one
        checked here.

        Args:
            uri: The redirect URI.

        Returns:
            ``True`` for ``https`` with a host, ``http`` on ``localhost``,
            ``127.0.0.1`` or ``[::1]``, and the schemes of
            :data:`PRIVATE_USE_SCHEMES`.
        """
        if any(ord(character) <= 0x20 or ord(character) == 0x7F for character in uri):
            return False
        try:
            parts = urlsplit(uri)
            hostname = parts.hostname
        except ValueError:
            return False
        if parts.scheme == "https":
            return bool(hostname)
        if parts.scheme == "http":
            return hostname in LOOPBACK_HOSTS
        return parts.scheme in PRIVATE_USE_SCHEMES

    @staticmethod
    def without_loopback_port(uri: str) -> str:
        """Drop the port of a loopback ``http`` URI, which a native app picks per request.

        Args:
            uri: The redirect URI.

        Returns:
            The URI without its port when it is ``http`` on a loopback host,
            otherwise the URI unchanged.
        """
        try:
            parts = urlsplit(uri)
            port = parts.port
        except ValueError:
            return uri
        if parts.scheme != "http" or parts.hostname not in LOOPBACK_HOSTS or port is None:
            return uri
        return parts._replace(netloc=parts.netloc.rsplit(":", 1)[0]).geturl()

    def check_redirect_uri(self, redirect_uri: str) -> bool:
        """Accept only a safe redirect URI the client registered: exactly, or on any port of a loopback one.

        Registration already refuses an unsafe URI; this guards a client
        stored any other way.

        Args:
            redirect_uri: The URI a request names.

        Returns:
            Whether it is registered and safe.
        """
        requested = self.without_loopback_port(redirect_uri)
        registered = {self.without_loopback_port(uri) for uri in self.row.redirect_uris}
        return requested in registered and self.is_safe_redirect_uri(redirect_uri)

    def check_client_secret(self, client_secret: str) -> bool:
        """Compare a presented secret with the stored hash, in constant time.

        Args:
            client_secret: The secret as presented.

        Returns:
            Whether it is the client's; always ``False`` for a public client.
        """
        if self.row.secret_hash is None:
            return False
        return hmac.compare_digest(hash_token(client_secret), self.row.secret_hash)

    def check_endpoint_auth_method(self, method: str, endpoint: str) -> bool:
        """Accept the authentication methods the client's kind allows, on every endpoint.

        Args:
            method: The method the request authenticated with.
            endpoint: The endpoint, ``token`` or ``revocation``.

        Returns:
            ``none`` for a public client; a secret method for a confidential one.
        """
        if self.row.secret_hash is None:
            return method == "none"
        return method in CONFIDENTIAL_AUTH_METHODS

    def check_response_type(self, response_type: str) -> bool:
        """Accept the authorization code flow only.

        Args:
            response_type: The requested response type.

        Returns:
            Whether it is ``code``.
        """
        return response_type == "code"

    def check_grant_type(self, grant_type: str) -> bool:
        """Accept the two grants the server runs.

        Args:
            grant_type: The requested grant type.

        Returns:
            Whether it is ``authorization_code`` or ``refresh_token``.
        """
        return grant_type in GRANT_TYPES


@dataclass(frozen=True)
class Code(AuthorizationCodeMixin):
    """A spent authorization code, as the token exchange checks it.

    Attributes:
        row: The stored code, just consumed.
    """

    row: AuthorizationCode

    @property
    def code_challenge(self) -> str:
        """The PKCE challenge the exchange's verifier must match.

        Returns:
            The ``S256`` challenge.
        """
        return self.row.code_challenge

    @property
    def code_challenge_method(self) -> str:
        """The PKCE method, the only one accepted.

        Returns:
            ``S256``.
        """
        return "S256"

    def get_redirect_uri(self) -> str:
        """The redirect URI the exchange must repeat.

        Returns:
            The URI the code was sent to.
        """
        return self.row.redirect_uri

    def get_scope(self) -> str:
        """The scope the code grants.

        Returns:
            The empty scope.
        """
        return ""


@dataclass(frozen=True)
class Credential(TokenMixin):
    """A refresh or access token, as the refresh grant and the revocation endpoint check it.

    Attributes:
        row: The stored refresh token, or the ``tokens`` row of an access token.
    """

    row: RefreshToken | Token

    def check_client(self, client: Client) -> bool:
        """Whether the token was issued to a client.

        Args:
            client: The authenticated client.

        Returns:
            Whether the token is that client's.
        """
        return self.row.client_id == client.get_client_id()

    def get_scope(self) -> str:
        """The scope the token grants.

        Returns:
            The empty scope.
        """
        return ""


# -- Grants --------------------------------------------------------------------


class S256CodeChallenge(CodeChallenge):
    """PKCE on every authorization request, with ``S256`` only."""

    def validate_code_challenge(self, grant: AuthorizationCodeGrant, redirect_uri: str) -> None:
        """Require a challenge and an explicit ``S256`` method, then Authlib's checks.

        Args:
            grant: The grant validating the authorization request.
            redirect_uri: The validated redirect URI.

        Raises:
            InvalidRequestError: When the challenge is missing or the method
                is not ``S256``; it redirects to the client.
        """
        data = grant.request.payload.data
        if not data.get("code_challenge"):
            raise InvalidRequestError("Missing 'code_challenge'")
        if data.get("code_challenge_method") != "S256":
            raise InvalidRequestError("'code_challenge_method' must be S256")
        super().validate_code_challenge(grant, redirect_uri)


class StoreGrant(grants.BaseGrant):
    """What both grants share: the store issues the token pair, in the grant's family.

    Attributes:
        request: The request the grant handles.
        server: The server the grant belongs to.
    """

    TOKEN_ENDPOINT_AUTH_METHODS = CLIENT_AUTH_METHODS

    request: OAuthRequest
    server: AuthorizationServer

    def generate_token(
        self,
        user: Profile | None = None,
        scope: str | None = None,
        grant_type: str | None = None,
        expires_in: int | None = None,
        include_refresh_token: bool = True,
    ) -> dict[str, Any]:
        """Issue an access token and a refresh token for the grant being exchanged.

        Both carry the grant's user, client and resource, and the refresh
        token joins the grant's family; every grant gets a refresh token.

        Args:
            user: The user the client acts for, read from the grant instead.
            scope: The requested scope, unused.
            grant_type: The grant type, unused.
            expires_in: Authlib's lifetime override, unused.
            include_refresh_token: Authlib's choice, unused.

        Returns:
            The token response body.

        Raises:
            InvalidGrantError: When the grant's family was revoked while the
                tokens were issued.
        """
        source = (self.request.authorization_code or self.request.refresh_token).row
        issued = self.server.store.oauth.issue_tokens(
            source.user_id, source.client_id, Audience(source.resource), source.family_id
        )
        if issued is None:
            raise InvalidGrantError("The grant was revoked.")
        access_token, refresh_token = issued
        return {
            "token_type": "Bearer",
            "access_token": access_token,
            "expires_in": self.server.access_token_expires_in,
            "refresh_token": refresh_token,
        }


class AuthorizationCodeGrant(StoreGrant, grants.AuthorizationCodeGrant):
    """The authorization code grant, bound to client, redirect URI, PKCE verifier and resource."""

    @property
    def audience(self) -> Audience:
        """The surface the authorization request names.

        Returns:
            The audience of its ``resource``.
        """
        return self.server.resolve_resource(self.request.payload.datalist.get("resource", []))

    def validate_authorization_request(self) -> str:
        """Validate the request as Authlib does, then its ``resource``.

        Returns:
            The validated redirect URI.
        """
        redirect_uri = super().validate_authorization_request()
        self.server.resolve_resource(self.request.payload.datalist.get("resource", []), redirect_uri=redirect_uri)
        return redirect_uri

    def generate_authorization_code(self) -> str:
        """Issue the code the approving user's consent produces.

        Returns:
            The raw code, valid ten minutes.
        """
        payload = self.request.payload
        return self.server.store.oauth.issue_code(
            client_id=self.request.client.get_client_id(),
            user_id=self.request.user.id,
            redirect_uri=payload.redirect_uri,
            code_challenge=payload.data["code_challenge"],
            resource=self.audience,
        )

    def save_authorization_code(self, code: str, request: OAuth2Request) -> None:
        """Keep nothing: the store saved the code when it issued it.

        Args:
            code: The raw code.
            request: The authorization request.
        """

    def query_authorization_code(self, code: str, client: Client) -> Code | None:
        """Spend a code presented by a client.

        The code is spent whatever follows, so a code presented by the wrong
        client or failing its checks cannot be tried again.

        Args:
            code: The raw code.
            client: The authenticated client.

        Returns:
            The code, or ``None`` when it is unknown, expired, already spent
            or another client's.
        """
        row = self.server.store.oauth.consume_code(code)
        if row is None or row.client_id != client.get_client_id():
            return None
        return Code(row)

    def delete_authorization_code(self, authorization_code: Code) -> None:
        """Keep nothing: consuming the code spent it.

        Args:
            authorization_code: The spent code.
        """

    def authenticate_user(self, authorization_code: Code) -> Profile | None:
        """Read the user who approved the code.

        Args:
            authorization_code: The spent code.

        Returns:
            The user, or ``None`` when their profile is gone.
        """
        return self.server.user(authorization_code.row.user_id)

    def validate_token_request(self) -> None:
        """Validate the exchange as Authlib does, then that it names the code's resource, if any.

        Raises:
            InvalidTargetError: When the exchange names another resource.
        """
        super().validate_token_request()
        requested = self.request.payload.datalist.get("resource")
        if requested and self.server.resolve_resource(requested).value != self.request.authorization_code.row.resource:
            raise InvalidTargetError("'resource' differs from the authorization request.")


class RefreshTokenGrant(StoreGrant, grants.RefreshTokenGrant):
    """The refresh token grant, rotating the token on every use."""

    INCLUDE_NEW_REFRESH_TOKEN = True

    def authenticate_refresh_token(self, refresh_token: str) -> Credential | None:
        """Rotate a refresh token presented by its client.

        A ``resource``, when the request names one, must be the token's, and
        is checked before the rotation so a mismatch leaves the token usable.

        Args:
            refresh_token: The raw refresh token.

        Returns:
            The rotated token, or ``None`` when it cannot be used. Presenting
            one already rotated revokes its family.

        Raises:
            InvalidTargetError: When the request names another resource.
        """
        store = self.server.store
        requested = self.request.payload.datalist.get("resource")
        if requested:
            audience = self.server.resolve_resource(requested)
            presented = store.oauth.find_refresh(refresh_token)
            if presented is not None and presented.resource != audience.value:
                raise InvalidTargetError("'resource' differs from the original grant.")
        row = store.oauth.rotate_refresh(refresh_token, self.request.client.get_client_id())
        return Credential(row) if row is not None else None

    def _validate_token_scope(self, token: Credential) -> None:
        """Accept any requested scope: grants carry none, and a refresh never widens one.

        Args:
            token: The rotated refresh token.
        """

    def authenticate_user(self, refresh_token: Credential) -> Profile | None:
        """Read the user the refresh token acts for.

        Args:
            refresh_token: The rotated refresh token.

        Returns:
            The user, or ``None`` when their profile is gone.
        """
        return self.server.user(refresh_token.row.user_id)

    def revoke_old_credential(self, refresh_token: Credential) -> None:
        """Keep nothing: rotating the token retired it.

        Args:
            refresh_token: The rotated refresh token.
        """


# -- Endpoints -----------------------------------------------------------------


class Revocation(RevocationEndpoint):
    """Token revocation (RFC 7009): a refresh token ends its family, an access token itself."""

    CLIENT_AUTH_METHODS = CLIENT_AUTH_METHODS

    server: AuthorizationServer

    def query_token(self, token_string: str, token_type_hint: str | None) -> Credential | None:
        """Find an access or refresh token by its raw value.

        Args:
            token_string: The token as presented.
            token_type_hint: The client's hint; both kinds are looked up anyway.

        Returns:
            The token, or ``None``; a session or personal token is never found.
        """
        store = self.server.store
        row = store.tokens.find_access(token_string) or store.oauth.find_refresh(token_string)
        return Credential(row) if row is not None else None

    def revoke_token(self, token: Credential, request: OAuthRequest) -> None:
        """Revoke a token the client holds.

        Args:
            token: The token, the client's own.
            request: The revocation request.
        """
        store = self.server.store
        if isinstance(token.row, RefreshToken):
            store.oauth.revoke_family(token.row)
        else:
            store.tokens.revoke(token.row.id)


class Registration(ClientRegistrationEndpoint):
    """Open dynamic client registration (RFC 7591).

    The store generates the client id and, for a confidential client, its
    secret; the response describes the client as registered, which may
    differ from what was asked (both grants, ``code`` only).

    Attributes:
        server: The server the endpoint belongs to.
    """

    server: AuthorizationServer

    def authenticate_token(self, request: RegistrationRequest) -> bool:
        """Let anyone register: a client is unverified until a user approves it.

        Args:
            request: The registration request.

        Returns:
            ``True``.
        """
        return True

    def get_server_metadata(self) -> dict[str, Any]:
        """Validate client metadata against what the server supports.

        Returns:
            The server metadata.
        """
        return self.server.metadata

    def create_registration_response(self, request: RegistrationRequest) -> tuple[int, dict[str, Any], list]:
        """Register a client from validated metadata.

        Args:
            request: The registration request.

        Returns:
            ``(201, body, headers)``; the body carries the client's secret
            once, for a confidential client only, and its name as stored, cut
            to :data:`CLIENT_NAME_MAX_LENGTH`.

        Raises:
            InvalidRedirectURIError: When no redirect URI is registered, or
                one is unsafe.
        """
        metadata = self.extract_client_metadata(request)
        redirect_uris = metadata.get("redirect_uris")
        if not redirect_uris:
            raise InvalidRedirectURIError("'redirect_uris' is required.")
        if not all(Client.is_safe_redirect_uri(uri) for uri in redirect_uris):
            raise InvalidRedirectURIError(
                "A redirect URI must use https, http on a loopback host, or the private-use scheme of a known "
                f"client ({', '.join(sorted(PRIVATE_USE_SCHEMES))})."
            )
        auth_method = metadata.get("token_endpoint_auth_method", "client_secret_basic")
        client, secret = self.server.store.oauth.register_client(
            (metadata.get("client_name") or "Unnamed application")[:CLIENT_NAME_MAX_LENGTH],
            redirect_uris,
            registered_by="dynamic",
            confidential=auth_method != "none",
        )
        issued_at = client.created_at.timestamp() if client.created_at else time.time()
        body: dict[str, Any] = {
            "client_id": client.client_id,
            "client_id_issued_at": int(issued_at),
            "client_name": client.name,
            "redirect_uris": client.redirect_uris,
            "grant_types": GRANT_TYPES,
            "response_types": ["code"],
            "token_endpoint_auth_method": auth_method,
        }
        if secret is not None:
            body |= {"client_secret": secret, "client_secret_expires_at": 0}
        return 201, body, default_json_headers


# -- Server --------------------------------------------------------------------


class AuthorizationServer(rfc6749.AuthorizationServer):
    """interloper's authorization server: the Starlette adapter over the store.

    Attributes:
        store: The store every hook reads and writes through.
        issuer: The server's identifier and the base of its endpoints.
        resources: The RFC 8707 resource URLs and the surface each names.
        access_token_expires_in: An access token's lifetime, in seconds.
        metadata: The RFC 8414 metadata document.
    """

    def __init__(
        self,
        store: Store,
        *,
        issuer: str,
        mcp_url: str,
        encryption_key: str,
        access_token_expiry: timedelta,
    ) -> None:
        """Build the server with its grants and endpoints.

        Args:
            store: The store every hook reads and writes through.
            issuer: The public base URL of the app, the server's identifier.
            mcp_url: The MCP endpoint's public URL, the resource naming it.
            encryption_key: The instance's encryption secret, which seals the
                authorization request while the user decides.
            access_token_expiry: How long an access token stays valid.
        """
        super().__init__()
        self.store = store
        self.issuer = issuer.rstrip("/")
        self.resources = {f"{self.issuer}/api": Audience.API, mcp_url.rstrip("/"): Audience.MCP}
        self.access_token_expires_in = int(access_token_expiry.total_seconds())
        self._fernet = Fernet(derive_fernet_key(encryption_key))
        self.metadata = self._build_metadata()
        self.register_grant(AuthorizationCodeGrant, [S256CodeChallenge(required=True)])
        self.register_grant(RefreshTokenGrant)
        self.register_endpoint(Revocation)
        # Authlib's annotation leaves out its own RFC 7591 endpoint, which is no Endpoint subclass.
        self.register_endpoint(Registration)  # ty: ignore[invalid-argument-type]

    @classmethod
    def from_settings(cls, store: Store, settings: Any) -> AuthorizationServer:
        """Build the server from the application settings.

        The issuer is the app's public URL (``server.public_url``); the MCP
        resource is the one the MCP server advertises (``mcp.public_url``).

        Args:
            store: The store every hook reads and writes through.
            settings: The full ``AppSettings``.

        Returns:
            The server.
        """
        issuer = settings.server.public_url
        return cls(
            store,
            issuer=issuer,
            mcp_url=settings.mcp.public_url(settings.server),
            encryption_key=settings.secrets.encryption_key,
            access_token_expiry=timedelta(minutes=settings.auth.access_token_expiry_minutes),
        )

    # -- Framework -------------------------------------------------------------

    def create_oauth2_request(self, request: OAuthRequest) -> OAuthRequest:
        """Take the snapshot the route built.

        Args:
            request: The request snapshot.

        Returns:
            The same snapshot.
        """
        return request

    def create_json_request(self, request: RegistrationRequest) -> RegistrationRequest:
        """Take the snapshot the route built.

        Args:
            request: The registration request snapshot.

        Returns:
            The same snapshot.
        """
        return request

    def handle_response(self, status: int, body: Any, headers: Any) -> Response:
        """Build the response, adding the issuer to a redirect to the client (RFC 9207).

        Only the authorization endpoint redirects, so every redirect is an
        authorization response, a code or an error.

        Args:
            status: The HTTP status.
            body: A dict sent as JSON, or a string.
            headers: The headers, as pairs.

        Returns:
            The Starlette response.
        """
        headers = dict(headers)
        if "Location" in headers:
            headers["Location"] = add_params_to_uri(headers["Location"], [("iss", self.issuer)])
        content = json.dumps(body) if isinstance(body, dict) else body
        return Response(content, status_code=status, headers=headers)

    def send_signal(self, name: str, *args: Any, **kwargs: Any) -> None:
        """Ignore Authlib's signals; nothing listens.

        Args:
            name: The signal.
            *args: Its arguments.
            **kwargs: Its keyword arguments.
        """

    # -- Storage hooks ---------------------------------------------------------

    def query_client(self, client_id: str) -> Client | None:
        """Find a registered client.

        Args:
            client_id: The client id a request presents.

        Returns:
            The client, or ``None`` when none has that id.
        """
        row = self.store.oauth.get_client(client_id)
        return Client(row) if row is not None else None

    def save_token(self, token: dict[str, Any], request: OAuth2Request) -> None:
        """Keep nothing: the store saved the tokens when it issued them.

        Args:
            token: The token response body.
            request: The token request.
        """

    def user(self, user_id: Any) -> Profile | None:
        """Read the user a grant acts for.

        Args:
            user_id: The profile id.

        Returns:
            The profile, or ``None`` when it is gone.
        """
        with suppress(NotFoundError):
            return self.store.profiles.get(user_id)
        return None

    # -- Resources & requests --------------------------------------------------

    def resolve_resource(self, values: list[str], *, redirect_uri: str | None = None) -> Audience:
        """Map a request's RFC 8707 ``resource`` to the surface it names.

        Args:
            values: Every ``resource`` value the request carries.
            redirect_uri: Where the error is sent, once the redirect URI is
                validated; ``None`` answers it directly.

        Returns:
            The surface.

        Raises:
            InvalidTargetError: When there is not exactly one value, or it
                is not a known resource.
        """
        if len(values) != 1 or values[0].rstrip("/") not in self.resources:
            raise InvalidTargetError(
                f"Exactly one 'resource' is required, one of: {', '.join(self.resources)}.", redirect_uri=redirect_uri
            )
        return self.resources[values[0].rstrip("/")]

    def uri(self, path: str) -> str:
        """The URL Authlib sees for a request to one of the server's paths.

        Args:
            path: The request's path.

        Returns:
            The path on the issuer.
        """
        return f"{self.issuer}{path}"

    def authorization_request(self, query: list[tuple[str, str]]) -> OAuthRequest:
        """Snapshot an authorization request from its parameters.

        Args:
            query: The request's query parameters, in order.

        Returns:
            The request, as the authorization endpoint receives it.
        """
        uri = f"{self.uri(AUTHORIZE_PATH)}?{urlencode(query)}" if query else self.uri(AUTHORIZE_PATH)
        return OAuthRequest("GET", uri, {}, query=query)

    def seal(self, request: OAuthRequest, user_id: UUID) -> str:
        """Seal a validated authorization request for the consent page to carry.

        Args:
            request: The authorization request.
            user_id: The user whose session started it, the only one who may
                decide on it.

        Returns:
            An opaque, url-safe value, encrypted and authenticated.
        """
        sealed = {"user_id": str(user_id), "query": request.query}
        return self._fernet.encrypt(json.dumps(sealed).encode()).decode()

    def unseal(self, value: str, user_id: UUID) -> OAuthRequest | None:
        """Open a sealed authorization request for the user deciding on it, within its ten minutes.

        Args:
            value: The value :meth:`seal` returned.
            user_id: The user whose session presents it.

        Returns:
            The authorization request, or ``None`` when the value is expired,
            altered, was never sealed here, or was sealed for another user.
        """
        try:
            sealed = json.loads(self._fernet.decrypt(value.encode(), ttl=int(CONSENT_REQUEST_TTL.total_seconds())))
        except (InvalidToken, ValueError):
            return None
        if sealed["user_id"] != str(user_id):
            return None
        return self.authorization_request([(name, parameter) for name, parameter in sealed["query"]])

    # -- Internals -------------------------------------------------------------

    def _build_metadata(self) -> dict[str, Any]:
        """Describe the server (RFC 8414).

        Returns:
            The validated metadata document.
        """
        metadata = AuthorizationServerMetadata(
            issuer=self.issuer,
            authorization_endpoint=self.uri(AUTHORIZE_PATH),
            token_endpoint=self.uri(TOKEN_PATH),
            registration_endpoint=self.uri(REGISTRATION_PATH),
            revocation_endpoint=self.uri(REVOCATION_PATH),
            response_types_supported=["code"],
            grant_types_supported=GRANT_TYPES,
            code_challenge_methods_supported=["S256"],
            token_endpoint_auth_methods_supported=CLIENT_AUTH_METHODS,
            revocation_endpoint_auth_methods_supported=CLIENT_AUTH_METHODS,
            authorization_response_iss_parameter_supported=True,
        )
        metadata.validate()
        return dict(metadata)


# -- Cross-origin access -------------------------------------------------------


class CrossOriginMiddleware:
    """Let any origin call the server's client-facing endpoints, ahead of the app's own CORS policy.

    A browser-based client (MCP Inspector) fetches the metadata, registers,
    exchanges and revokes tokens from its own origin. Those endpoints read no
    cookie, so any origin may call them, without credentials; every other
    path keeps the app's policy.
    """

    def __init__(self, app: ASGIApp) -> None:
        """Wrap the application.

        Args:
            app: The application, with its own CORS policy if any.
        """
        self.app = app
        self._cors = CORSMiddleware(
            app, allow_origins=["*"], allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"]
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Answer cross-origin on :data:`CROSS_ORIGIN_PATHS`, hand everything else on untouched.

        Args:
            scope: The ASGI connection scope.
            receive: The ASGI receive callable.
            send: The ASGI send callable.
        """
        open_to_any_origin = scope["type"] == "http" and scope["path"] in CROSS_ORIGIN_PATHS
        await (self._cors if open_to_any_origin else self.app)(scope, receive, send)
