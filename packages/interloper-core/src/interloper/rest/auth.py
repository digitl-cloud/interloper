"""Authentication handlers for the REST client (Bearer, OAuth2)."""

from __future__ import annotations

import logging
from collections.abc import Generator

import httpx

from interloper.errors import AuthenticationError

logger = logging.getLogger(__name__)


class HTTPBearerAuth(httpx.Auth):
    """HTTP Bearer authentication."""

    def __init__(self, token: str):
        """Initialize the HTTP Bearer authentication.

        Args:
            token: The bearer token.
        """
        self._token = token

    def sync_auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        """Authenticate the request with a Bearer token.

        Args:
            request: The request to authenticate.

        Yields:
            The authenticated request.
        """
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request


class OAuth2Auth(httpx.Auth):
    """OAuth2 authentication with automatic token acquisition and 401 refresh.

    Async-native: the token request is *yielded into the active client's flow*
    rather than performed inline, so a single :meth:`auth_flow` drives both
    :class:`~interloper.rest.client.RESTClient` and
    :class:`~interloper.rest.client.AsyncRESTClient` — the token exchange runs
    sync on one and async on the other, with no blocking I/O on the event loop.
    """

    requires_response_body = True

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str | None = None,
        scope: str | None = None,
        token_endpoint: str = "/oauth2/token",
        access_token: str | None = None,
    ):
        """Initialize the OAuth2 authentication.

        Args:
            base_url: The base URL of the API (token endpoint is resolved against it).
            client_id: The client ID.
            client_secret: The client secret.
            refresh_token: The refresh token (optional).
            scope: The scope (optional).
            token_endpoint: The token endpoint path.
            access_token: Pre-existing access token (optional).
        """
        self._base_url = base_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._scope = scope
        self._token_endpoint = token_endpoint if token_endpoint.startswith("/") else f"/{token_endpoint}"
        self._access_token = access_token

    @property
    def grant_type(self) -> str:
        """The grant type."""
        return "client_credentials"

    @property
    def auth_data(self) -> dict[str, str]:
        """The authentication data."""
        data: dict[str, str] = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": self.grant_type,
        }

        if self._scope is not None:
            data["scope"] = self._scope

        return data

    @property
    def auth_headers(self) -> dict[str, str] | None:
        """The authentication headers for token requests."""
        return None

    @property
    def access_token(self) -> str:
        """The access token.

        Raises:
            AuthenticationError: If no access token is available.
        """
        if self._access_token is None:
            raise AuthenticationError("No access token available. Authentication required.")
        return self._access_token

    @property
    def refresh_token(self) -> str | None:
        """The refresh token."""
        return self._refresh_token

    def clear_token(self) -> None:
        """Clear the cached access token (forces re-acquisition on the next request)."""
        self._access_token = None

    def _token_request(self) -> httpx.Request:
        """Build the token-exchange request to be executed by the active client.

        Returns:
            A POST to the token endpoint carrying the grant's ``auth_data``.
        """
        url = self._base_url.rstrip("/") + self._token_endpoint
        return httpx.Request("POST", url, data=self.auth_data, headers=self.auth_headers or {})

    def _store_token(self, response: httpx.Response) -> None:
        """Persist the access/refresh tokens from a token-endpoint response."""
        response.raise_for_status()
        token_data = response.json()
        self._access_token = token_data["access_token"]
        self._refresh_token = token_data.get("refresh_token", self._refresh_token)
        logger.info("OAuth2 access token acquired")

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        """Attach a Bearer token, acquiring/refreshing it via yielded requests.

        Defining ``auth_flow`` (rather than ``sync_auth_flow``) lets httpx drive
        the same generator for sync and async clients: the yielded token request
        is executed by whichever client is active.

        Yields:
            The token request (when needed) and the authenticated request.
        """
        if self._access_token is None:
            self._store_token((yield self._token_request()))

        request.headers["Authorization"] = f"Bearer {self._access_token}"
        response = yield request

        if response.status_code == 401:
            self._store_token((yield self._token_request()))
            request.headers["Authorization"] = f"Bearer {self._access_token}"
            yield request


class OAuth2ClientCredentialsAuth(OAuth2Auth):
    """OAuth2 client credentials authentication."""

    @property
    def grant_type(self) -> str:
        """The grant type."""
        return "client_credentials"


class OAuth2RefreshTokenAuth(OAuth2Auth):
    """OAuth2 refresh token authentication."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        scope: str | None = None,
        token_endpoint: str = "/oauth2/token",
    ):
        """Initialize the OAuth2 refresh token authentication.

        Args:
            base_url: The base URL of the API.
            client_id: The client ID.
            client_secret: The client secret.
            refresh_token: The refresh token (required).
            scope: The scope (optional).
            token_endpoint: The token endpoint path.
        """
        super().__init__(
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            scope=scope,
            token_endpoint=token_endpoint,
        )

    @property
    def grant_type(self) -> str:
        """The grant type."""
        return "refresh_token"

    @property
    def auth_data(self) -> dict[str, str]:
        """The authentication data.

        Raises:
            AuthenticationError: If no refresh token is available.
        """
        if self._refresh_token is None:
            raise AuthenticationError("Refresh token is required")
        auth_data = super().auth_data.copy()
        auth_data["refresh_token"] = self._refresh_token
        return auth_data
