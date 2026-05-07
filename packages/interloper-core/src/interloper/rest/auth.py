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
    """OAuth2 authentication with automatic token refresh."""

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
            base_url: The base URL of the API.
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
        self._token_client: httpx.Client | None = None

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

    def _get_token_client(self) -> httpx.Client:
        """Get or create a client for token requests.

        Returns:
            An httpx client for making token requests.
        """
        if self._token_client is None:
            self._token_client = httpx.Client(base_url=self._base_url, timeout=None)
        return self._token_client

    def _acquire_token(self) -> None:
        """Acquire a new access token."""
        logger.info("Acquiring OAuth2 access token...")

        token_client = self._get_token_client()
        response = token_client.post(
            self._token_endpoint,
            data=self.auth_data,
            headers=self.auth_headers,
        )
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data["access_token"]
        self._refresh_token = token_data.get("refresh_token", self._refresh_token)

        logger.info("OAuth2 access token acquired")

    def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token.

        Raises:
            AuthenticationError: If no refresh token is available.
        """
        if self._refresh_token is None:
            raise AuthenticationError("Cannot refresh token: no refresh token available")

        logger.info("Refreshing OAuth2 access token...")

        token_client = self._get_token_client()
        refresh_data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        }

        response = token_client.post(
            self._token_endpoint,
            data=refresh_data,
            headers=self.auth_headers,
        )
        response.raise_for_status()

        token_data = response.json()
        self._access_token = token_data["access_token"]
        self._refresh_token = token_data.get("refresh_token", self._refresh_token)

        logger.info("OAuth2 access token refreshed")

    @property
    def auth_headers(self) -> dict[str, str] | None:
        """The authentication headers for token requests."""
        return None

    def clear_token(self) -> None:
        """Clear the cached access token."""
        self._access_token = None

    def sync_auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        """Authenticate the request with OAuth2, handling token acquisition and refresh.

        Args:
            request: The request to authenticate.

        Yields:
            The authenticated request(s).
        """
        # Acquire token if we don't have one
        if self._access_token is None:
            self._acquire_token()

        # Add the access token to the request
        request.headers["Authorization"] = f"Bearer {self._access_token}"

        # Send the request
        response = yield request

        # If we get a 401, try to refresh the token and retry
        if response.status_code == 401:
            try:
                self._refresh_access_token()
                request.headers["Authorization"] = f"Bearer {self._access_token}"
                yield request
            except AuthenticationError:
                # No refresh token available, try to acquire a new token
                self._acquire_token()
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

    def refresh(self) -> None:
        """Refresh the access token using the refresh token."""
        self._refresh_access_token()

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
