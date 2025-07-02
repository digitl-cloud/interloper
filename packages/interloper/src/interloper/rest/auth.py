"""This module contains the authentication classes for the REST client."""
import base64
import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class Auth(ABC):
    """An abstract class for authentication."""

    def __init__(self):
        """Initialize the authentication."""
        self.authenticated = False

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any):
        """Authenticate the client."""
        ...


class HTTPAuth(Auth):
    """An abstract class for HTTP authentication."""

    @abstractmethod
    def __call__(self, client: httpx.Client):
        """Authenticate the client."""
        ...


class HTTPBasicAuth(HTTPAuth):
    """HTTP Basic authentication."""

    def __init__(self, username: str, password: str):
        """Initialize the HTTP Basic authentication.

        Args:
            username: The username.
            password: The password.
        """
        super().__init__()
        self._username = username
        self._password = password

    def __call__(self, client: httpx.Client):
        """Authenticate the client."""
        credentials = base64.b64encode(f"{self._username}:{self._password}".encode()).decode()
        client.headers.update({"Authorization": f"Basic {credentials}"})
        self.authenticated = True


class HTTPBearerAuth(HTTPAuth):
    """HTTP Bearer authentication."""

    def __init__(self, token: str):
        """Initialize the HTTP Bearer authentication.

        Args:
            token: The token.
        """
        super().__init__()
        self._token = token

    def __call__(self, client: httpx.Client):
        """Authenticate the client."""
        client.headers.update({"Authorization": f"Bearer {self._token}"})
        self.authenticated = True


class OAuth2Auth(HTTPAuth):
    """OAuth2 authentication."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str | None = None,
        scope: str | None = None,
        token_endpoint: str = "/oauth2/token",
    ):
        """Initialize the OAuth2 authentication.

        Args:
            base_url: The base URL of the API.
            client_id: The client ID.
            client_secret: The client secret.
            refresh_token: The refresh token.
            scope: The scope.
            token_endpoint: The token endpoint.
        """
        super().__init__()
        self._base_url = base_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._scope = scope
        self._token_endpoint = token_endpoint if token_endpoint.startswith("/") else f"/{token_endpoint}"

    @property
    @abstractmethod
    def grant_type(self) -> str:
        """The grant type."""
        ...

    @property
    @abstractmethod
    def auth_data(self) -> dict[str, str]:
        """The authentication data."""
        ...

    @property
    def auth_headers(self) -> dict[str, str] | None:
        """The authentication headers."""
        return None

    @property
    def access_token(self) -> str:
        """The access token.

        Raises:
            ValueError: If the client is not authenticated.
        """
        if not self.authenticated:
            raise ValueError("Cannot access access token: not authenticated")
        return self._access_token

    @property
    def refresh_token(self) -> str | None:
        """The refresh token.

        Raises:
            ValueError: If the client is not authenticated.
        """
        if not self.authenticated:
            raise ValueError("Cannot access refresh token: not authenticated")
        return self._refresh_token

    def __call__(self, client: httpx.Client):
        """Authenticate the client."""
        logger.info("Authenticating API...")

        response = httpx.post(
            f"{self._base_url}{self._token_endpoint}",
            data=self.auth_data,
            headers=self.auth_headers,
            timeout=None,
        )
        response.raise_for_status()

        self._access_token = response.json()["access_token"]
        self._refresh_token = response.json().get("refresh_token")

        client.headers.update({"Authorization": f"Bearer {self._access_token}"})
        self.authenticated = True

        logger.info("API authenticated")


class OAuth2ClientCredentialsAuth(OAuth2Auth):
    """OAuth2 client credentials authentication."""

    @property
    def grant_type(self) -> str:
        """The grant type."""
        return "client_credentials"

    @property
    def auth_data(self) -> dict[str, str]:
        """The authentication data."""
        data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": self.grant_type,
        }

        if self._scope is not None:
            data.update({"scope": self._scope})

        return data


class OAuth2RefreshTokenAuth(OAuth2ClientCredentialsAuth):
    """OAuth2 refresh token authentication."""

    @property
    def grant_type(self) -> str:
        """The grant type."""
        return "refresh_token"

    @property
    def auth_data(self) -> dict[str, str]:
        """The authentication data.

        Raises:
            ValueError: If the refresh token is required but not provided.
        """
        if self._refresh_token is None:
            raise ValueError("Refresh token is required when using `OAuth2RefreshTokenConnector`")

        auth_data = super().auth_data
        auth_data.update({"refresh_token": self._refresh_token})
        return auth_data
