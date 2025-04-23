import base64
import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class Auth(ABC):
    def __init__(self):
        self.authenticated = False

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any): ...


class HTTPAuth(Auth):
    @abstractmethod
    def __call__(self, client: httpx.Client): ...


class HTTPBasicAuth(HTTPAuth):
    def __init__(self, username: str, password: str):
        super().__init__()
        self._username = username
        self._password = password

    def __call__(self, client: httpx.Client):
        credentials = base64.b64encode(f"{self._username}:{self._password}".encode()).decode()
        client.headers.update({"Authorization": f"Basic {credentials}"})
        self.authenticated = True


class HTTPBearerAuth(HTTPAuth):
    def __init__(self, token: str):
        super().__init__()
        self._token = token

    def __call__(self, client: httpx.Client):
        client.headers.update({"Authorization": f"Bearer {self._token}"})
        self.authenticated = True


class OAuth2Auth(HTTPAuth):
    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        refresh_token: str | None = None,
        scope: str | None = None,
        token_endpoint: str = "/oauth2/token",
    ):
        super().__init__()
        self._base_url = base_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._scope = scope
        self._token_endpoint = token_endpoint if token_endpoint.startswith("/") else f"/{token_endpoint}"

    @property
    @abstractmethod
    def grant_type(self) -> str: ...

    @property
    @abstractmethod
    def auth_data(self) -> dict[str, str]: ...

    @property
    def auth_headers(self) -> dict[str, str] | None:
        return None

    @property
    def access_token(self) -> str:
        return self._access_token

    @property
    def refresh_token(self) -> str | None:
        return self._refresh_token

    def __call__(self, client: httpx.Client):
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
    @property
    def grant_type(self) -> str:
        return "client_credentials"

    @property
    def auth_data(self) -> dict[str, str]:
        data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": self.grant_type,
        }

        if self._scope is not None:
            data.update({"scope": self._scope})

        return data


class OAuth2RefreshTokenAuth(OAuth2ClientCredentialsAuth):
    @property
    def grant_type(self) -> str:
        return "refresh_token"

    @property
    def auth_data(self) -> dict[str, str]:
        if self._refresh_token is None:
            raise ValueError("Refresh token is required when using `OAuth2RefreshTokenConnector`")

        auth_data = super().auth_data
        auth_data.update({"refresh_token": self._refresh_token})
        return auth_data
