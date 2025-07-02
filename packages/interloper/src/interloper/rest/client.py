"""This module contains the REST client."""
import logging
from collections.abc import Generator
from typing import Any

import httpx

from interloper.rest.auth import Auth
from interloper.rest.paginator import Paginator

logger = logging.getLogger(__name__)


class RESTClient:
    """A REST client."""

    def __init__(
        self,
        base_url: str,
        auth: Auth | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        paginator: Paginator | None = None,
    ):
        """Initialize the REST client.

        Args:
            base_url: The base URL of the API.
            auth: The authentication method.
            timeout: The timeout for requests.
            headers: The headers to include in requests.
            params: The parameters to include in requests.
            paginator: The paginator to use.
        """
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers=headers,
            params=params,
        )
        self._auth = auth
        self._paginator = paginator

    @property
    def auth(self) -> Auth:
        """The authentication method.

        Raises:
            ValueError: If no authentication method is configured.
        """
        if self._auth is None:
            raise ValueError("RESTClient has no authentication configured")
        return self._auth

    @property
    def authenticated(self) -> bool:
        """Whether the client is authenticated."""
        return self._auth.authenticated if self._auth else False

    @property
    def client(self) -> httpx.Client:
        """The httpx client."""
        if self._auth and not self._auth.authenticated:
            logger.debug("Client hasn't been authenticated yet. Authenticating...")
            self.authenticate()

        return self._client

    def authenticate(self) -> None:
        """Authenticate the client."""
        self.auth(self._client)

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make a GET request.

        Args:
            url: The URL to make the request to.
            params: The parameters to include in the request.
            headers: The headers to include in the request.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            The response from the request.
        """
        return self.client.get(
            url,
            params=params,
            headers=headers,
            **kwargs,
        )

    def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make a POST request.

        Args:
            url: The URL to make the request to.
            data: The data to include in the request.
            json: The JSON data to include in the request.
            params: The parameters to include in the request.
            headers: The headers to include in the request.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            The response from the request.
        """
        return self.client.post(
            url,
            data=data,
            json=json,
            params=params,
            headers=headers,
            **kwargs,
        )

    def paginate(self, path: str) -> Generator[Any]:
        """Paginate through a resource.

        Args:
            path: The path to the resource.

        Yields:
            The items in the resource.

        Raises:
            ValueError: If no paginator is configured.
        """
        if self._paginator is None:
            raise ValueError("RESTClient has no paginator configured")

        yield from self._paginator.paginate(self.client, path)
