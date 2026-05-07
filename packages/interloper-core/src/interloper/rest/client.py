"""REST client extending httpx with pagination support."""

from __future__ import annotations

import logging
from collections.abc import Generator
from typing import Any

import httpx

from interloper.errors import ConfigError
from interloper.rest.paginator import Paginator

logger = logging.getLogger(__name__)


class RESTClient(httpx.Client):
    """A REST client that extends httpx.Client with pagination support."""

    def __init__(
        self,
        base_url: str,
        auth: httpx.Auth | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        paginator: Paginator | None = None,
        **kwargs: Any,
    ):
        """Initialize the REST client.

        Args:
            base_url: The base URL of the API.
            auth: The authentication method (httpx.Auth instance).
            timeout: The timeout for requests.
            headers: The headers to include in requests.
            params: The parameters to include in requests.
            paginator: The paginator to use.
            **kwargs: Additional keyword arguments to pass to httpx.Client.
        """
        super().__init__(
            base_url=base_url,
            auth=auth,
            timeout=timeout,
            headers=headers,
            params=params,
            **kwargs,
        )
        self._paginator = paginator

    def paginate(self, path: str) -> Generator[Any]:
        """Paginate through a resource.

        Args:
            path: The path to the resource.

        Yields:
            The items in the resource.

        Raises:
            ConfigError: If no paginator is configured.
        """
        if self._paginator is None:
            raise ConfigError("RESTClient has no paginator configured")

        yield from self._paginator.paginate(self, path)
