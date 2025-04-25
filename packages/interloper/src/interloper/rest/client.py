import logging
from collections.abc import Generator
from typing import Any

import httpx

from interloper.rest.auth import Auth
from interloper.rest.paginator import Paginator

logger = logging.getLogger(__name__)


class RESTClient:
    def __init__(
        self,
        base_url: str,
        auth: Auth | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        paginator: Paginator | None = None,
    ):
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
        if self._auth is None:
            raise ValueError("RESTClient has no authentication configured")
        return self._auth

    @property
    def authenticated(self) -> bool:
        return self._auth.authenticated if self._auth else False

    @property
    def client(self) -> httpx.Client:
        if self._auth and not self._auth.authenticated:
            logger.debug("Client hasn't been authenticated yet. Authenticating...")
            self.authenticate()

        return self._client

    def authenticate(self) -> None:
        self.auth(self._client)

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
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
        return self.client.post(
            url,
            data=data,
            json=json,
            params=params,
            headers=headers,
            **kwargs,
        )

    def paginate(self, path: str) -> Generator[Any]:
        if self._paginator is None:
            raise ValueError("RESTClient has no paginator configured")

        yield from self._paginator.paginate(self.client, path)
