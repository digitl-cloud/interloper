"""Databricks connection resource holding service principal or personal access token credentials."""

from __future__ import annotations

import time
from collections.abc import Callable, Generator
from functools import cached_property
from typing import Any
from urllib.parse import urlsplit

import httpx2
from databricks import sql
from databricks.sql.client import Connection as Session
from interloper.connection import Connection, connection
from interloper.resource.fields import InputField, SecretField, fetch_field_provider
from interloper.rest import JSONCursorPaginator, RESTClient
from pydantic import PrivateAttr, field_validator, model_validator
from pydantic_settings import SettingsConfigDict

_TOKEN_ENDPOINT = "/oidc/v1/token"

#: A token this close to expiry is replaced before it is handed out, so a
#: statement never starts on a token that lapses mid-flight.
_REFRESH_MARGIN = 300.0

#: Every REST call here serves an operator waiting on a form or a check.
_TIMEOUT = 30.0


class _TokenAuth(httpx2.Auth):
    """Bearer authentication reading the token from a callable on every request."""

    def __init__(self, token: Callable[[], str]) -> None:
        """Bind the auth to its token source.

        Args:
            token: Returns a valid access token, refreshing it when due.
        """
        self._token = token

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response, None]:
        """Attach the current token as a bearer header.

        Args:
            request: The request to authenticate.

        Yields:
            The authenticated request.
        """
        request.headers["Authorization"] = f"Bearer {self._token()}"
        yield request


@connection(
    key="databricks_connection",
    name="Databricks",
    icon="icon:databricks",
    tags=["Cloud"],
    maturity="alpha",
)
class DatabricksConnection(Connection):
    """Connection resource holding Databricks workspace credentials.

    Authenticates either as a service principal through OAuth
    machine-to-machine (``client_id`` and ``client_secret``, the recommended
    path) or with a personal access token; exactly one of the two must be
    set. The connection's own check and pickers call the workspace REST API
    over plain HTTP; each destination opens its own SQL session through
    :meth:`connect`.
    """

    model_config = SettingsConfigDict(env_prefix="databricks_")

    host: str = InputField(
        label="Workspace URL",
        description="e.g. https://dbc-a1b2345c-d6e7.cloud.databricks.com",
    )
    client_id: str | None = InputField(
        default=None,
        label="Client ID",
        description="Service principal application ID",
        section="Service principal (recommended)",
    )
    client_secret: str | None = SecretField(
        default=None,
        label="Client secret",
        description="Service principal OAuth secret",
        section="Service principal (recommended)",
    )
    access_token: str | None = SecretField(
        default=None,
        label="Access token",
        description="Personal access token, instead of a service principal",
        section="Personal access token",
    )

    _token: tuple[str, float] | None = PrivateAttr(default=None)

    @field_validator("host")
    @classmethod
    def normalize_host(cls, value: str) -> str:
        """Give the workspace URL a scheme and drop any trailing slash.

        Args:
            value: The workspace URL or bare hostname.

        Returns:
            The URL as ``https://<hostname>``.
        """
        value = value.strip().rstrip("/")
        return value if "://" in value else f"https://{value}"

    @field_validator("client_id", "client_secret", "access_token", mode="before")
    @classmethod
    def blank_is_unset(cls, value: Any) -> Any:
        """Treat an empty credential, as a form submits it, as unset.

        Args:
            value: The raw field value.

        Returns:
            ``None`` for an empty string, the value otherwise.
        """
        return None if value == "" else value

    @model_validator(mode="after")
    def one_credential(self) -> DatabricksConnection:
        """Require exactly one credential: a complete service principal or an access token.

        Returns:
            The validated connection.

        Raises:
            ValueError: If the service principal is half set, or if both or
                neither credentials are set.
        """
        principal = self.client_id is not None or self.client_secret is not None
        if principal and (self.client_id is None or self.client_secret is None):
            raise ValueError("A service principal needs both 'client_id' and 'client_secret'")
        if principal and self.access_token is not None:
            raise ValueError("Set either a service principal or an access token, not both")
        if not principal and self.access_token is None:
            raise ValueError("Set a service principal ('client_id' and 'client_secret') or an 'access_token'")
        return self

    @property
    def hostname(self) -> str:
        """The workspace hostname, as the SQL connector takes it.

        Returns:
            The host without its scheme.
        """
        return urlsplit(self.host).netloc

    # -- Credentials ---------------------------------------------------------------

    def token(self) -> str:
        """Return a bearer token for the workspace.

        A personal access token is returned as is. A service principal's
        token comes from the workspace's OAuth token endpoint (client
        credentials, ``all-apis`` scope) and is cached until close to its
        expiry; two threads refreshing at once only cost a second exchange.

        Returns:
            The access token.
        """
        if self.access_token is not None:
            return self.access_token
        cached = self._token
        if cached is not None and cached[1] - time.monotonic() > _REFRESH_MARGIN:
            return cached[0]
        assert self.client_id is not None and self.client_secret is not None
        with RESTClient(self.host, timeout=_TIMEOUT) as oauth:
            response = oauth.post(
                _TOKEN_ENDPOINT,
                data={"grant_type": "client_credentials", "scope": "all-apis"},
                auth=httpx2.BasicAuth(self.client_id, self.client_secret),
            )
        response.raise_for_status()
        body = response.json()
        self._token = (body["access_token"], time.monotonic() + float(body.get("expires_in", 3600)))
        return body["access_token"]

    def _authorization(self) -> dict[str, str]:
        """Build the header the SQL connector sends with each request.

        Returns:
            The ``Authorization`` header carrying a current token.
        """
        return {"Authorization": f"Bearer {self.token()}"}

    def _credentials_provider(self) -> Callable[[], dict[str, str]]:
        """Hand the SQL connector its header factory.

        The connector calls the provider once per session and the factory it
        returns whenever it needs headers, so the factory refreshes the
        service principal's token as it ages.

        Returns:
            The header factory.
        """
        return self._authorization

    def connect(self, **session: Any) -> Session:
        """Open a new SQL session on the workspace with this connection's credentials.

        A personal access token goes to the connector as is; a service
        principal goes through a ``credentials_provider`` built on this
        connection's own token exchange, which spares the Databricks SDK as
        a dependency.

        Args:
            **session: Session settings passed to the connector, such as
                ``http_path`` and ``catalog``.

        Returns:
            The new connector session.
        """
        if self.access_token is not None:
            credentials: dict[str, Any] = {"access_token": self.access_token}
        else:
            credentials = {"credentials_provider": self._credentials_provider}
        return sql.connect(server_hostname=self.hostname, **credentials, **session)

    # -- REST API ------------------------------------------------------------------

    @cached_property
    def client(self) -> RESTClient:
        """The workspace REST API client the check and the pickers share.

        Plain HTTP rather than the SQL connector: both run in the API
        process, where opening a warehouse session would be slow and could
        start a stopped warehouse.

        Returns:
            The bearer-authenticated client, cached per connection instance.
        """
        return RESTClient(self.host, auth=_TokenAuth(self.token), timeout=_TIMEOUT)

    def _list(self, path: str, key: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        """Walk a paginated list endpoint and collect its items.

        Args:
            path: The endpoint path.
            key: The response key holding each page's items.
            params: Static query parameters for every page.

        Returns:
            Every item across the pages.
        """
        pages = self.client.paginate(
            path,
            JSONCursorPaginator(cursor_path="next_page_token", cursor_param="page_token"),
            params=params,
            data_selector=lambda response: response.json().get(key, []),
        )
        return [item for page in pages for item in page]

    @fetch_field_provider
    def warehouses(self) -> list[dict[str, str]]:
        """List the SQL warehouses this principal can use.

        Backs the destination's ``warehouse`` ``FetchField``. The stored value
        is the warehouse's HTTP path, which is what the SQL connector takes.

        Returns:
            Warehouse options with ``path`` and ``name``, sorted case-insensitively by name.
        """
        warehouses = self._list("/api/2.0/sql/warehouses", "warehouses")
        options = [{"path": w["odbc_params"]["path"], "name": w["name"]} for w in warehouses]
        return sorted(options, key=lambda option: option["name"].lower())

    @fetch_field_provider
    def catalogs(self) -> list[dict[str, str]]:
        """List the Unity Catalog catalogs this principal can see.

        Backs the destination's ``catalog`` ``FetchField``. ``max_results=0``
        asks for the paginated form, which Databricks recommends over the
        unpaginated one.

        Returns:
            Catalog options with ``name``, sorted case-insensitively.
        """
        catalogs = self._list("/api/2.1/unity-catalog/catalogs", "catalogs", params={"max_results": "0"})
        return sorted(({"name": c["name"]} for c in catalogs), key=lambda option: option["name"].lower())

    def check(self) -> bool:
        """Prove the credentials work by asking the workspace who the caller is.

        ``/api/2.0/preview/scim/v2/Me`` needs nothing beyond a valid token,
        so a failure isolates a bad credential from a missing grant; for a
        service principal it also exercises the token exchange.

        Returns:
            True; a rejected credential raises out of the HTTP call.
        """
        self.client.get("/api/2.0/preview/scim/v2/Me").raise_for_status()
        return True
