"""Microsoft Azure connection resource holding a Microsoft Entra service principal."""

from __future__ import annotations

from collections.abc import Generator
from functools import cached_property
from typing import Any

import httpx2
from azure.identity import ClientSecretCredential
from interloper.connection import Connection, connection
from interloper.resource.fields import InputField, SecretField, fetch_field_provider
from interloper.rest import JSONCursorPaginator, RESTClient
from pydantic_settings import SettingsConfigDict

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"

# Every REST call here serves an operator waiting on a form or a check.
_TIMEOUT = 30.0


class _CredentialAuth(httpx2.Auth):
    """Bearer auth drawing the token from the connection on every request.

    The credential caches the token and renews it before it expires, so a
    long-lived client never sends a stale one.
    """

    def __init__(self, owner: AzureConnection, scope: str) -> None:
        """Bind the auth to the connection whose credential signs the requests.

        Args:
            owner: The connection holding the credential.
            scope: The scope the token is requested for.
        """
        self._owner = owner
        self._scope = scope

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response, None]:
        """Authenticate the request with a bearer token for the scope.

        Args:
            request: The request to authenticate.

        Yields:
            The authenticated request.
        """
        request.headers["Authorization"] = f"Bearer {self._owner.token(self._scope)}"
        yield request


@connection(
    key="azure_connection",
    name="Microsoft Azure",
    icon="icon:azure",
    tags=["Cloud"],
    maturity="alpha",
)
class AzureConnection(Connection):
    """Connection resource holding a Microsoft Entra service principal.

    The principal is an app registration's client ID and secret in a tenant.
    ``azure-identity`` only obtains the tokens; every REST call goes through
    httpx2.
    """

    model_config = SettingsConfigDict(env_prefix="azure_")

    tenant_id: str = InputField(label="Tenant ID", description="Directory (tenant) ID of the Microsoft Entra tenant")
    client_id: str = InputField(label="Client ID", description="Application (client) ID of the service principal")
    client_secret: str = SecretField(
        label="Client secret",
        description="Client secret of the service principal",
        info="From the app registration's Certificates & secrets page: the secret's Value, not its ID.",
    )

    @cached_property
    def credential(self) -> ClientSecretCredential:
        """The credential every token is obtained through.

        Returns:
            The client-secret credential, cached per connection instance so
            its token cache is shared by every caller.
        """
        return ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)

    def token(self, scope: str) -> str:
        """Obtain an access token for a scope.

        Args:
            scope: The scope requested, e.g. ``https://api.fabric.microsoft.com/.default``.

        Returns:
            The bearer token.
        """
        return self.credential.get_token(scope).token

    @cached_property
    def client(self) -> RESTClient:
        """The Fabric REST API client the check and the picker share.

        Returns:
            The client, authenticated for the Fabric API and cached per connection instance.
        """
        return RESTClient(FABRIC_API, auth=_CredentialAuth(self, FABRIC_SCOPE), timeout=_TIMEOUT)

    def _list(self, path: str) -> list[dict[str, Any]]:
        """List every item of a Fabric collection, following continuation tokens.

        Args:
            path: The collection path, relative to the API root.

        Returns:
            The items of every page.
        """
        pages = self.client.paginate(
            path,
            JSONCursorPaginator(cursor_path="continuationToken", cursor_param="continuationToken"),
            data_selector="value",
        )
        return [item for page in pages for item in page]

    @fetch_field_provider
    def workspaces(self) -> list[dict[str, str]]:
        """List the workspaces holding a warehouse, with the SQL endpoint each serves.

        Backs the warehouse destination's ``server`` ``FetchField``. Every
        warehouse of a workspace is served by the workspace's one SQL
        connection string, so the first warehouse listed names it, and a
        workspace without a warehouse has nothing to write to and is left out.

        Returns:
            Workspace options with ``id``, ``name`` and ``server``, sorted
            case-insensitively by name.
        """
        options: list[dict[str, str]] = []
        for workspace in self._list("/workspaces"):
            response = self.client.get(f"/workspaces/{workspace['id']}/warehouses")
            response.raise_for_status()
            warehouses = response.json().get("value", [])
            if not warehouses:
                continue
            options.append(
                {
                    "id": workspace["id"],
                    "name": workspace["displayName"],
                    "server": warehouses[0]["properties"]["connectionString"],
                }
            )
        return sorted(options, key=lambda option: option["name"].lower())

    def check(self) -> bool:
        """Prove the principal can authenticate and reach Fabric by listing one page of workspaces.

        Returns:
            True; a rejected secret raises out of the credential, and a principal
            Fabric refuses (the tenant does not let service principals use
            Fabric APIs) raises an HTTP 401 or 403.
        """
        self.client.get("/workspaces").raise_for_status()
        return True
