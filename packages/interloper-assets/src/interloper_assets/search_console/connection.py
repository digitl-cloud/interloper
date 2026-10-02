import asyncio
import json
from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.search_console import constants


@il.connection(
    name="Search Console",
    icon="devicon:google",
    tags=["SEO"],
    maturity="beta",
)
class SearchConsoleConnection(il.Connection):
    """Google Search Console API connection with service account auth."""

    model_config = SettingsConfigDict(env_prefix="search_console_")

    service_account_key: str = il.JsonField(description="Google service account key JSON")

    @cached_property
    def credentials(self) -> Any:
        """Load the read-only service account credentials, shared across clients.

        Returns:
            The ``google.oauth2`` service account credentials, scoped read-only.
        """
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=constants.SCOPES,
        )

    def client(self) -> Any:
        """Build a fresh Search Console API service client.

        Not cached: the client's ``httplib2`` transport is not thread-safe and
        the assets run concurrently in threads, so each caller builds its own.

        Returns:
            The ``searchconsole`` v1 discovery client over the shared credentials.
        """
        from googleapiclient.discovery import build

        return build(constants.API_SERVICE, constants.API_VERSION, credentials=self.credentials)

    @il.fetch_field_provider
    async def sites(self) -> list[dict[str, str]]:
        """Fetch the Search Console properties the service account has been added to.

        Returns:
            One option per property: its ``site_url`` and a ``name`` label carrying
            the service account's permission level on it.
        """
        response = await asyncio.to_thread(lambda: self.client().sites().list().execute())
        return [
            {"site_url": site["siteUrl"], "name": f"{site['siteUrl']} ({site.get('permissionLevel', 'UNKNOWN')})"}
            for site in response.get("siteEntry", [])
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``sites`` lookup.

        Returns:
            True, since any credential failure raises out of the lookup.
        """
        await self.sites()
        return True
