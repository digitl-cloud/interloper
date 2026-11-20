import json
from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.google.auth import ServiceAccountAuth
from interloper_assets.search_console import constants
from interloper_assets.search_console.api import SearchConsoleApi


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
    def client(self) -> il.AsyncRESTClient:
        """The Search Console API client every caller shares.

        Returns:
            The client authenticated as the service account with read-only scopes, cached per connection instance.
        """
        auth = ServiceAccountAuth(json.loads(self.service_account_key), constants.SCOPES)
        return il.AsyncRESTClient(constants.BASE_URL, auth=auth, retry=il.HttpRetryPolicy())

    @cached_property
    def api(self) -> SearchConsoleApi:
        """The Search Console API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return SearchConsoleApi(self.client)

    @il.fetch_field_provider
    async def sites(self) -> list[dict[str, str]]:
        """Fetch the Search Console properties the service account has been added to.

        Returns:
            One option per property: its ``site_url`` and a ``name`` label carrying
            the service account's permission level on it.
        """
        response = await self.api.get("/webmasters/v3/sites")
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
