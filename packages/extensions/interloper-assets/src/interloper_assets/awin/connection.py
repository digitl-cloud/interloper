from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.awin.api import AwinApi
from interloper_assets.awin.constants import BASE_URL


@il.connection(
    name="Awin",
    icon="icon:awin",
    tags=["Affiliate"],
    maturity="beta",
)
class AwinConnection(il.Connection):
    """Awin affiliate network API connection with bearer token auth."""

    model_config = SettingsConfigDict(env_prefix="awin_")

    access_token: str = il.SecretField(label="Access Token", description="Awin API access token")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Awin API client every caller shares.

        Returns:
            The authenticated client, cached per connection instance.
        """
        return il.AsyncRESTClient(BASE_URL, auth=il.HTTPBearerAuth(self.access_token))

    @cached_property
    def api(self) -> AwinApi:
        """The Awin API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return AwinApi(self.client)

    @il.fetch_field_provider
    async def advertisers(self) -> list[dict[str, str]]:
        """List the advertiser accounts this token can access, sorted by name.

        Backs the source's ``advertiser_id`` ``FetchField``. Goes through ``self.api``,
        over the lightweight ``AsyncRESTClient``, so it runs in the API process.

        Returns:
            The options for the field's dropdown.

        """
        data = await self.api.get("/accounts", type="advertiser")
        accounts = data.get("accounts", [])
        results = [
            {"advertiser_id": str(a["accountId"]), "name": a.get("accountName") or str(a["accountId"])}
            for a in accounts
        ]
        return sorted(results, key=lambda a: a["name"].lower())

    async def check(self) -> bool:
        """Prove the credentials work by running the ``advertisers`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.advertisers()
        return True
