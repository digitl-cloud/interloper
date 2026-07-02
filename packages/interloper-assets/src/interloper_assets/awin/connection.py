from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.awin.constants import BASE_URL


@il.connection(
    name="Awin",
    icon="icon:awin",
    tags=["Affiliate"],
)
class AwinConnection(il.Connection):
    """Awin affiliate network API connection with bearer token auth."""

    model_config = SettingsConfigDict(env_prefix="awin_")

    access_token: str = il.SecretField(description="Awin API access token")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        return il.AsyncRESTClient(BASE_URL, auth=il.HTTPBearerAuth(self.access_token))

    @il.fetch_field_provider
    async def advertisers(self) -> list[dict[str, str]]:
        """List the advertiser accounts this token can access, sorted by name.

        Backs the source's ``advertiser_id`` ``FetchField``. Reuses ``self.client``
        (bearer auth is all Awin's ``/accounts`` endpoint needs) so it runs in the
        API process over the lightweight ``AsyncRESTClient``.
        """
        response = await self.client.get("/accounts", params={"type": "advertiser"})
        response.raise_for_status()
        accounts = response.json().get("accounts", [])
        results = [
            {"advertiser_id": str(a["accountId"]), "name": a.get("accountName") or str(a["accountId"])}
            for a in accounts
        ]
        return sorted(results, key=lambda a: a["name"].lower())
