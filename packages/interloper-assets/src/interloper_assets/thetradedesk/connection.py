from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.thetradedesk.constants import BASE_URL


@il.connection(
    name="The Trade Desk",
    icon="icon:thetradedesk",
    tags=["Advertising"],
)
class TheTradeDeskConnection(il.Connection):
    """The Trade Desk API connection with custom TTD-Auth header authentication."""

    model_config = SettingsConfigDict(env_prefix="thetradedesk_")

    api_key: str = il.SecretField(title="API Key", description="The Trade Desk API key")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client for the The Trade Desk API."""
        return il.AsyncRESTClient(
            BASE_URL,
            headers={"TTD-Auth": self.api_key},
            timeout=60,
        )

    @il.fetch_field_provider
    async def partners(self) -> list[dict[str, str]]:
        """Fetch the partners accessible to the API key."""
        response = await self.client.post("/partner/query", json={"PageStartIndex": 0, "PageSize": 100})
        response.raise_for_status()
        return [
            {
                "partner_id": p["PartnerId"],
                "name": f"{p.get('PartnerName', p['PartnerId'])} ({p['PartnerId']})",
            }
            for p in response.json().get("Result", [])
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``partners`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.partners()
        return True
