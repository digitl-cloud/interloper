import json
from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.campaign_manager_360 import constants
from interloper_assets.campaign_manager_360.api import CampaignManager360Api
from interloper_assets.google.auth import ServiceAccountAuth


@il.connection(
    name="Campaign Manager 360",
    icon="icon:cm360",
    tags=["Advertising"],
    maturity="beta",
)
class CampaignManager360Connection(il.Connection):
    """Campaign Manager 360 API connection with service account auth."""

    model_config = SettingsConfigDict(env_prefix="campaign_manager_360_")
    key = "campaign_manager_360_connection"

    service_account_key: str = il.JsonField(description="Google service account key JSON")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Campaign Manager 360 (DFA Reporting) API client every caller shares.

        Returns:
            The client authenticated as the service account, cached per connection instance.
        """
        auth = ServiceAccountAuth(json.loads(self.service_account_key), constants.SCOPES)
        return il.AsyncRESTClient(constants.BASE_URL, auth=auth, retry=il.HttpRetryPolicy())

    @cached_property
    def api(self) -> CampaignManager360Api:
        """The Campaign Manager 360 API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return CampaignManager360Api(self.client)

    @il.fetch_field_provider
    async def profiles(self) -> list[dict[str, str]]:
        """Fetch the CM360 user profiles the service account has access to.

        Each profile carries both the profile id and its account id/name, so the
        same lookup feeds the source's ``profile_id`` and ``account_id`` fields.

        Returns:
            The options for the field's dropdown.

        """
        response = await self.api.get("/userprofiles")
        return [
            {
                "profile_id": str(p["profileId"]),
                "account_id": str(p["accountId"]),
                "name": f"{p.get('userName', p['profileId'])} ({p.get('accountName', p['accountId'])})",
                "account_name": str(p.get("accountName", p["accountId"])),
            }
            for p in response.get("items", [])
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``profiles`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.profiles()
        return True
