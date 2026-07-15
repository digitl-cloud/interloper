import asyncio
import json
from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.campaign_manager_360 import constants


@il.connection(
    name="Campaign Manager 360",
    icon="icon:cm360",
    tags=["Advertising"],
)
class CampaignManager360Connection(il.Connection):
    """Campaign Manager 360 API connection with service account auth."""

    model_config = SettingsConfigDict(env_prefix="campaign_manager_360_")
    key = "campaign_manager_360_connection"

    service_account_key: str = il.JsonField(description="Google service account key JSON")

    @cached_property
    def client(self) -> Any:
        """Build the Campaign Manager 360 (DFA Reporting) API service client."""
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=constants.SCOPES,
        )
        return build(constants.API_SERVICE, constants.API_VERSION, credentials=credentials)

    @il.fetch_field_provider
    async def profiles(self) -> list[dict[str, str]]:
        """Fetch the CM360 user profiles the service account has access to.

        Each profile carries both the profile id and its account id/name, so the
        same lookup feeds the source's ``profile_id`` and ``account_id`` fields.
        """
        response = await asyncio.to_thread(lambda: self.client.userProfiles().list().execute())
        return [
            {
                "profile_id": str(p["profileId"]),
                "account_id": str(p["accountId"]),
                "name": f"{p.get('userName', p['profileId'])} ({p.get('accountName', p['accountId'])})",
                "account_name": f"{p.get('accountName', p['accountId'])} ({p['accountId']})",
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
