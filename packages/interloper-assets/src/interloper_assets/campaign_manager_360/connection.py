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
    def credentials(self) -> Any:
        """Load the service account credentials, shared by every client built from them.

        Returns:
            The ``google.oauth2`` service account credentials, scoped to CM360 reporting.
        """
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=constants.SCOPES,
        )

    def client(self) -> Any:
        """Build a fresh Campaign Manager 360 (DFA Reporting) API service client.

        Not cached: the client's ``httplib2`` transport is not thread-safe and
        the assets run concurrently in threads, so each caller builds its own.

        Returns:
            The ``dfareporting`` discovery client over the shared credentials.
        """
        from googleapiclient.discovery import build

        return build(constants.API_SERVICE, constants.API_VERSION, credentials=self.credentials)

    @il.fetch_field_provider
    async def profiles(self) -> list[dict[str, str]]:
        """Fetch the CM360 user profiles the service account has access to.

        Each profile carries both the profile id and its account id/name, so the
        same lookup feeds the source's ``profile_id`` and ``account_id`` fields.

        Returns:
            The options for the field's dropdown.

        """
        response = await asyncio.to_thread(lambda: self.client().userProfiles().list().execute())
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
