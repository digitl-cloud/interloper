from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict


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
        import json

        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        key_data = json.loads(self.service_account_key)
        scopes = [
            "https://www.googleapis.com/auth/dfareporting",
            "https://www.googleapis.com/auth/dfatrafficking",
            "https://www.googleapis.com/auth/cloud-platform",
        ]
        credentials = service_account.Credentials.from_service_account_info(key_data, scopes=scopes)

        return build("dfareporting", "v4", credentials=credentials)
