import json
from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.double_click_bid_manager import constants


@il.connection(
    name="DoubleClick Bid Manager",
    icon="icon:dv360",
    tags=["Advertising"],
)
class DoubleClickBidManagerConnection(il.Connection):
    """DoubleClick Bid Manager API connection using Google service account credentials."""

    model_config = SettingsConfigDict(env_prefix="double_click_bid_manager_")

    service_account_key: str = il.SecretField(description="Google service account key JSON")

    @cached_property
    def client(self) -> Any:
        """Build and return the DBM API service client."""
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=constants.DBM_SCOPES,
        )

        return build(
            constants.DBM_API_SERVICE,
            constants.DBM_API_VERSION,
            credentials=credentials,
        )
