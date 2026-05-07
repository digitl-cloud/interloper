import json
from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.display_video_360 import constants


@il.connection(
    name="Display & Video 360",
    icon="icon:dv360",
    tags=["Advertising"],
)
class DisplayVideo360Connection(il.Connection):
    """Display & Video 360 API connection using Google service account credentials."""

    model_config = SettingsConfigDict(env_prefix="display_video_360_")
    key = "display_video_360_connection"

    service_account_key: str = il.SecretField(description="Google service account key JSON")

    @cached_property
    def client(self) -> Any:
        """Build and return the DV360 API service client."""
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(self.service_account_key),
            scopes=constants.DISPLAY_VIDEO_SCOPES,
        )

        return build(
            constants.DISPLAY_VIDEO_API_SERVICE,
            constants.DISPLAY_VIDEO_API_VERSION,
            credentials=credentials,
        )
