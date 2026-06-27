from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.brandwatch import constants


@il.connection(
    name="Brandwatch",
    icon="fluent:connector-24-filled",
    tags=["Social"],
)
class BrandwatchConnection(il.Connection):
    """Brandwatch (Falcon.io) API connection with API key auth."""

    model_config = SettingsConfigDict(env_prefix="brandwatch_")

    api_key: str = il.SecretField(description="Brandwatch API key")
    channel_id: str = il.InputField(description="Channel ID to fetch insights for")
    network: str = il.SelectField(
        options=[
            {"label": "Facebook", "value": "facebook"},
            {"label": "Instagram", "value": "instagram"},
            {"label": "LinkedIn", "value": "linkedin"},
            {"label": "Twitter", "value": "twitter"},
            {"label": "YouTube", "value": "youtube"},
        ],
        description="Social network to fetch insights for",
    )

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        return il.AsyncRESTClient(constants.BASE_URL, params={"param_key": self.api_key})
