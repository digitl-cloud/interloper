from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.brandwatch import constants


@il.connection(
    name="Brandwatch",
    icon="fluent:connector-24-filled",
    tags=["Social Media"],
)
class BrandwatchConnection(il.Connection):
    """Brandwatch (Falcon.io) Measure API connection with API-key auth."""

    model_config = SettingsConfigDict(env_prefix="brandwatch_")

    api_key: str = il.SecretField(label="API Key", description="Brandwatch (Falcon.io) Measure API key")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        # Falcon.io authenticates the Measure API with the key as a query
        # parameter applied to every request. NOTE: the parameter name
        # (`param_key`) is carried over unverified from the reference connector
        # and may need to be `apikey` — confirm against a live key.
        return il.AsyncRESTClient(constants.BASE_URL, params={"param_key": self.api_key}, timeout=60)
