from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.brandwatch import constants
from interloper_assets.brandwatch.api import BrandwatchApi


@il.connection(
    name="Brandwatch",
    icon="fluent:connector-24-filled",
    tags=["Social Media"],
    maturity="beta",
)
class BrandwatchConnection(il.Connection):
    """Brandwatch (Falcon.io) Measure API connection with API-key auth."""

    model_config = SettingsConfigDict(env_prefix="brandwatch_")

    api_key: str = il.SecretField(label="API Key", description="Brandwatch (Falcon.io) Measure API key")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Brandwatch API client every caller shares.

        Returns:
            The authenticated client, cached per connection instance.
        """
        # Brandwatch's API reference authenticates every request by an `apikey` query parameter, never a header.
        return il.AsyncRESTClient(
            constants.BASE_URL, params={"apikey": self.api_key}, timeout=60, retry=il.HttpRetryPolicy()
        )

    @cached_property
    def api(self) -> BrandwatchApi:
        """The Brandwatch Measure API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return BrandwatchApi(self.client)

    async def check(self) -> bool:
        """Prove the key works by reading the channel metrics' metadata.

        Returns:
            True; a refused key raises out of the read.
        """
        await self.api.get("/metrics/channel")
        return True
