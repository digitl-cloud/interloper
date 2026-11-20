from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.adtraction.api import AdtractionApi


@il.connection(
    name="Adtraction",
    icon="icon:adtraction",
    tags=["Affiliate"],
    maturity="beta",
)
class AdtractionConnection(il.Connection):
    """Adtraction advertiser API connection with API token auth."""

    model_config = SettingsConfigDict(env_prefix="adtraction_")

    api_token: str = il.SecretField(
        label="API Token", description="Adtraction API token, under Account > Settings in the advertiser account"
    )

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The Adtraction API client every caller shares.

        Adtraction refuses a request without a JSON content type with HTTP 415,
        a GET included, so the client sends one on every request.

        Returns:
            The authenticated client, cached per connection instance.
        """
        headers = {"X-Token": self.api_token, "Accept": "application/json", "Content-Type": "application/json"}
        return il.AsyncRESTClient("https://api.adtraction.net/v2", headers=headers, retry=il.HttpRetryPolicy())

    @cached_property
    def api(self) -> AdtractionApi:
        """The Adtraction API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return AdtractionApi(self.client)

    async def check(self) -> bool:
        """Prove the token works by reading the advertiser account it belongs to.

        Returns:
            True; a refused token raises out of the read.
        """
        await self.api.get("/advertiser/account/")
        return True
