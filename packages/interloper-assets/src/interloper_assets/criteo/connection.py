from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.criteo import constants


@il.connection(
    name="Criteo",
    icon="icon:criteo",
    tags=["Advertising"],
    oauth=il.OAuthConfig("criteo"),
)
class CriteoConnection(il.OAuthConnection):
    """Criteo API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="criteo_")

    @cached_property
    def client(self) -> il.RESTClient:
        return il.RESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.BASE_URL,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
                token_endpoint="/oauth2/token",
            ),
        )

    @il.fetch_field_provider
    async def advertisers(self) -> list[dict[str, str]]:
        """Fetch the Criteo advertisers accessible by this connection, sorted by name."""
        async with httpx.AsyncClient(timeout=30) as client:
            token_resp = await client.post(
                f"{constants.BASE_URL}/oauth2/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                },
            )
            token_resp.raise_for_status()
            access_token = token_resp.json()["access_token"]

            advertisers_resp = await client.get(
                f"{constants.BASE_URL}/{constants.API_VERSION}/advertisers/me",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            advertisers_resp.raise_for_status()
            advertisers = advertisers_resp.json().get("data", [])

        results = [
            {"id": a["id"], "name": a.get("attributes", {}).get("advertiserName") or a["id"]}
            for a in advertisers
        ]
        return sorted(results, key=lambda a: a["name"].lower())
