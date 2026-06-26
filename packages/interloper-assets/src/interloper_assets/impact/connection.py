from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.impact.constants import BASE_URL


@il.connection(
    name="Impact",
    icon="fluent:connector-24-filled",
    tags=["Affiliate"],
)
class ImpactConnection(il.Connection):
    """Impact.com API connection with HTTP Basic Auth (account SID + auth token)."""

    model_config = SettingsConfigDict(env_prefix="impact_")

    account_sid: str = il.InputField(description="Impact account SID")
    auth_token: str = il.SecretField(description="Impact auth token")

    @cached_property
    def client(self) -> il.RESTClient:
        auth = httpx.BasicAuth(username=self.account_sid, password=self.auth_token)
        return il.RESTClient(
            f"{BASE_URL}/Advertisers/{self.account_sid}",
            auth,
            headers={"Accept": "application/json"},
        )

    @il.fetch_field_provider
    async def programs(self) -> list[dict[str, str]]:
        """Fetch Impact programs (campaigns) accessible by the connection."""
        programs: list[dict[str, str]] = []
        async with httpx.AsyncClient(
            timeout=30,
            auth=httpx.BasicAuth(self.account_sid, self.auth_token),
            headers={"Accept": "application/json"},
        ) as client:
            page, num_pages = 1, 1
            while page <= num_pages:
                resp = await client.get(
                    f"{BASE_URL}/Advertisers/{self.account_sid}/Campaigns",
                    params={"Page": page},
                )
                resp.raise_for_status()
                data = resp.json()
                num_pages = int(data["@numpages"])
                for campaign in data.get("Campaigns", []):
                    programs.append({"Id": campaign["Id"], "Name": campaign.get("Name", campaign["Id"])})
                page += 1

        return programs
