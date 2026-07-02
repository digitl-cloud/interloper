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

    account_sid: str = il.InputField(title="Account SID", description="Impact account SID")
    auth_token: str = il.SecretField(description="Impact auth token")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        return il.AsyncRESTClient(
            f"{BASE_URL}/Advertisers/{self.account_sid}",
            auth=httpx.BasicAuth(username=self.account_sid, password=self.auth_token),
            headers={"Accept": "application/json"},
        )

    @il.fetch_field_provider
    async def programs(self) -> list[dict[str, str]]:
        """Fetch Impact programs (campaigns) accessible by the connection."""
        paginator = il.PageNumberPaginator(page_param="Page", total_path="@numpages")
        pages = self.client.paginate("/Campaigns", paginator, data_selector=lambda r: r.json().get("Campaigns") or [])
        return [{"Id": c["Id"], "Name": c.get("Name", c["Id"])} async for page in pages for c in page]
