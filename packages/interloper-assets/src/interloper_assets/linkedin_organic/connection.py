from functools import cached_property

import httpx
import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.linkedin_organic import constants


@il.connection(
    name="LinkedIn Organic",
    icon="devicon:linkedin",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "linkedin",
        scope="r_organization_social,rw_organization_admin,r_organization_social_feed",
    ),
)
class LinkedinOrganicConnection(il.OAuthConnection):
    """LinkedIn Organic API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="linkedin_organic_")

    @cached_property
    def client(self) -> il.RESTClient:
        client = il.RESTClient(
            constants.BASE_URL,
            auth=il.OAuth2RefreshTokenAuth(
                base_url=constants.AUTH_BASE_URL,
                token_endpoint=constants.TOKEN_ENDPOINT,
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
            ),
        )
        client.headers.update(
            {
                "LinkedIn-Version": constants.LINKEDIN_VERSION,
                "X-Restli-Protocol-Version": constants.RESTLI_PROTOCOL_VERSION,
                "Accept-Encoding": "gzip, deflate, br",
            }
        )
        return client

    async def _get_access_token(self) -> str:
        """Exchange the refresh token for an access token."""
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{constants.AUTH_BASE_URL}{constants.TOKEN_ENDPOINT}",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            response.raise_for_status()
            return response.json()["access_token"]

    @il.fetch_field_provider
    async def organizations(self) -> list[dict[str, str]]:
        """List the organizations this connection administers.

        Backs the source's ``organization_id`` ``FetchField``. Talks to the
        versioned REST API over httpx (not the SDK) so it runs in the API
        process. Reads the ``ADMINISTRATOR`` organization ACLs and decorates
        each entry with the organization entity to resolve its localized name.
        """
        token = await self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "LinkedIn-Version": constants.LINKEDIN_VERSION,
            "X-Restli-Protocol-Version": constants.RESTLI_PROTOCOL_VERSION,
        }

        organizations: list[dict[str, str]] = []
        start = 0
        count = 100

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            while True:
                response = await client.get(
                    f"{constants.BASE_URL}/organizationAcls",
                    params={
                        "q": "roleAssignee",
                        "role": "ADMINISTRATOR",
                        "state": "APPROVED",
                        "start": str(start),
                        "count": str(count),
                        # Decorate the ACL with the organization entity so the
                        # localized name comes back in a single request.
                        "projection": (
                            "(elements*(organization~(id,localizedName)),paging)"
                        ),
                    },
                )
                response.raise_for_status()
                data = response.json()

                elements = data.get("elements", [])
                for element in elements:
                    # ``organization`` is the URN
                    # (e.g. "urn:li:organization:12345"); ``organization~`` is
                    # the decorated entity when the projection resolves.
                    org_urn = element.get("organization", "")
                    org_id = org_urn.rsplit(":", 1)[-1] if org_urn else ""
                    if not org_id:
                        continue

                    org_entity = element.get("organization~") or {}
                    name = org_entity.get("localizedName") or org_id

                    organizations.append({"id": org_id, "name": name})

                paging = data.get("paging", {})
                total = paging.get("total")
                start += count
                if not elements or (total is not None and start >= total):
                    break

        return organizations
