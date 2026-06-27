from functools import cached_property

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
    def client(self) -> il.AsyncRESTClient:
        client = il.AsyncRESTClient(
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

    @il.fetch_field_provider
    async def organizations(self) -> list[dict[str, str]]:
        """List the organizations this connection administers.

        Backs the source's ``organization_id`` ``FetchField``. Reuses
        ``self.client`` to read the ``ADMINISTRATOR`` organization ACLs and
        decorates each entry with the organization entity to resolve its
        localized name.
        """
        paginator = il.OffsetPaginator(
            limit=100,
            offset_param="start",
            limit_param="count",
            total_path="paging.total",
            data_selector="elements",
        )
        params = {
            "q": "roleAssignee",
            "role": "ADMINISTRATOR",
            "state": "APPROVED",
            # Decorate the ACL with the organization entity so the localized
            # name comes back in a single request.
            "projection": "(elements*(organization~(id,localizedName)),paging)",
        }
        pages = self.client.paginate(
            "/organizationAcls",
            paginator,
            params=params,
            data_selector="elements",
        )

        organizations: list[dict[str, str]] = []
        async for page in pages:
            for element in page:
                # ``organization`` is the URN (e.g. "urn:li:organization:12345");
                # ``organization~`` is the decorated entity when the projection
                # resolves.
                org_urn = element.get("organization", "")
                org_id = org_urn.rsplit(":", 1)[-1] if org_urn else ""
                if not org_id:
                    continue

                org_entity = element.get("organization~") or {}
                name = org_entity.get("localizedName") or org_id

                organizations.append({"id": org_id, "name": name})

        return organizations
