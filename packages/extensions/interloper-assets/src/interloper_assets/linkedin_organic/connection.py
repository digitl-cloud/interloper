from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.linkedin.api import LinkedinApi
from interloper_assets.linkedin_organic import constants


@il.connection(
    name="LinkedIn Organic",
    icon="devicon:linkedin",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "linkedin",
        scope="r_organization_social,rw_organization_admin,r_organization_social_feed",
    ),
    maturity="beta",
)
class LinkedinOrganicConnection(il.RefreshTokenOAuthConnection):
    """LinkedIn Organic API connection with OAuth2 refresh token auth."""

    model_config = SettingsConfigDict(env_prefix="linkedin_organic_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """The LinkedIn organic API client every caller shares.

        Returns:
            The authenticated client, cached per connection instance.
        """
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

    @cached_property
    def api(self) -> LinkedinApi:
        """The LinkedIn REST API engine every caller shares, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return LinkedinApi(self.client)

    @il.fetch_field_provider
    async def organizations(self) -> list[dict[str, str]]:
        """List the organizations this connection administers.

        Backs the source's ``organization_id`` ``FetchField``. Reads the
        ``ADMINISTRATOR`` organization ACLs and decorates each entry with the
        organization entity to resolve its localized name.

        Returns:
            The options for the field's dropdown.

        """
        elements = await self.api.rows(
            "/organizationAcls",
            q="roleAssignee",
            role="ADMINISTRATOR",
            state="APPROVED",
            # Decorate the ACL with the organization entity so the localized
            # name comes back in a single request.
            projection="(elements*(organization~(id,localizedName)),paging)",
            count=100,
        )

        organizations: list[dict[str, str]] = []
        for element in elements:
            # ``organization`` is the URN (e.g. "urn:li:organization:12345");
            # ``organization~`` is the decorated entity when the projection
            # resolves.
            urn = element.get("organization", "")
            organization_id = urn.rsplit(":", 1)[-1] if urn else ""
            if not organization_id:
                continue

            entity = element.get("organization~") or {}
            name = entity.get("localizedName") or organization_id

            organizations.append({"id": organization_id, "name": name})

        return organizations

    async def check(self) -> bool:
        """Prove the credentials work by running the ``organizations`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.organizations()
        return True
