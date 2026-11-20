from functools import cached_property

import interloper as il
from interloper.errors import ConnectionCheckError
from pydantic_settings import SettingsConfigDict

from interloper_assets.linkedin.api import LinkedinApi, LinkedinApiError
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
            retry=il.HttpRetryPolicy(),
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
        approved ``ADMINISTRATOR`` access control entries, then names their
        organizations in one batch lookup of administered organizations. An
        organization LinkedIn does not name is listed by its id.

        Returns:
            The options for the field's dropdown.

        Raises:
            ConnectionCheckError: If LinkedIn refuses the lookup, carrying LinkedIn's code and message, which name
                no credential.
        """
        try:
            acls = await self.api.rows(
                "/organizationAcls", q="roleAssignee", role="ADMINISTRATOR", state="APPROVED", count=100
            )
            # LinkedIn's samples name the organization URN either way
            urns = [acl.get("organization") or acl.get("organizationTarget") for acl in acls]
            organization_ids = [urn.rsplit(":", 1)[-1] for urn in urns if urn]
            if not organization_ids:
                return []
            names = (await self.api.get("/organizations", ids=organization_ids))["results"]
        except LinkedinApiError as error:
            raise ConnectionCheckError(str(error)) from error

        return [
            {"id": organization_id, "name": names.get(organization_id, {}).get("localizedName") or organization_id}
            for organization_id in organization_ids
        ]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``organizations`` lookup.

        Returns:
            True — any credential failure raises out of the lookup, LinkedIn's refusal as a `ConnectionCheckError`.
        """
        await self.organizations()
        return True
