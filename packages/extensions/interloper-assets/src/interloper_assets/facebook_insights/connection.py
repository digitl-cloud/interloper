from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import cached_property

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.facebook_insights import constants
from interloper_assets.facebook_insights.api import FacebookInsightsApi


@il.connection(
    name="Facebook Insights",
    icon="logos:facebook",
    tags=["Social"],
    oauth=il.OAuthConfig(
        "facebook",
        scope="pages_show_list,pages_read_engagement,pages_read_user_content,read_insights",
    ),
    maturity="beta",
)
class FacebookInsightsConnection(il.RefreshTokenOAuthConnection):
    """Facebook Insights API connection with OAuth2 refresh token auth.

    Uses the standard ``client_id`` / ``client_secret`` / ``refresh_token``
    trio from ``OAuthConnection``; ``refresh_token`` holds a long-lived access
    token used directly as the bearer.
    """

    model_config = SettingsConfigDict(env_prefix="facebook_insights_")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client for the Facebook Graph API, authenticated as the user.

        The ``refresh_token`` field holds a long-lived user access token, used
        directly as the bearer.

        Returns:
            The client, pinned to ``constants.API_VERSION`` and cached per connection instance.
        """
        return self._client(self.refresh_token)

    @cached_property
    def api(self) -> FacebookInsightsApi:
        """The Graph API engine acting as the user, built on ``client``.

        Returns:
            The engine, cached per connection instance.
        """
        return FacebookInsightsApi(self.client)

    @asynccontextmanager
    async def page_api(self, page_id: str) -> AsyncGenerator[FacebookInsightsApi, None]:
        """Open a Graph API engine acting as the Page.

        Page and post insights (and the Page's published posts and stories)
        only answer to a Page access token; the connection holds a user token,
        so the Page token is derived from it via ``GET /{page_id}?fields=access_token``,
        which returns one only when the user can perform the ``ANALYZE`` task on the Page.

        Args:
            page_id: The Facebook Page to act as.

        Yields:
            An engine on a new client carrying the Page access token as its bearer, closed on exit.

        Raises:
            PermissionError: If the user token yields no Page access token for *page_id*.
        """
        page = await self.api.get(f"/{page_id}", fields="access_token")
        page_token = page.get("access_token")
        if not page_token:
            raise PermissionError(f"The connection's user cannot obtain a Page access token for page {page_id}")
        async with self._client(page_token) as client:
            yield FacebookInsightsApi(client)

    @il.fetch_field_provider
    async def pages(self) -> list[dict[str, str]]:
        """List the Facebook Pages reachable by this connection.

        Backs the source's ``page_id`` ``FetchField``. Talks to the Graph API
        over the lightweight ``AsyncRESTClient`` (not the SDK) so it runs in the
        API process. The ``refresh_token`` field holds a long-lived access token,
        used directly as the bearer for ``GET /me/accounts``.

        Returns:
            The options for the field's dropdown.

        """
        pages = await self.api.rows("/me/accounts", fields=["id", "name"], limit=100)
        return [{"id": page["id"], "name": page.get("name", page["id"])} for page in pages]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``pages`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.pages()
        return True

    @staticmethod
    def _client(access_token: str) -> il.AsyncRESTClient:
        """Build a Graph API client sending *access_token* as its bearer.

        Args:
            access_token: The user or Page access token every request carries.

        Returns:
            A new client, pinned to ``constants.API_VERSION``.
        """
        return il.AsyncRESTClient(
            f"{constants.BASE_URL}/{constants.API_VERSION}",
            auth=il.HTTPBearerAuth(access_token),
        )
