from functools import cached_property
from typing import Any

import interloper as il
from pydantic_settings import SettingsConfigDict

from interloper_assets.thetradedesk.constants import BASE_URL, GRAPHQL_PAGE_SIZE, GRAPHQL_URL


@il.connection(
    name="The Trade Desk",
    icon="icon:thetradedesk",
    tags=["Advertising"],
)
class TheTradeDeskConnection(il.Connection):
    """The Trade Desk API connection with custom TTD-Auth header authentication."""

    model_config = SettingsConfigDict(env_prefix="thetradedesk_")

    api_key: str = il.SecretField(label="API Key", description="The Trade Desk API key")

    @cached_property
    def client(self) -> il.AsyncRESTClient:
        """Async REST client for the The Trade Desk API."""
        return il.AsyncRESTClient(
            BASE_URL,
            headers={"TTD-Auth": self.api_key},
            timeout=60,
        )

    @il.fetch_field_provider
    async def partners(self) -> list[dict[str, str]]:
        """Fetch the partners accessible to the API key.

        Returns:
            The options for the field's dropdown.

        """
        response = await self.client.post("/partner/query", json={"PageStartIndex": 0, "PageSize": 100})
        response.raise_for_status()
        return [
            {
                "partner_id": p["PartnerId"],
                "name": f"{p.get('PartnerName', p['PartnerId'])} ({p['PartnerId']})",
            }
            for p in response.json().get("Result", [])
        ]

    async def nodes(self, query: str, variables: dict[str, Any], path: tuple[str, ...]) -> list[dict[str, Any]]:
        """Run a paginated GraphQL query and return every node of the connection at *path*.

        The query declares ``$first`` and ``$after`` and selects ``nodes`` and
        ``pageInfo { hasNextPage endCursor }`` on the connection; each page
        asks for the next cursor until the connection says there is none.
        The Trade Desk answers a failed query with HTTP 200 and an
        ``errors`` list, so the list is checked on every page.

        Args:
            query: The GraphQL document.
            variables: The query's variables, without ``first`` and ``after``.
            path: The fields leading from ``data`` to the connection, such as
                ``("partner", "advertisers")``. A null object on the way (an
                id the key cannot see) reads as no nodes.

        Returns:
            The nodes of every page, as the API returned them.

        Raises:
            RuntimeError: If the API answers with GraphQL errors.
        """
        nodes: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            response = await self.client.post(
                GRAPHQL_URL,
                json={"query": query, "variables": {**variables, "first": GRAPHQL_PAGE_SIZE, "after": after}},
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("errors"):
                messages = "; ".join(error.get("message", str(error)) for error in payload["errors"])
                raise RuntimeError(f"The Trade Desk GraphQL query failed: {messages}")
            listing = payload.get("data")
            for field in path:
                listing = listing.get(field) if listing is not None else None
            if listing is None:
                return nodes
            nodes.extend(listing.get("nodes") or [])
            page = listing.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                return nodes
            after = page["endCursor"]

    async def check(self) -> bool:
        """Prove the credentials work by running the ``partners`` lookup.

        Returns:
            True — any credential failure raises out of the lookup.
        """
        await self.partners()
        return True
