"""MCP server factory: MCPServer wired with bearer-token auth and the toolkit's tools."""

from __future__ import annotations

from importlib.metadata import version
from typing import Any

from interloper.settings import AppSettings, McpSettings
from interloper_db import Store
from mcp.server.auth.settings import AuthSettings as McpAuthSettings
from mcp.server.mcpserver import MCPServer
from mcp.types import Icon

from interloper_mcp.auth import PatVerifier
from interloper_mcp.tools import register_tools

SERVER_NAME = "interloper"
SERVER_TITLE = "Interloper"

INSTRUCTIONS = (
    "Access to an interloper deployment: the catalog of available component "
    "definitions, the organisation's collection (sources, connections, "
    "destinations, jobs), asset lineage, run and backfill monitoring, "
    "analytics (job health, run statistics, coverage, error breakdowns), and, for "
    "users holding the editor role, the writes: editing components, creating "
    "sources and jobs, toggling, triggering, retrying and canceling runs and "
    "backfills. Connections are set up in the app; request_connection_setup "
    "hands the user the link. Recap a write and get the user's explicit "
    "confirmation before calling it. Every tool acts in one organisation, "
    "named by its `organisation` argument (an id from list_organisations), "
    "which may be left out when the user belongs to exactly one."
)


def http_options(settings: McpSettings) -> dict[str, Any]:
    """Options the streamable HTTP transport is served with.

    Passed to ``run()`` and ``streamable_http_app()`` alike, so the app a test
    drives is the one production serves.

    Args:
        settings: The MCP settings (host).

    Returns:
        The keyword arguments for the SDK's streamable HTTP entry points.
    """
    return {"host": settings.host, "stateless_http": True}


def create_mcp_server(settings: McpSettings, store: Store | None = None) -> MCPServer:
    """Build the MCP server.

    The server introduces itself to clients with its title, version, the app's
    URL and the app's icon, all from the app's public URL
    (``server.public_url``). The protected-resource metadata names the
    interloper authorization server, served at that same URL, as the issuer of
    the tokens the server accepts.

    Args:
        settings: The MCP settings (host, port, external URL).
        store: When provided, requests authenticate with bearer tokens
            against this store (the streamable HTTP mode). ``None`` builds an
            unauthenticated server for the stdio transport, whose user is
            seeded once at startup instead.

    Returns:
        The configured server with the tools registered.
    """
    server = AppSettings.get().server
    auth_kwargs: dict = {}
    if store is not None:
        # The endpoint's URL (https://host/mcp) is the RFC 9728 resource, which puts the
        # metadata at /.well-known/oauth-protected-resource/mcp.
        # Strings, not AnyHttpUrl instances: the SDK's settings model keeps a path-less URL
        # as given only when it validates the string itself, and the issuer must match the
        # authorization server's own metadata exactly (RFC 8414 section 3.3).
        auth_kwargs = {
            "token_verifier": PatVerifier(store),
            "auth": McpAuthSettings.model_validate(
                {
                    "issuer_url": server.public_url,
                    "resource_server_url": settings.public_url(server),
                    "validate_token_resource": False,
                }
            ),
        }

    mcp = MCPServer(
        SERVER_NAME,
        title=SERVER_TITLE,
        instructions=INSTRUCTIONS,
        website_url=server.public_url,
        icons=[Icon(src=f"{server.public_url}/icon.svg", mime_type="image/svg+xml", sizes=["any"])],
        version=version("interloper-mcp"),
        **auth_kwargs,
    )
    register_tools(mcp)
    return mcp
