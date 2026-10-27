"""MCP server factory: MCPServer wired with PAT auth and the toolkit's tools."""

from __future__ import annotations

from typing import Any

from interloper.settings import McpSettings
from interloper_db import Store
from mcp.server.auth.settings import AuthSettings as McpAuthSettings
from mcp.server.mcpserver import MCPServer
from pydantic import AnyHttpUrl

from interloper_mcp.auth import PatVerifier
from interloper_mcp.tools import register_tools

SERVER_NAME = "interloper"

INSTRUCTIONS = (
    "Access to an interloper deployment: the catalog of available component "
    "definitions, the organisation's collection (sources, connections, "
    "destinations, jobs), asset lineage, run and backfill monitoring, "
    "analytics (job health, run statistics, coverage, error breakdowns), and, for "
    "tokens holding the editor role, the writes: editing components, creating "
    "sources and jobs, toggling, triggering, retrying and canceling runs and "
    "backfills. Connections are set up in the app; request_connection_setup "
    "hands the user the link. Recap a write and get the user's explicit "
    "confirmation before calling it. All results are scoped to the "
    "organisation of the authenticated token."
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

    Args:
        settings: The MCP settings (host, port, external URL).
        store: When provided, requests authenticate as bearer PATs against
            this store (the streamable HTTP mode). ``None`` builds an
            unauthenticated server for the stdio transport, whose context is
            seeded once at startup instead.

    Returns:
        The configured server with the tools registered.
    """
    auth_kwargs: dict = {}
    if store is not None:
        # external_url names the MCP endpoint (https://host/mcp): it is the RFC 9728
        # resource, which puts the metadata at /.well-known/oauth-protected-resource/mcp.
        # Default to the local bind address so dev servers work without configuration.
        local_url = f"http://localhost:{settings.port}"
        auth_kwargs = {
            "token_verifier": PatVerifier(store),
            "auth": McpAuthSettings(
                issuer_url=AnyHttpUrl(settings.external_url or local_url),
                resource_server_url=AnyHttpUrl(settings.external_url or f"{local_url}/mcp"),
                validate_token_resource=False,
            ),
        }

    mcp = MCPServer(SERVER_NAME, instructions=INSTRUCTIONS, **auth_kwargs)
    register_tools(mcp)
    return mcp
