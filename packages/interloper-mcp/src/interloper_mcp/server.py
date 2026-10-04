"""MCP server factory: FastMCP wired with PAT auth and the toolkit's tools."""

from __future__ import annotations

from interloper.settings import McpSettings
from interloper_db import Store
from mcp.server.auth.settings import AuthSettings as McpAuthSettings
from mcp.server.fastmcp import FastMCP
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


def create_mcp_server(settings: McpSettings, store: Store | None = None) -> FastMCP:
    """Build the FastMCP server.

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
        # external_url doubles as issuer and resource URL in the RFC 9728
        # protected-resource metadata; default to the local bind address so
        # dev servers work without configuration.
        base_url = settings.external_url or f"http://localhost:{settings.port}"
        auth_kwargs = {
            "token_verifier": PatVerifier(store),
            "auth": McpAuthSettings(
                issuer_url=AnyHttpUrl(base_url),
                resource_server_url=AnyHttpUrl(base_url),
            ),
        }

    mcp = FastMCP(
        SERVER_NAME,
        instructions=INSTRUCTIONS,
        host=settings.host,
        port=settings.port,
        stateless_http=True,
        **auth_kwargs,
    )
    register_tools(mcp)
    return mcp
