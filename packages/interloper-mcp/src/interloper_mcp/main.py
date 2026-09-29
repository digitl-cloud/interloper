"""Console entry point for the interloper MCP server."""

from __future__ import annotations

import argparse
import logging
import sys
from uuid import UUID

from interloper.catalog.base import Catalog
from interloper.settings import AppSettings
from interloper_db import Store

from interloper_mcp.context import init_context, set_static_ctx
from interloper_mcp.server import create_mcp_server

logger = logging.getLogger(__name__)


def main() -> None:
    """Run the MCP server on the selected transport."""
    parser = argparse.ArgumentParser(prog="interloper-mcp", description="Interloper MCP server (read-only)")
    parser.add_argument(
        "--transport",
        choices=["streamable-http", "stdio"],
        default="streamable-http",
        help="streamable-http serves bearer-authenticated HTTP (default); stdio authenticates once at startup",
    )
    parser.add_argument("--host", default=None, help="Bind host (overrides INTERLOPER_MCP_HOST)")
    parser.add_argument("--port", type=int, default=None, help="Bind port (overrides INTERLOPER_MCP_PORT)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    settings = AppSettings.get().mcp
    if args.host is not None:
        settings.host = args.host
    if args.port is not None:
        settings.port = args.port

    catalog = Catalog.from_settings()
    store = Store.from_settings(catalog=catalog)
    init_context(store, catalog)

    if args.transport == "stdio":
        org_id, role = _resolve_stdio_scope(store, settings.token, settings.org_id)
        set_static_ctx(org_id, role=role)
        mcp = create_mcp_server(settings, store=None)
        mcp.run(transport="stdio")
    else:
        mcp = create_mcp_server(settings, store=store)
        logger.info("Serving streamable HTTP on %s:%s/mcp", settings.host, settings.port)
        mcp.run(transport="streamable-http")


def _resolve_stdio_scope(store: Store, token: str, org_id: str) -> tuple[UUID, str]:
    """Resolve the organisation scope and role for the stdio transport.

    A PAT (``INTERLOPER_MCP_TOKEN``) is the normal path; a bare
    ``INTERLOPER_MCP_ORG_ID`` is a local-development fallback that skips
    authentication entirely and, being a developer's own instance, runs as
    an editor.

    Args:
        store: The Store to resolve the token against.
        token: Raw personal access token, or empty.
        org_id: Organisation UUID string, or empty.

    Returns:
        The organisation UUID to scope all tool calls to, and the role the
        write tools are gated on.

    Raises:
        SystemExit: If the token is invalid, expired or revoked, or if
            neither environment variable is set.
    """
    if token:
        resolved = store.tokens.resolve(token)
        if resolved is None:
            raise SystemExit("INTERLOPER_MCP_TOKEN is invalid, expired, or revoked")
        profile, pat, role = resolved
        logger.info("Authenticated as %s (role %s)", profile.email, role)
        return pat.organisation_id, role
    if org_id:
        logger.warning("Running without authentication as editor, scoped to org %s (INTERLOPER_MCP_ORG_ID)", org_id)
        return UUID(org_id), "editor"
    raise SystemExit("stdio transport needs INTERLOPER_MCP_TOKEN (or INTERLOPER_MCP_ORG_ID for local development)")


if __name__ == "__main__":
    main()
