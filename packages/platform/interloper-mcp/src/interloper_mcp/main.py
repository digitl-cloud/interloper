"""Console entry point for the interloper MCP server."""

from __future__ import annotations

import argparse
import logging
import sys

from interloper.catalog import Catalog
from interloper.settings import AppSettings
from interloper_db import Audience, Profile, Store

from interloper_mcp.context import init_context, set_static_user
from interloper_mcp.server import create_mcp_server, http_options

logger = logging.getLogger(__name__)


def main() -> None:
    """Run the MCP server on the selected transport."""
    parser = argparse.ArgumentParser(prog="interloper-mcp", description="Interloper MCP server")
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
        set_static_user(_resolve_stdio_user(store, settings.token))
        mcp = create_mcp_server(settings, store=None)
        mcp.run(transport="stdio")
    else:
        mcp = create_mcp_server(settings, store=store)
        logger.info("Serving streamable HTTP on %s:%s/mcp", settings.host, settings.port)
        mcp.run(transport="streamable-http", port=settings.port, **http_options(settings))


def _resolve_stdio_user(store: Store, token: str) -> Profile:
    """Resolve the user the stdio transport runs as, from its token.

    Args:
        store: The Store to resolve the token against.
        token: Raw personal access token (``INTERLOPER_MCP_TOKEN``), or empty.

    Returns:
        The token's holder; each tool call names the organisation it acts in.

    Raises:
        SystemExit: If the token is missing, invalid, expired or revoked.
    """
    if not token:
        raise SystemExit("stdio transport needs INTERLOPER_MCP_TOKEN")
    resolved = store.tokens.resolve(token, Audience.MCP)
    if resolved is None:
        raise SystemExit("INTERLOPER_MCP_TOKEN is invalid, expired, or revoked")
    profile, _ = resolved
    logger.info("Authenticated as %s", profile.email)
    return profile


if __name__ == "__main__":
    main()
