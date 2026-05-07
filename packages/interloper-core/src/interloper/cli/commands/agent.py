"""``interloper agent`` — launch the ADK development UI.

Starts the Google ADK web interface for the interloper agent, which
provides natural-language discovery, lineage analysis, and operational
monitoring of the asset catalog.

Available when ``interloper-agent`` is installed.
"""

from __future__ import annotations

import argparse
import logging

logger = logging.getLogger(__name__)


def register(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    """Register the ``agent`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    agent_parser = subparsers.add_parser(
        "agent",
        help="Launch the Interloper Agent development UI",
    )
    agent_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind host (default: 127.0.0.1)",
    )
    agent_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Bind port (default: 8000)",
    )
    agent_parser.set_defaults(func=_cmd_agent, requires=["interloper_agent"])


def _cmd_agent(args: argparse.Namespace) -> None:
    """Start the ADK development UI for the interloper agent."""
    from pathlib import Path

    import uvicorn
    from google.adk.cli.fast_api import get_fast_api_app

    from interloper.settings import AppSettings

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    settings = AppSettings.get()

    # Initialize the agent context (Store + catalog) before ADK loads the agent
    from interloper.catalog import Catalog

    catalog = Catalog.from_settings()

    from interloper_agent.context import init

    init(settings.postgres.dsn, catalog)

    # Resolve agents_dir to the parent of the interloper_agent package so
    # ADK's AgentLoader.list_agents() finds the ``interloper_agent/`` directory
    # on disk and can import ``interloper_agent.agent.root_agent``.
    import interloper_agent

    agents_dir = str(Path(interloper_agent.__file__).resolve().parent.parent)

    app = get_fast_api_app(
        agents_dir=agents_dir,
        web=True,
        host=args.host,
        port=args.port,
    )

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
