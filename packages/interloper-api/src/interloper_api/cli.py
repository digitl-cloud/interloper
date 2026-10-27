"""``interloper api`` serves the HTTP API; ``interloper oauth`` registers its OAuth clients."""

from __future__ import annotations

import argparse
import logging

from interloper.catalog import Catalog
from interloper.cli.console import configure_console_logging
from interloper.settings import AppSettings
from interloper.utils import Supervisor
from interloper_db import Store
from interloper_db.provision import bootstrap

from interloper_api.authorization import Client
from interloper_api.server import ApiServer


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``api`` command.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    parser = subparsers.add_parser(
        "api",
        help="Serve the HTTP API",
        description="Serve the HTTP API. The frontend is served separately: by nginx in containers, by "
        "`interloper app` locally.",
    )
    parser.add_argument("--host", default=None, help="Bind host (default: server.host, 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None, help="Bind port (default: server.port, 3000)")
    parser.add_argument(
        "--no-create-tables",
        action="store_true",
        default=False,
        help="Skip CREATE TABLE bootstrap on startup (production: run `interloper db init` once instead)",
    )
    parser.set_defaults(handler=_cmd_api)


def _cmd_api(args: argparse.Namespace) -> None:
    """Serve the API until a signal or its death.

    Args:
        args: Parsed CLI arguments: ``host``, ``port`` and ``no_create_tables``.
    """
    settings = AppSettings.get()
    if args.host is not None:
        settings = settings.model_copy(update={"server": settings.server.model_copy(update={"host": args.host})})
        AppSettings.activate(settings)
    configure_console_logging(
        logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=settings.log.format
    )
    bootstrap(
        settings.postgres.dsn,
        statement_timeout=settings.postgres.statement_timeout,
        create_tables=not args.no_create_tables,
    )
    catalog = Catalog.from_settings()
    store = Store.from_settings(catalog=catalog)
    port = settings.server.port if args.port is None else args.port
    Supervisor({"api": ApiServer.from_settings(settings, store=store, catalog=catalog, port=port)}).run()


def register_oauth(subparsers: argparse._SubParsersAction) -> None:
    """Register the ``oauth`` command group.

    A group of its own rather than subcommands of ``api``: ``api`` is a serve
    command with serving flags, and these act on the database and exit.

    Args:
        subparsers: The root subparsers action to attach to.
    """
    parser = subparsers.add_parser(
        "oauth",
        help="Manage operator-registered OAuth clients",
        description="Manage the OAuth clients an operator registers for apps that cannot register themselves "
        "(a Slack app, a shared connector). Clients that register dynamically are not listed or deleted here.",
    )
    commands = parser.add_subparsers(dest="oauth_command")

    create = commands.add_parser(
        "create",
        help="Register a confidential client and print its id and secret",
        description="Register a confidential client. The secret is printed once and cannot be recovered.",
    )
    create.add_argument("--name", required=True, help="Name shown on the consent page and in Authorised apps")
    create.add_argument(
        "--redirect-uri",
        action="append",
        required=True,
        metavar="URI",
        help="Exact URI a code may be sent to; repeat for several",
    )
    create.set_defaults(handler=_cmd_oauth_create)

    list_parser = commands.add_parser("list", help="List operator-registered clients")
    list_parser.set_defaults(handler=_cmd_oauth_list)

    delete = commands.add_parser(
        "delete", help="Delete a client and revoke every token it holds", description="Delete a client by id."
    )
    delete.add_argument("client_id", help="The client id `oauth list` shows")
    delete.set_defaults(handler=_cmd_oauth_delete)

    parser.set_defaults(handler=lambda args: parser.print_help())


def _cmd_oauth_create(args: argparse.Namespace) -> None:
    """Register an operator client and print its credentials.

    Args:
        args: Parsed CLI arguments: ``name`` and ``redirect_uri`` (a list).

    Raises:
        SystemExit: When a redirect URI is one dynamic registration would refuse.
    """
    for uri in args.redirect_uri:
        if not Client.is_safe_redirect_uri(uri):
            raise SystemExit(
                f"Refusing redirect URI {uri!r}: it must use https, http on a loopback host, or a private-use scheme."
            )
    client, secret = Store.from_settings(catalog=Catalog()).oauth.register_client(
        args.name, args.redirect_uri, registered_by="operator", confidential=True
    )
    print(f"Registered {client.name}.")
    print(f"client_id:     {client.client_id}")
    print(f"client_secret: {secret}")
    print("Store the secret now: it is not stored and cannot be shown again.")


def _cmd_oauth_list(args: argparse.Namespace) -> None:
    """Print the operator clients, one per line.

    Args:
        args: Parsed CLI arguments; this subcommand takes none of its own.
    """
    for client in Store.from_settings(catalog=Catalog()).oauth.list_clients(registered_by="operator"):
        print(f"{client.client_id}  {client.name}  {' '.join(client.redirect_uris)}")


def _cmd_oauth_delete(args: argparse.Namespace) -> None:
    """Delete an operator client.

    Args:
        args: Parsed CLI arguments: ``client_id``.

    Raises:
        SystemExit: When no operator client has that id.
    """
    oauth = Store.from_settings(catalog=Catalog()).oauth
    client = oauth.get_client(args.client_id)
    if client is None or client.registered_by != "operator":
        raise SystemExit(f"No operator client {args.client_id}.")
    oauth.delete_client(client.client_id)
    print(f"Deleted {client.name} ({client.client_id}) and revoked its tokens.")
