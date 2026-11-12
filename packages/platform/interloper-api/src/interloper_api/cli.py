"""``interloper api`` serves the HTTP API; ``interloper oauth`` registers its OAuth clients.

``oauth`` is a command of its own rather than a group under ``api``: ``api``
serves, with serving flags, while these act on the database and exit.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Mapping
from typing import ClassVar

from authlib.oauth2.rfc7591 import InvalidRedirectURIError
from interloper.catalog import Catalog
from interloper.cli import Command
from interloper.cli.console import configure_console_logging
from interloper.settings import LogSettings, ServerSettings
from interloper.utils import Supervisor
from interloper_db import Store
from interloper_db.provision import bootstrap
from interloper_db.settings import PostgresSettings

from interloper_api.authorization import Client
from interloper_api.server import ApiServer
from interloper_api.settings import ApiSettings


class ApiCommand(Command):
    """Serve the HTTP API.

    The frontend is served separately: by nginx in containers, by `interloper app` locally.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare where to bind and whether to bootstrap the tables.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("--host", default=None, help="Bind host (default: server.host, 0.0.0.0)")
        parser.add_argument("--port", type=int, default=None, help="Bind port (default: server.port, 3000)")
        parser.add_argument(
            "--no-create-tables",
            action="store_true",
            default=False,
            help="Skip CREATE TABLE bootstrap on startup (production: run `interloper db init` once instead)",
        )

    def run(self, args: argparse.Namespace) -> None:
        """Serve the API until a signal or its death.

        Args:
            args: The parsed command line: ``host``, ``port`` and ``no_create_tables``.
        """
        if args.host is not None:
            ServerSettings.activate(ServerSettings.get().model_copy(update={"host": args.host}))
        settings = ApiSettings.get()
        configure_console_logging(
            logging.INFO, "%(asctime)s %(name)s %(levelname)s %(message)s", log_format=LogSettings.get().format
        )
        postgres = PostgresSettings.get()
        bootstrap(postgres.dsn, statement_timeout=postgres.statement_timeout, create_tables=not args.no_create_tables)
        catalog = Catalog.from_settings()
        store = Store.from_settings(
            catalog=catalog,
            session_expiry=settings.auth.session_expiry,
            access_token_expiry=settings.auth.access_token_expiry,
            refresh_token_expiry=settings.auth.refresh_token_expiry,
        )
        port = settings.server.port if args.port is None else args.port
        Supervisor({"api": ApiServer.from_settings(settings, store=store, catalog=catalog, port=port)}).run()


class OAuthCreateCommand(Command):
    """Register a confidential client and print its id and secret.

    The secret is printed once and cannot be recovered.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the client's name and redirect URIs.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("--name", required=True, help="Name shown on the consent page and in Authorised apps")
        parser.add_argument(
            "--redirect-uri",
            action="append",
            required=True,
            metavar="URI",
            help="Exact URI a code may be sent to; repeat for several",
        )

    def run(self, args: argparse.Namespace) -> None:
        """Register an operator client and print its credentials.

        Args:
            args: The parsed command line: ``name`` and ``redirect_uri`` (a list).

        Raises:
            SystemExit: When a redirect URI is one dynamic registration would refuse.
        """
        try:
            Client.check_redirect_uris(args.redirect_uri)
        except InvalidRedirectURIError as error:
            raise SystemExit(error.description) from error
        client, secret = Store.from_settings(catalog=Catalog()).oauth.register_client(
            args.name, args.redirect_uri, registered_by="operator", confidential=True
        )
        print(f"Registered {client.name}.")
        print(f"client_id:     {client.client_id}")
        print(f"client_secret: {secret}")
        print("Store the secret now: it is not stored and cannot be shown again.")


class OAuthListCommand(Command):
    """List operator-registered clients."""

    def run(self, args: argparse.Namespace) -> None:
        """Print the operator clients, one per line.

        Args:
            args: The parsed command line; this command takes no arguments.
        """
        for client in Store.from_settings(catalog=Catalog()).oauth.list_clients(registered_by="operator"):
            print(f"{client.client_id}  {client.name}  {' '.join(client.redirect_uris)}")


class OAuthDeleteCommand(Command):
    """Delete a client and revoke every token it holds."""

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the client to delete.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("client_id", help="The client id `oauth list` shows")

    def run(self, args: argparse.Namespace) -> None:
        """Delete an operator client.

        Args:
            args: The parsed command line: ``client_id``.

        Raises:
            SystemExit: When no operator client has that id.
        """
        client = Store.from_settings(catalog=Catalog()).oauth.delete_client(args.client_id, registered_by="operator")
        if client is None:
            raise SystemExit(f"No operator client {args.client_id}.")
        print(f"Deleted {client.name} ({client.client_id}) and revoked its tokens.")


class OAuthRotateCommand(Command):
    """Replace a client's secret and print the new one.

    The old secret stops working at once; tokens the client holds are untouched, so no user has to authorise it
    again. The new secret is printed once and cannot be recovered.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the client whose secret to replace.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument("client_id", help="The client id `oauth list` shows")

    def run(self, args: argparse.Namespace) -> None:
        """Replace an operator client's secret and print the new one.

        Args:
            args: The parsed command line: ``client_id``.

        Raises:
            SystemExit: When no operator client has that id.
        """
        rotated = Store.from_settings(catalog=Catalog()).oauth.rotate_secret(args.client_id, registered_by="operator")
        if rotated is None:
            raise SystemExit(f"No operator client {args.client_id}.")
        client, secret = rotated
        print(f"Rotated the secret of {client.name}; the old one no longer works.")
        print(f"client_id:     {client.client_id}")
        print(f"client_secret: {secret}")
        print("Store the secret now: it is not stored and cannot be shown again.")


class OAuthCommand(Command):
    """Manage operator-registered OAuth clients.

    These are the clients an operator registers for apps that cannot register themselves (a Slack app, a shared
    connector), as the admin portal's OAuth clients page does. Clients that register dynamically are not listed or
    deleted here.
    """

    commands: ClassVar[Mapping[str, type[Command]]] = {
        "create": OAuthCreateCommand,
        "list": OAuthListCommand,
        "delete": OAuthDeleteCommand,
        "rotate": OAuthRotateCommand,
    }
