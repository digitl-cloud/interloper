"""``interloper dev``: serve the project in the current directory with the real app, signed in, reloading on edits.

One command for every local case: classes in a plain project, an extension
with its routes and UI, the platform itself. What the project holds is
served from source and reloads; everything else runs as installed.
"""

from __future__ import annotations

import argparse
import itertools
import os
import signal
import sys
import webbrowser
from pathlib import Path
from types import FrameType

from interloper.cli import Command
from interloper.settings import ServerSettings
from interloper_db.provision import bootstrap
from interloper_db.settings import PostgresSettings
from sqlalchemy.exc import OperationalError
from watchfiles import watch

from interloper_app.dev.platform import Platform, PlatformSpec
from interloper_app.dev.project import DevProject
from interloper_app.dev.signin import SignIn
from interloper_app.dev.vite import ViteServer
from interloper_app.services import DevServer


class DevCommand(Command):
    """Serve the project in this directory on 127.0.0.1, signed in, restarting on edits.

    Classes named as targets join the catalog. The extensions the project
    holds are served from source, their UIs from their Vite dev servers. The
    platform restarts when a Python file, a manifest or interloper.yaml
    changes; open the printed link to sign in.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the targets, the port and whether to open a browser.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument(
            "targets", nargs="*", metavar="CLASS", help="Component class to add to the catalog (module.Class)"
        )
        parser.add_argument("--port", type=int, default=None, help="Port on 127.0.0.1 (default: server.port, 3000)")
        parser.add_argument(
            "--no-browser", action="store_true", default=False, help="Print the sign-in link without opening it"
        )

    def run(self, args: argparse.Namespace) -> None:
        """Serve the project until Ctrl-C, a terminal closed or a termination signal.

        Args:
            args: The parsed command line: ``targets``, ``port`` and ``no_browser``.

        Raises:
            SystemExit: On Windows, when the port is taken or Postgres does not answer.
        """
        if sys.platform == "win32":
            raise SystemExit("Error: interloper dev runs on macOS and Linux; on Windows, run it inside WSL.")
        project = DevProject.discover(Path.cwd())
        port = args.port or ServerSettings.get().port
        if DevServer.port_in_use(port):
            raise SystemExit(f"Error: port {port} is already in use. Stop what listens on it, or pass --port.")
        self._check_database()
        extensions = [extension for extension in project.extensions() if extension.ui_directory is not None]
        uis = [
            ViteServer(extension.key, directory, pnpm=project.pnpm_workspace)
            for extension in extensions
            if (directory := extension.ui_directory) is not None
        ]
        sign_in = SignIn.generate(project.name, extensions[0].key if len(extensions) == 1 else None)
        platform = Platform(
            PlatformSpec(
                port=port, parent=os.getpid(), python_path=[str(project.root)], classes=args.targets, sign_in=sign_in
            )
        )
        for signal_number in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signal_number, self._interrupt)
        try:
            for ui in uis:
                ui.start()
            platform.spec.ui_entries = {ui.key: ui.entry for ui in uis}
            self._serve(project, platform, link=sign_in.link(port), browser=not args.no_browser)
        except KeyboardInterrupt:
            print("\nStopping.", flush=True)
        finally:
            # A second Ctrl-C must not cut the stop short and leave a server running.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            platform.stop()
            for ui in uis:
                ui.stop()

    # -- Internals -------------------------------------------------------------

    @staticmethod
    def _interrupt(signal_number: int, frame: FrameType | None) -> None:
        """Turn a termination signal or a closed terminal into the Ctrl-C path, which stops everything.

        Args:
            signal_number: The signal received.
            frame: The frame it interrupted.

        Raises:
            KeyboardInterrupt: Always.
        """
        raise KeyboardInterrupt

    @staticmethod
    def _check_database() -> None:
        """Bring the configured database up, its tables created, or say where Postgres was looked for.

        Raises:
            SystemExit: When Postgres does not answer.
        """
        postgres = PostgresSettings.get()
        try:
            bootstrap(postgres.dsn, statement_timeout=postgres.statement_timeout)
        except OperationalError as error:
            raise SystemExit(
                f"Error: Postgres at {postgres.host}:{postgres.port} (database '{postgres.database}') does not "
                f"answer. Start it, or point the postgres section of interloper.yaml at one you run.\n{error.orig}"
            ) from error

    @staticmethod
    def _serve(project: DevProject, platform: Platform, *, link: str, browser: bool) -> None:
        """Run the platform, then restart it on every watched edit, until interrupted.

        A platform that fails to start (an error in a held file) leaves
        ``dev`` running: the next edit starts it again. The browser opens the
        first time the platform answers.

        Args:
            project: The project, which says which edits restart the platform.
            platform: The platform process.
            link: The sign-in link.
            browser: Whether to open the link once the platform first answers.
        """
        print(f"\n  interloper dev is serving {project.name} from {project.root}\n", flush=True)
        platform.start()
        for changes in itertools.chain([set()], watch(project.root, watch_filter=project.watches)):
            if changes:
                edited = ", ".join(sorted({str(Path(path).relative_to(project.root)) for _, path in changes}))
                print(f"\n  {edited} changed: restarting the platform.\n", flush=True)
                platform.restart()
            if not platform.wait_ready():
                print("\n  The platform did not start; fix the error above and save to restart.\n", flush=True)
                continue
            print(f"\n  Ready. Sign in: {link}\n", flush=True)
            if browser:
                webbrowser.open(link)
                browser = False
