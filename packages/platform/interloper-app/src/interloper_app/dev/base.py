"""``interloper dev``: serve the project in the current directory with the real app, signed in, reloading on edits.

One command for every local case: classes in a plain project, an extension
with its routes and UI, the platform itself. What the project holds is
served from source and reloads; everything else runs as installed.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import webbrowser
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING

from interloper.cli import Command
from interloper.settings import ServerSettings
from watchfiles import watch

from interloper_app.dev.nuxt import NuxtServer
from interloper_app.dev.port import Port
from interloper_app.dev.project import MANIFEST, DevProject
from interloper_app.dev.vite import ViteServer

if TYPE_CHECKING:
    from interloper_app.dev.platform import Platform

logger = logging.getLogger(__name__)


class DevCommand(Command):
    """Serve the project in this directory on 127.0.0.1, signed in, restarting on edits.

    When the project holds the app's own source (the platform's repository),
    the app is served from the Nuxt dev server, with hot reload. Classes
    named as targets join the catalog. Instances named as targets, or spec
    files passed with -f, put dev in file mode: the collection is served from
    them alone, read-only in the app, from a database of its own; without
    them, the collection is the one made in the app. The extensions the
    project holds are served from source, their UIs from their Vite dev
    servers. The platform restarts when a Python file, a manifest, a spec file
    or interloper.yaml changes; open the printed link to sign in. Its state is
    kept in SQLite files in .interloper/, unless interloper.yaml or the
    environment configures a Postgres.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the targets, the port and whether to open a browser.

        Args:
            parser: The command's own parser.
        """
        parser.add_argument(
            "targets",
            nargs="*",
            metavar="TARGET",
            help="A component class to add to the catalog (weather.OpenMeteo), or a component instance to serve "
            "the collection from, read-only in the app (weather.paris)",
        )
        parser.add_argument(
            "-f",
            "--file",
            dest="files",
            action="append",
            default=[],
            metavar="FILE",
            help="A spec file to serve the collection from, read-only in the app (repeatable)",
        )
        parser.add_argument("--port", type=int, default=None, help="Port on 127.0.0.1 (default: server.port, 3000)")
        parser.add_argument(
            "--no-browser", action="store_true", default=False, help="Print the sign-in link without opening it"
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Print debug lines, every request, and the info lines of libraries and of Nuxt and Vite",
        )

    def run(self, args: argparse.Namespace) -> None:
        """Serve the project until Ctrl-C, a terminal closed or a termination signal.

        Args:
            args: The parsed command line: ``targets``, ``files``, ``port``, ``no_browser`` and ``verbose``.

        Raises:
            SystemExit: On Windows, when the port is taken or the database cannot be brought up.
        """
        from interloper.cli.console import configure_console_logging
        from interloper.settings import LogSettings

        from interloper_app.dev.platform import Platform, PlatformSpec
        from interloper_app.dev.signin import SignIn

        if sys.platform == "win32":
            raise SystemExit("Error: interloper dev runs on macOS and Linux; on Windows, run it inside WSL.")
        files = [Path(file).resolve() for file in args.files]
        if missing := [str(file) for file in files if not file.is_file()]:
            raise SystemExit(f"Error: no such spec file: {', '.join(missing)}")
        project = DevProject.discover(Path.cwd(), files)
        configure_console_logging(
            logging.DEBUG if args.verbose else logging.INFO, log_format=LogSettings.get().format, project=project.root
        )
        port = Port(args.port or ServerSettings.get().port)
        if port.in_use:
            raise SystemExit(f"Error: port {port.number} is already in use. Stop what listens on it, or pass --port.")
        self._check_database(project.root)
        extensions = [extension for extension in project.extensions() if extension.ui_directory is not None]
        uis = [
            ViteServer(extension.key, directory, pnpm=project.pnpm_workspace)
            for extension in extensions
            if (directory := extension.ui_directory) is not None
        ]
        sign_in = SignIn.generate(project.name, extensions[0].key if len(extensions) == 1 else None)
        app_source = project.app_source
        nuxt = None
        api_port = port
        if app_source is not None:
            api_port = Port.free()
            nuxt = NuxtServer(app_source, port=port, api_port=api_port, ui_entries={})
        platform = Platform(
            PlatformSpec(
                port=api_port.number,
                spa=nuxt is None,
                public_url=port.origin,
                parent=os.getpid(),
                root=str(project.root),
                targets=args.targets,
                files=[str(file) for file in files],
                sign_in=sign_in,
                verbose=args.verbose,
            )
        )
        for signal_number in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signal_number, self._interrupt)
        try:
            for ui in uis:
                ui.start()
            platform.spec.ui_entries = {ui.key: ui.entry for ui in uis}
            if nuxt is not None:
                nuxt.ui_entries = platform.spec.ui_entries
                nuxt.start()
            self._serve(project, platform, nuxt, uis, link=sign_in.link(port.number), browser=not args.no_browser)
        except KeyboardInterrupt:
            logger.info("Stopping")
        finally:
            # A second Ctrl-C must not cut the stop short and leave a server running.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            platform.stop()
            if nuxt is not None:
                nuxt.stop()
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
    def _check_database(root: Path) -> None:
        """Bring the databases dev keeps up, their tables created, or say where they were looked for.

        Args:
            root: The project root, whose ``.interloper/`` holds the databases when no Postgres is configured.
        """
        from interloper_app.dev.database import DevDatabase

        DevDatabase.from_settings(root).prepare()

    @classmethod
    def _serve(
        cls,
        project: DevProject,
        platform: Platform,
        nuxt: NuxtServer | None,
        uis: list[ViteServer],
        *,
        link: str,
        browser: bool,
    ) -> None:
        """Run the platform, then restart it on every watched edit, until interrupted.

        A platform that fails to start (an error in a held file) leaves
        ``dev`` running: the next edit starts it again. The browser opens the
        first time the platform answers. A Nuxt dev server that stops stops
        ``dev``: nothing would serve the app any more.

        Args:
            project: The project, which says which edits restart the platform.
            platform: The platform process.
            nuxt: The Nuxt dev server serving the app, when the project holds
                its source.
            uis: The Vite servers of the held UIs, kept in step with the
                manifests: a UI added or removed while ``dev`` runs starts or
                stops its server.
            link: The sign-in link.
            browser: Whether to open the link once the platform first answers.

        Raises:
            SystemExit: When the Nuxt dev server stops.
        """
        logger.debug("Serving %s from %s", project.name, project.root)
        platform.start()
        browser = cls._announce(platform, link, browser=browser)
        for changes in watch(*project.watched, watch_filter=project.watches, yield_on_timeout=True, rust_timeout=2000):
            if nuxt is not None and not nuxt.running:
                raise SystemExit("Error: the app's Nuxt dev server stopped (see its output above).")
            if not changes:
                continue
            edited = ", ".join(sorted({str(Path(path).relative_to(project.root)) for _, path in changes}))
            logger.info("%s changed: restarting", edited)
            if any(Path(path).name == MANIFEST for _, path in changes):
                cls._refresh_uis(project, platform, nuxt, uis)
            platform.restart()
            browser = cls._announce(platform, link, browser=browser)

    @staticmethod
    def _refresh_uis(project: DevProject, platform: Platform, nuxt: NuxtServer | None, uis: list[ViteServer]) -> None:
        """Start the Vite server of each UI a manifest now declares, and stop those no manifest declares any more.

        A UI whose server fails to start (its install fails, Node is missing)
        is left out, and ``dev`` keeps serving the rest. A manifest that does
        not read, a save caught mid-edit, changes nothing until one that does.

        Args:
            project: The project.
            platform: The platform process, whose spec carries the UI entries.
            nuxt: The Nuxt dev server, restarted with the new entries when the
                set changed.
            uis: The running Vite servers, updated in place.
        """
        if unread := project.unread():
            logger.warning("The manifest of %s does not read: its UI stays as it is.", ", ".join(unread))
            return
        declared = {
            extension.key: directory
            for extension in project.extensions()
            if (directory := extension.ui_directory) is not None
        }
        running = {ui.key for ui in uis}
        for ui in [ui for ui in uis if ui.key not in declared]:
            ui.stop()
            uis.remove(ui)
        for key in declared.keys() - running:
            ui = ViteServer(key, declared[key], pnpm=project.pnpm_workspace)
            try:
                ui.start()
            except SystemExit as error:
                logger.warning("%s The next save of its manifest tries again.", error)
                continue
            uis.append(ui)
        entries = {ui.key: ui.entry for ui in uis}
        if entries == platform.spec.ui_entries:
            return
        platform.spec.ui_entries = entries
        if nuxt is not None:
            nuxt.ui_entries = entries
            nuxt.stop()
            nuxt.start()

    @staticmethod
    def _announce(platform: Platform, link: str, *, browser: bool) -> bool:
        """Wait for the platform to answer, and open the sign-in link the first time.

        The platform logs the ready line, with the link; a platform that does
        not answer is an error line here.

        Args:
            platform: The platform process.
            link: The sign-in link.
            browser: Whether the link is still to be opened.

        Returns:
            Whether it is still to be opened: until the platform first answers.
        """
        if not platform.wait_ready():
            logger.error("The platform did not start: fix the error above and save to restart.")
            return browser
        if browser:
            webbrowser.open(link)
        return False
