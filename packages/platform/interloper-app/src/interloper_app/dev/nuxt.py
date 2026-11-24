"""The Nuxt dev server ``interloper dev`` runs when the project holds the app's own source: the platform's repository.

It serves the app with hot reload on ``127.0.0.1:<port>`` and proxies the
API, which the platform process serves on a free port behind it. Like the
Vite servers, it outlives the platform's restarts. The extension UIs' dev
entries reach it through ``INTERLOPER_UI_DEV``, set only on the process
``dev`` starts: the app's own build never reads it, so no deployment can be
made to import code from elsewhere.
"""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from pathlib import Path

from interloper_app.dev.port import Port
from interloper_app.dev.process import ProcessGroup


class NuxtServer:
    """The app's Nuxt dev server, in front of the platform's API, until stopped.

    Attributes:
        directory: The app's Nuxt source.
        port: The port it serves the app on.
        api_port: The port of the platform's API it proxies to.
        ui_entries: Each held extension UI's entry on its Vite dev server, by key.
    """

    def __init__(self, directory: Path, *, port: Port, api_port: Port, ui_entries: Mapping[str, str]) -> None:
        """Prepare the server; nothing starts until `start`.

        Args:
            directory: The app's Nuxt source.
            port: The port to serve the app on.
            api_port: The port of the platform's API.
            ui_entries: The extension UIs' dev entries, by key.
        """
        self.directory = directory
        self.port = port
        self.api_port = api_port
        self.ui_entries = dict(ui_entries)
        self._group = ProcessGroup()

    @property
    def running(self) -> bool:
        """Whether the server is alive.

        Returns:
            True from `start` until it exits or is stopped.
        """
        return self._group.running

    def environment(self) -> dict[str, str]:
        """Spell the environment the server runs in: this process's, with the ports and the dev entries.

        Returns:
            The environment.
        """
        entries = ",".join(f"{key}={entry}" for key, entry in self.ui_entries.items())
        return {
            **os.environ,
            "INTERLOPER_SERVER_PORT": str(self.port.number),
            "INTERLOPER_API_PORT": str(self.api_port.number),
            "INTERLOPER_UI_DEV": entries,
        }

    def command(self) -> list[str]:
        """Spell the command running the server on the loopback.

        Returns:
            ``pnpm dev`` with the host and port, pnpm saying only its warnings.
        """
        return ["pnpm", "--loglevel", "warn", "dev", "--host", Port.HOST, "--port", str(self.port.number)]

    def start(self, timeout: float = 180.0) -> None:
        """Install the app's dependencies when missing, start the server and wait until it answers.

        Args:
            timeout: Seconds to wait for the first compile.

        Raises:
            SystemExit: When pnpm is missing, the install fails, the port was
                taken meanwhile (Nuxt would move to another one silently), or
                the server exits or stays silent past the timeout.
        """
        if not (self.directory / "node_modules").is_dir():
            self._start(["pnpm", "--loglevel", "warn", "install"])
            if self._group.wait() != 0:
                raise SystemExit(f"`pnpm install` failed in {self.directory}.")
        if self.port.in_use:
            raise SystemExit(
                f"Error: port {self.port.number} was taken meanwhile. Stop what listens on it, or pass --port."
            )
        self._start(self.command(), environment=self.environment())
        deadline = time.monotonic() + timeout
        while not self.port.answers:
            if not self._group.running or time.monotonic() > deadline:
                self.stop()
                raise SystemExit("The app's Nuxt dev server did not start (see its output above).")
            time.sleep(0.5)

    def stop(self) -> None:
        """Stop the server and everything it started."""
        self._group.stop()

    def _start(self, command: list[str], environment: dict[str, str] | None = None) -> None:
        """Start a pnpm command in the app's directory.

        Args:
            command: The command.
            environment: Its environment; this process's when ``None``.

        Raises:
            SystemExit: When pnpm is not installed.
        """
        try:
            self._group.start(command, cwd=self.directory, environment=environment, output="nuxt")
        except FileNotFoundError as error:
            raise SystemExit("Serving the app from its source needs pnpm, which was not found.") from error
