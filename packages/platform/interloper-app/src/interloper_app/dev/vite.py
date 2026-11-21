"""The Vite dev server of a held extension UI, which the page imports the UI from while ``dev`` runs.

The server outlives the platform's restarts: a ``.vue`` or stylesheet edit is
Vite's to apply, a Python edit is the platform's.
"""

from __future__ import annotations

import time
from pathlib import Path

from interloper_app.dev.port import Port
from interloper_app.dev.process import ProcessGroup


class ViteServer:
    """One extension UI's Vite dev server, on a free port of ``127.0.0.1``, until stopped.

    Attributes:
        key: The extension's key, which the API lists the UI under.
        directory: The UI's source, holding its ``package.json``.
        pnpm: Whether the UI belongs to a pnpm workspace; npm runs it otherwise.
        port: The port it serves on, picked when it starts.
    """

    def __init__(self, key: str, directory: Path, *, pnpm: bool) -> None:
        """Prepare the server; nothing starts until `start`.

        Args:
            key: The extension's key.
            directory: The UI's source.
            pnpm: Whether to install and run it with pnpm rather than npm.
        """
        self.key = key
        self.directory = directory
        self.pnpm = pnpm
        self.port = Port(0)
        self._group = ProcessGroup()

    @property
    def origin(self) -> str:
        """The server's origin, which the page's import map names its shared modules under.

        Returns:
            ``http://127.0.0.1:<port>``.
        """
        return self.port.origin

    @property
    def entry(self) -> str:
        """The UI's entry module on the server, which the API lists instead of a bundle.

        Returns:
            The URL of ``src/index.ts``, the entry ``interloper extension add ui`` writes.
        """
        return f"{self.origin}/src/index.ts"

    def install_command(self) -> list[str]:
        """Spell the command installing the UI's dependencies.

        Returns:
            ``pnpm install`` inside a pnpm workspace, ``npm install`` otherwise.
        """
        return ["pnpm", "install"] if self.pnpm else ["npm", "install", "--no-audit", "--no-fund"]

    def command(self) -> list[str]:
        """Spell the command running the server.

        It keeps the terminal's history (``--clearScreen false``) and fails
        rather than move when its port is taken.

        Returns:
            The command, Vite run through the UI's own package manager.
        """
        runner = ["pnpm", "exec"] if self.pnpm else ["npm", "exec", "--no", "--"]
        return [
            *runner,
            "vite",
            "--host",
            Port.HOST,
            "--port",
            str(self.port.number),
            "--strictPort",
            "--clearScreen",
            "false",
        ]

    def start(self, timeout: float = 60.0) -> None:
        """Install the dependencies when missing, start the server and wait until it answers.

        Args:
            timeout: Seconds to wait for the server.

        Raises:
            SystemExit: When Node is missing, the install fails, or the server
                exits or stays silent past the timeout.
        """
        if not (self.directory / "node_modules").is_dir():
            self._start(self.install_command())
            if self._group.wait() != 0:
                raise SystemExit(f"`{' '.join(self.install_command())}` failed in {self.directory}.")
        self.port = Port.free()
        self._start(self.command())
        deadline = time.monotonic() + timeout
        while not self.port.answers:
            if not self._group.running or time.monotonic() > deadline:
                self.stop()
                raise SystemExit(f"The {self.key} UI's Vite dev server did not start (see its output above).")
            time.sleep(0.2)

    def stop(self) -> None:
        """Stop the server, or the install still running, and everything either started."""
        self._group.stop(timeout=5.0)

    # -- Internals -------------------------------------------------------------

    def _start(self, command: list[str]) -> None:
        """Start a package manager command in the UI's directory.

        Args:
            command: The command.

        Raises:
            SystemExit: When its program is not installed.
        """
        try:
            self._group.start(command, cwd=self.directory)
        except FileNotFoundError as error:
            raise SystemExit(f"The {self.key} UI needs Node.js: {command[0]} was not found.") from error
