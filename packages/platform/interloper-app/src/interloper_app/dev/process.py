"""A process ``interloper dev`` starts, in a process group of its own so that stopping it stops everything it started.

The terminal's Ctrl-C reaches only ``dev``, which then stops each group in
turn: a package manager's children, Vite's workers and the platform's
services all go with the process that started them.
"""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path


class ProcessGroup:
    """One command at a time, run in a process group of its own and stopped as a whole."""

    def __init__(self) -> None:
        """Hold no process until `start`."""
        self._process: subprocess.Popen[bytes] | None = None

    @property
    def running(self) -> bool:
        """Whether the last command started is alive.

        Returns:
            True from `start` until it exits or is stopped.
        """
        return self._process is not None and self._process.poll() is None

    def start(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        stdin: bytes | None = None,
        environment: dict[str, str] | None = None,
    ) -> None:
        """Start a command, after the previous one ended.

        Args:
            command: The command and its arguments.
            cwd: Where it runs; the current directory when ``None``.
            stdin: Bytes written to its standard input, which is then closed;
                ``None`` gives it none.
            environment: Its environment; this process's when ``None``.
        """
        self._process = subprocess.Popen(
            command,
            cwd=cwd,
            env=environment,
            stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
            start_new_session=True,
        )
        if stdin is not None and self._process.stdin is not None:
            with self._process.stdin as pipe:
                pipe.write(stdin)

    def wait(self) -> int:
        """Wait for the command to end.

        Returns:
            Its exit code; 0 when nothing was started.
        """
        return self._process.wait() if self._process is not None else 0

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the command and everything it started, killing what outlives the timeout.

        Args:
            timeout: Seconds the group gets to stop after ``SIGTERM``.
        """
        process, self._process = self._process, None
        if process is None or process.poll() is not None:
            return
        self._signal(process, signal.SIGTERM)
        try:
            process.wait(timeout)
        except subprocess.TimeoutExpired:
            self._signal(process, signal.SIGKILL)
            process.wait()

    @staticmethod
    def _signal(process: subprocess.Popen[bytes], signal_number: int) -> None:
        """Signal a process's whole group, which may have ended in the meantime.

        Args:
            process: The process leading the group.
            signal_number: The signal.
        """
        try:
            os.killpg(process.pid, signal_number)
        except ProcessLookupError:
            pass
