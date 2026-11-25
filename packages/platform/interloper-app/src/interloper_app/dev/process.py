"""A process ``interloper dev`` starts, in a process group of its own so that stopping it stops everything it started.

The terminal's Ctrl-C reaches only ``dev``, which then stops each group in
turn: a package manager's children, Vite's workers and the platform's
services all go with the process that started them. A process given a
source has its output read and logged in the console's one format.
"""

from __future__ import annotations

import logging
import os
import re
import signal
import subprocess
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import IO, ClassVar


class ProcessOutput:
    """A process's two streams, read line by line and logged under one source until they close.

    What a tool writes for a person is logged at INFO, what it writes to
    stderr (its warnings and errors) at WARNING; the console shows the
    first only with ``-v``. Colour codes and overwritten progress go.
    """

    ESCAPES: ClassVar[re.Pattern[str]] = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")

    def __init__(self, process: subprocess.Popen[bytes], source: str) -> None:
        """Start reading both streams, each on a thread of its own.

        Args:
            process: The process, started with both streams piped.
            source: The logger's name, which the console shows as the source (``nuxt``).
        """
        self._logger = logging.getLogger(source)
        self._readers = [
            threading.Thread(target=self._read, args=(stream, level), daemon=True)
            for stream, level in ((process.stdout, logging.INFO), (process.stderr, logging.WARNING))
            if stream is not None
        ]
        for reader in self._readers:
            reader.start()

    def join(self, timeout: float = 2.0) -> None:
        """Wait for what the streams still hold to be logged.

        Args:
            timeout: Seconds each reader gets once the process ended.
        """
        for reader in self._readers:
            reader.join(timeout)

    def _read(self, stream: IO[bytes], level: int) -> None:
        """Log each non-empty line of a stream until it closes.

        Args:
            stream: The stream.
            level: The level its lines are logged at.
        """
        with stream:
            for raw in iter(stream.readline, b""):
                line = self.ESCAPES.sub("", raw.decode("utf-8", errors="replace").rstrip("\r\n")).split("\r")[-1]
                if line.strip():
                    self._logger.log(level, "%s", line.rstrip())


class ProcessGroup:
    """One command at a time, run in a process group of its own and stopped as a whole."""

    def __init__(self) -> None:
        """Hold no process until `start`."""
        self._process: subprocess.Popen[bytes] | None = None
        self._output: ProcessOutput | None = None

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
        output: str | None = None,
    ) -> None:
        """Start a command, after the previous one ended.

        Args:
            command: The command and its arguments.
            cwd: Where it runs; the current directory when ``None``.
            stdin: Bytes written to its standard input, which is then closed;
                ``None`` gives it none.
            environment: Its environment; this process's when ``None``.
            output: The source its output is logged under (see
                `ProcessOutput`); ``None`` leaves it the terminal's.
        """
        piped = subprocess.PIPE if output is not None else None
        self._process = subprocess.Popen(
            command,
            cwd=cwd,
            env=environment,
            stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
            stdout=piped,
            stderr=piped,
            start_new_session=True,
        )
        self._output = ProcessOutput(self._process, output) if output is not None else None
        if stdin is not None and self._process.stdin is not None:
            with self._process.stdin as pipe:
                pipe.write(stdin)

    def wait(self) -> int:
        """Wait for the command to end, and for what it wrote to be logged.

        Returns:
            Its exit code; 0 when nothing was started.
        """
        if self._process is None:
            return 0
        code = self._process.wait()
        if self._output is not None:
            self._output.join()
        return code

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the command and everything it started, killing what outlives the timeout.

        Args:
            timeout: Seconds the group gets to stop after ``SIGTERM``.
        """
        process, self._process = self._process, None
        if process is None:
            return
        self._signal(process, signal.SIGTERM)
        deadline = time.monotonic() + timeout
        while self._alive(process) and time.monotonic() < deadline:
            time.sleep(0.05)
        if self._alive(process):
            self._signal(process, signal.SIGKILL)
            process.wait()
        if self._output is not None:
            self._output.join()

    @staticmethod
    def _alive(process: subprocess.Popen[bytes]) -> bool:
        """Whether the command, or anything in its group, still runs.

        A child can outlive the command it was started by, and hold what the
        command held (a port) until it ends.

        Args:
            process: The process leading the group.

        Returns:
            True while the leader runs or the group has a member left.
        """
        if process.poll() is None:
            return True
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return False
        return True

    @staticmethod
    def _signal(process: subprocess.Popen[bytes], signal_number: int) -> None:
        """Signal a process's whole group, which may have ended in the meantime.

        Args:
            process: The process leading the group.
            signal_number: The signal.
        """
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal_number)
