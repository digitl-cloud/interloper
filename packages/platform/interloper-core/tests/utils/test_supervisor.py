"""Tests for ``interloper.utils.supervisor``."""

from __future__ import annotations

import os
import signal
import threading
import time
from collections.abc import Iterator

import pytest

from interloper.utils import Supervisor


class Loop:
    """A service that loops until stopped, recording its lifecycle."""

    def __init__(self) -> None:
        """Start unstopped."""
        self.stopped = threading.Event()

    def start(self) -> None:
        """Block until stopped."""
        self.stopped.wait()

    def stop(self) -> None:
        """Unblock ``start``."""
        self.stopped.set()


class Exits:
    """A service whose blocking call returns on its own."""

    def start(self) -> None:
        """Return at once."""

    def stop(self) -> None:
        """Nothing to stop."""


class Raises:
    """A service whose blocking call raises."""

    def start(self) -> None:
        """Fail at once.

        Raises:
            OSError: Always, as a server failing to bind its port would.
        """
        raise OSError("address already in use")

    def stop(self) -> None:
        """Nothing to stop."""


@pytest.fixture(autouse=True)
def _restore_signal_handlers() -> Iterator[None]:
    """Put back the handlers the supervisor installs, so later tests keep pytest's."""
    saved = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    yield
    for number, handler in saved.items():
        signal.signal(number, handler)


def test_stop_stops_every_service_and_returns():
    """``stop`` ends ``run`` with every service stopped."""
    first, second = Loop(), Loop()
    supervisor = Supervisor({"first": first, "second": second}, grace=1.0)
    threading.Timer(0.2, supervisor.stop, args=["test"]).start()

    supervisor.run()

    assert first.stopped.is_set() and second.stopped.is_set()


def test_sigterm_stops_every_service_and_returns():
    """SIGTERM is a clean shutdown: every service stops and ``run`` returns normally."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop}, grace=1.0)
    threading.Timer(0.2, os.kill, args=[os.getpid(), signal.SIGTERM]).start()

    supervisor.run()

    assert loop.stopped.is_set()


def test_a_service_exiting_stops_the_rest_and_fails():
    """A service that returns on its own takes the process down, with a failure status."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop, "exits": Exits()}, grace=1.0)

    with pytest.raises(SystemExit) as error:
        supervisor.run()

    assert error.value.code == 1
    assert loop.stopped.is_set()


def test_a_service_raising_on_start_stops_the_process_with_failure(caplog: pytest.LogCaptureFixture):
    """A service that raises is logged by name and fails the process, within the grace period."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop, "api": Raises()}, grace=1.0)

    started = time.monotonic()
    with pytest.raises(SystemExit):
        supervisor.run()

    assert time.monotonic() - started < 3
    assert any("'api'" in record.getMessage() for record in caplog.records)


def test_stop_is_idempotent():
    """A second ``stop`` is ignored."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop}, grace=1.0)
    threading.Timer(0.2, supervisor.stop, args=["first"]).start()
    threading.Timer(0.3, supervisor.stop, args=["second"]).start()

    supervisor.run()

    assert loop.stopped.is_set()


class ExitsTheProcess:
    """A service whose blocking call calls ``sys.exit``, as uvicorn does when it cannot bind its port."""

    def start(self) -> None:
        """Exit at once.

        Raises:
            SystemExit: Always, with uvicorn's startup-failure status.
        """
        raise SystemExit(3)

    def stop(self) -> None:
        """Nothing to stop."""


def test_a_service_calling_sys_exit_stops_the_process_with_failure():
    """``sys.exit`` inside a service ends its thread; it is a death, not a hang."""
    loop = Loop()
    supervisor = Supervisor({"loop": loop, "api": ExitsTheProcess()}, grace=1.0)
    outcome: list[object] = []

    def run() -> None:
        try:
            supervisor.run()
        except SystemExit as error:
            outcome.append(error.code)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=5)

    assert not thread.is_alive(), "the supervisor hung on a service that called sys.exit"
    assert outcome == [1]
    assert loop.stopped.is_set()
