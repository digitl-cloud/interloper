"""Tests for ``interloper.utils.supervisor``."""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from collections.abc import Iterator

import pytest

from interloper.utils import ReadyLine, Supervisor


class Loop:
    """A service that loops until stopped, ready once it runs, recording its lifecycle."""

    def __init__(self, ready_after: float = 0.0) -> None:
        """Start unstopped.

        Args:
            ready_after: Seconds its loop runs before it answers.
        """
        self.stopped = threading.Event()
        self.ready_after = ready_after
        self.ready_at: float | None = None

    @property
    def ready(self) -> bool:
        """Whether it has run for ``ready_after`` seconds.

        Returns:
            True once it answers.
        """
        return self.ready_at is not None and time.monotonic() >= self.ready_at

    def start(self) -> None:
        """Block until stopped."""
        self.ready_at = time.monotonic() + self.ready_after
        self.stopped.wait()

    def stop(self) -> None:
        """Unblock ``start``."""
        self.stopped.set()


class Exits:
    """A service whose blocking call returns on its own, never ready."""

    ready = False

    def start(self) -> None:
        """Return at once."""

    def stop(self) -> None:
        """Nothing to stop."""


class Raises:
    """A service whose blocking call raises, never ready."""

    ready = False

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

    ready = False

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


def _ready_line(**overrides: object) -> ReadyLine:
    fields: dict = {
        "logger": logging.getLogger("interloper_app.cli"),
        "started": time.time(),
        "services": ["api", "cron"],
        "definitions": 88,
        "url": "http://127.0.0.1:3000",
    } | overrides
    return ReadyLine(**fields)


def _ready_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.getMessage().startswith("Ready in")]


def test_the_ready_line_follows_every_service_being_ready(caplog: pytest.LogCaptureFixture):
    """The ready line is logged once, after the slowest service answers."""
    caplog.set_level(logging.INFO)
    quick, slow = Loop(), Loop(ready_after=0.3)
    supervisor = Supervisor({"api": slow, "cron": quick}, grace=1.0, ready=_ready_line())
    threading.Timer(0.8, supervisor.stop, args=["test"]).start()

    supervisor.run()

    (record,) = _ready_records(caplog)
    assert slow.ready_at is not None and record.created >= time.time() - 0.8
    assert record.levelno == logging.INFO
    assert record.getMessage().endswith(" on http://127.0.0.1:3000\napi, cron · 88 definitions")


def test_no_ready_line_when_a_service_dies_before_it_is_ready(caplog: pytest.LogCaptureFixture):
    """A process that fails to start never says it is ready."""
    caplog.set_level(logging.INFO)
    supervisor = Supervisor({"loop": Loop(), "api": Raises()}, grace=1.0, ready=_ready_line())

    with pytest.raises(SystemExit):
        supervisor.run()

    assert _ready_records(caplog) == []


def test_starting_the_services_is_a_debug_line(caplog: pytest.LogCaptureFixture):
    """The services starting is not news at INFO: the ready line is."""
    caplog.set_level(logging.DEBUG)
    supervisor = Supervisor({"loop": Loop()}, grace=1.0)
    threading.Timer(0.2, supervisor.stop, args=["test"]).start()

    supervisor.run()

    (record,) = [record for record in caplog.records if record.getMessage().startswith("Started services")]
    assert record.levelno == logging.DEBUG


class TestReadyLine:
    def test_it_names_the_time_to_start_the_url_the_services_and_the_catalog(self, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.INFO)

        _ready_line(started=time.time() - 4.42).log()

        (record,) = caplog.records
        assert record.name == "interloper_app.cli"
        assert record.getMessage() == "Ready in 4.4s on http://127.0.0.1:3000\napi, cron · 88 definitions"

    def test_without_a_url_it_names_none(self, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.INFO)

        _ready_line(started=time.time() - 1.02, url=None, services=["cron", "worker"]).log()

        assert caplog.records[0].getMessage() == "Ready in 1.0s\ncron, worker · 88 definitions"
