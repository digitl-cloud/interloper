"""Run long-lived services in threads until a signal or a service's death."""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)


class Service(Protocol):
    """Something a `Supervisor` runs: a blocking ``start``, a ``stop`` that ends it, and whether it answers yet."""

    @property
    def ready(self) -> bool:
        """Whether what ``start`` serves answers: a server once it listens, a loop once it runs."""
        ...

    def start(self) -> None:
        """Run until stopped."""

    def stop(self) -> None:
        """Ask the running ``start`` to return."""


@dataclass(frozen=True)
class ReadyLine:
    """The one line a serving command prints once everything it runs answers.

    ``Ready in 4.4s on http://127.0.0.1:3000``, then the services and the
    catalog's size on a second line. The command builds it, since it knows
    what to name; the `Supervisor` logs it, since it knows when.

    Attributes:
        logger: The command's logger, which names it in the source column.
        started: When the command began, as ``time.time()``; the line says how long ago.
        services: The services to name.
        definitions: How many definitions the catalog holds.
        url: The address to open; ``None`` for a process that serves none.
    """

    logger: logging.Logger
    started: float
    services: Sequence[str]
    definitions: int
    url: str | None = None

    def log(self) -> None:
        """Log the line at INFO, timed from `started`."""
        where = f" on {self.url}" if self.url else ""
        self.logger.info(
            "Ready in %.1fs%s\n%s · %d definitions",
            time.time() - self.started,
            where,
            ", ".join(self.services),
            self.definitions,
        )


class Supervisor:
    """Run services in daemon threads and stop them all together.

    A signal (SIGINT, SIGTERM, SIGHUP) is a clean shutdown. A service whose
    ``start`` returns or raises while nothing asked it to is a death: every
    other service is stopped too and ``run`` fails, so an orchestrator
    restarts the process instead of keeping one that serves nothing.
    """

    def __init__(self, services: Mapping[str, Service], *, grace: float = 5.0, ready: ReadyLine | None = None) -> None:
        """Hold the services to run.

        Args:
            services: The services, by the name their thread and log lines carry.
            grace: Seconds each thread is given to finish once stopped.
            ready: The line to log once every service is ready; ``None`` logs none.
        """
        self.services = dict(services)
        self.grace = grace
        self.ready = ready
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._failed = False

    def run(self) -> None:
        """Start every service, log the ready line once they all answer, and block until they are stopped.

        A process whose services stop before they are all ready never logs it.

        Raises:
            SystemExit: With status 1 when a service died rather than being stopped.
        """
        if threading.current_thread() is threading.main_thread():
            self._install_signal_handlers()
        threads = [
            threading.Thread(target=self._run_service, args=(name, service), name=name, daemon=True)
            for name, service in self.services.items()
        ]
        for thread in threads:
            thread.start()
        logger.debug("Started services: %s", ", ".join(self.services))
        waiting = self.ready
        while not self._stopping.wait(timeout=1 if waiting is None else 0.05):
            if waiting is not None and all(service.ready for service in self.services.values()):
                waiting.log()
                waiting = None
        for thread in threads:
            thread.join(timeout=self.grace)
        logger.info("Shutdown complete.")
        if self._failed:
            raise SystemExit(1)

    def stop(self, reason: str) -> None:
        """Stop every service; later calls are ignored.

        Args:
            reason: Why the shutdown started, logged with it.
        """
        with self._lock:
            if self._stopping.is_set():
                return
            self._stopping.set()
        logger.info("Shutting down (%s)...", reason)
        for name, service in self.services.items():
            try:
                service.stop()
            except Exception:
                logger.exception("Service '%s' failed to stop", name)

    # -- Internals -------------------------------------------------------------

    def _run_service(self, name: str, service: Service) -> None:
        """Run one service, turning any end of it nobody asked for into a shutdown.

        ``BaseException`` because a server may end its own thread with
        ``sys.exit`` (uvicorn does on a port it cannot bind); an end that comes
        after a stop began is the shutdown itself, not a death.

        Args:
            name: The service's name.
            service: The service.
        """
        try:
            service.start()
        except BaseException:
            if self._stopping.is_set():
                logger.exception("Service '%s' failed while stopping", name)
                return
            logger.exception("Service '%s' failed", name)
            self._die(name)
            return
        if not self._stopping.is_set():
            logger.error("Service '%s' exited unexpectedly", name)
            self._die(name)

    def _die(self, name: str) -> None:
        """Record a service's death and stop the rest, unless a stop already began.

        Args:
            name: The service that died.
        """
        with self._lock:
            if self._stopping.is_set():
                return
            self._failed = True
        self.stop(f"service '{name}' died")

    def _install_signal_handlers(self) -> None:
        """Route SIGINT, SIGTERM and (where available) SIGHUP into ``stop``."""
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        if hasattr(signal, "SIGHUP"):
            signal.signal(signal.SIGHUP, self._on_signal)

    def _on_signal(self, signal_number: int, frame: object) -> None:
        """Stop, naming the received signal.

        Args:
            signal_number: The signal number delivered by the OS.
            frame: The interrupted stack frame; unused.
        """
        self.stop(f"signal {signal.Signals(signal_number).name}")
