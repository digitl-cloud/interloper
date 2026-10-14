"""Run long-lived services in threads until a signal or a service's death."""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Mapping
from typing import Protocol

logger = logging.getLogger(__name__)


class Service(Protocol):
    """Something a `Supervisor` runs: a blocking ``start`` and a ``stop`` that ends it."""

    def start(self) -> None:
        """Run until stopped."""

    def stop(self) -> None:
        """Ask the running ``start`` to return."""


class Supervisor:
    """Run services in daemon threads and stop them all together.

    A signal (SIGINT, SIGTERM, SIGHUP) is a clean shutdown. A service whose
    ``start`` returns or raises while nothing asked it to is a death: every
    other service is stopped too and ``run`` fails, so an orchestrator
    restarts the process instead of keeping one that serves nothing.
    """

    def __init__(self, services: Mapping[str, Service], *, grace: float = 5.0) -> None:
        """Hold the services to run.

        Args:
            services: The services, by the name their thread and log lines carry.
            grace: Seconds each thread is given to finish once stopped.
        """
        self.services = dict(services)
        self.grace = grace
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._failed = False

    def run(self) -> None:
        """Start every service and block until they are stopped.

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
        logger.info("Started services: %s", ", ".join(self.services))
        while not self._stopping.wait(timeout=1):
            pass
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
