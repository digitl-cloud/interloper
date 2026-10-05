"""Run heartbeat: an executing run's proof of life, and its stop signal.

While a run executes, a background thread renews its ``heartbeat_at`` every
``interval`` seconds. The reaper fails a run whose heartbeat went silent, so
a run that dies (its pod evicted, its node lost, its worker restarted) is
failed without anyone asking its launcher.

The renewal also answers whether the run should still be executing. A run
canceled, timed out or reaped elsewhere is no longer ``running``, and the
heartbeat hands control to ``on_lost``. So does a run that cannot reach the
database for half the reaper's ``timeout``: it can no longer prove it is
alive, and must be gone before the reaper fails it and a retry starts writing
the same partitions. The silence is measured from when the last successful
renewal was sent, which precedes its commit, so this process never believes
itself alive longer than the database does.

A thread rather than an asyncio task, so an operation that blocks the event
loop cannot silence it.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from types import TracebackType
from uuid import UUID

from interloper_db import Store
from typing_extensions import Self

logger = logging.getLogger(__name__)


class RunHeartbeat:
    """Renews a running run's heartbeat for as long as the block it guards runs.

    Used as a context manager around a run's execution::

        with RunHeartbeat(store, run_id, interval=10, timeout=90, on_lost=stop):
            ...
    """

    def __init__(
        self,
        store: Store,
        run_id: UUID,
        *,
        interval: float,
        timeout: float,
        on_lost: Callable[[], None] | None = None,
    ) -> None:
        """Bind the heartbeat to its run.

        Args:
            store: The store the heartbeat is renewed through.
            run_id: The running run.
            interval: Seconds between renewals.
            timeout: The reaper's heartbeat timeout in seconds; renewals
                failing for half of it count as the run being lost.
            on_lost: Called once, from the heartbeat thread, when the run
                should stop executing. A container exits its process here;
                ``None`` only stops the heartbeat, for an executor sharing its
                process with others.
        """
        self._store = store
        self._run_id = run_id
        self._interval = interval
        self._timeout = timeout
        self._on_lost = on_lost
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._beat, name=f"heartbeat-{run_id}", daemon=True)

    def __enter__(self) -> Self:
        """Start renewing the heartbeat.

        Returns:
            The heartbeat itself.
        """
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop renewing the heartbeat and wait for its thread.

        Args:
            exc_type: The exception type raised in the block, if any.
            exc: The exception raised in the block, if any.
            traceback: Its traceback, if any.
        """
        self._stopped.set()
        self._thread.join()

    def _beat(self) -> None:
        """Renew the heartbeat until stopped, or until the run is lost."""
        last_sent = time.monotonic()
        while not self._stopped.wait(self._interval):
            sent = time.monotonic()
            try:
                running = self._store.runs.heartbeat(self._run_id)
            except Exception:
                silence = time.monotonic() - last_sent
                if silence < self._timeout / 2:
                    logger.warning("Run %s could not renew its heartbeat; retrying", self._run_id, exc_info=True)
                    continue
                self._lose(f"could not renew its heartbeat for {silence:.0f}s")
                return
            if not running:
                self._lose("was ended elsewhere")
                return
            last_sent = sent

    def _lose(self, reason: str) -> None:
        """Hand a run that should stop executing to ``on_lost``.

        Args:
            reason: Why the run is lost, for the log.
        """
        logger.warning("Run %s %s; stopping it", self._run_id, reason)
        if self._on_lost is not None:
            self._on_lost()
