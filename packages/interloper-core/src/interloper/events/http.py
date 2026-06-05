"""EventBus handler that POSTs events to the internal ingest endpoint.

Per-asset worker processes run in slim, DB-less pods, so instead of relying on
the host scraping their stdout (lossy), they ship events here.  Events are
buffered and flushed in batches on a background thread; transient failures are
retried, and :meth:`close` drains the buffer before the process exits.

Delivery is at-least-once; the ingest endpoint deduplicates by event id, so
retries never create duplicates.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

import httpx

from interloper.events.types import EventType

if TYPE_CHECKING:
    from interloper.events.event import Event

logger = logging.getLogger(__name__)

# Run-level events are owned and persisted by the host orchestrator, not by the
# per-asset child — shipping them here would duplicate them once per asset.
_RUN_LEVEL_EVENTS = frozenset(
    {EventType.RUN_STARTED, EventType.RUN_COMPLETED, EventType.RUN_FAILED}
)


class HttpEventSink:
    """Buffer events and POST them in batches to the event-ingest endpoint.

    Subscribe an instance to the :class:`~interloper.events.EventBus` and call
    :meth:`close` before the process exits to drain any buffered events.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        run_id: str,
        batch_size: int = 100,
        flush_interval: float = 0.5,
        shutdown_timeout: float = 30.0,
        exclude_types: frozenset[EventType] = _RUN_LEVEL_EVENTS,
        client: httpx.Client | None = None,
    ) -> None:
        """Initialize the sink and start its background flush thread.

        Args:
            base_url: Base API URL (e.g. the in-cluster service URL).
            token: Shared ingest service token (sent as a bearer token).
            run_id: The run these events belong to.
            batch_size: Max events per POST; also the buffer threshold that
                triggers an immediate flush.
            flush_interval: Seconds between idle flushes.
            shutdown_timeout: Max seconds :meth:`close` spends draining.
            exclude_types: Event types never shipped (run-level by default).
            client: Optional pre-built HTTP client (for tests); otherwise a
                default ``httpx.Client`` is created and owned by the sink.
        """
        self._url = f"{base_url.rstrip('/')}/api/internal/runs/{run_id}/events"
        self._headers = {"Authorization": f"Bearer {token}"}
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._shutdown_timeout = shutdown_timeout
        self._exclude_types = exclude_types
        self._client = client or httpx.Client(timeout=10.0)
        self._owns_client = client is None

        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._closing = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="http-event-sink", daemon=True)
        self._thread.start()

    def __call__(self, event: Event) -> None:
        """Buffer *event* for delivery (non-blocking)."""
        if event.type in self._exclude_types:
            return
        with self._lock:
            self._buffer.append(event.to_dict())
            pending = len(self._buffer)
        if pending >= self._batch_size:
            self._wake.set()

    def close(self) -> None:
        """Stop the background thread after a final, time-bounded drain."""
        self._closing.set()
        self._wake.set()
        self._thread.join(timeout=self._shutdown_timeout + 5.0)
        if self._owns_client:
            self._client.close()

    # -- internals ------------------------------------------------------------

    def _loop(self) -> None:
        """Background loop: flush on an interval until closed, then drain."""
        while not self._closing.is_set():
            self._wake.wait(self._flush_interval)
            self._wake.clear()
            self._drain(deadline=None)
        # Final drain on shutdown, bounded by shutdown_timeout.
        self._drain(deadline=time.monotonic() + self._shutdown_timeout)

    def _drain(self, *, deadline: float | None) -> None:
        """Flush buffered events.

        With ``deadline=None`` (periodic flush) a transient failure simply
        returns and is retried next tick.  With a deadline (shutdown) the same
        batch is retried with backoff until it succeeds or the deadline passes.
        """
        backoff = 0.5
        while True:
            with self._lock:
                if not self._buffer:
                    return
                batch = list(self._buffer[: self._batch_size])
            outcome = self._post(batch)
            if outcome == "ok":
                with self._lock:
                    del self._buffer[: len(batch)]
                backoff = 0.5
                continue
            if outcome == "drop":
                logger.error("Dropping %d event(s): ingest rejected the batch", len(batch))
                with self._lock:
                    del self._buffer[: len(batch)]
                continue
            # transient failure
            if deadline is None:
                return  # retry on the next tick
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                with self._lock:
                    lost = len(self._buffer)
                logger.warning("Event ingest unreachable at shutdown; dropping %d unsent event(s)", lost)
                return
            time.sleep(min(backoff, remaining))
            backoff = min(backoff * 2, 5.0)

    def _post(self, batch: list[dict]) -> str:
        """POST a batch of events.

        Returns:
            ``ok`` if persisted, ``retry`` for a transient failure, or
            ``drop`` for a permanent one.
        """
        try:
            resp = self._client.post(self._url, headers=self._headers, json={"events": batch})
        except httpx.HTTPError as e:
            logger.debug("Event ingest POST failed: %s", e)
            return "retry"
        if resp.status_code < 300:
            return "ok"
        if resp.status_code >= 500:
            logger.debug("Event ingest %s (will retry)", resp.status_code)
            return "retry"
        logger.error("Event ingest rejected batch: %s %s", resp.status_code, resp.text[:200])
        return "drop"
