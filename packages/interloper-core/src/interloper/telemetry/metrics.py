"""OTel metrics derived from the event bus.

Counters and durations are order-insensitive, so they ride the
:class:`~interloper.events.bus.EventBus` (which already carries the full
lifecycle taxonomy with timestamps) instead of the execution hot path.
Metric attributes stay low-cardinality by design: ``asset_key`` is
bounded by the catalog, while ids and partitions are deliberately
excluded.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections import OrderedDict
from typing import ClassVar

from interloper.events.event import Event
from interloper.events.types import EventType
from interloper.telemetry.tracer import meter

logger = logging.getLogger(__name__)

_RUN_STATUS = {EventType.RUN_COMPLETED: "completed", EventType.RUN_FAILED: "failed"}
_ASSET_STATUS = {
    EventType.ASSET_COMPLETED: "completed",
    EventType.ASSET_FAILED: "failed",
    EventType.ASSET_CANCELED: "canceled",
}
_DEST_STATUS = {
    EventType.DEST_READ_COMPLETED: ("read", "completed"),
    EventType.DEST_READ_FAILED: ("read", "failed"),
    EventType.DEST_WRITE_COMPLETED: ("write", "completed"),
    EventType.DEST_WRITE_FAILED: ("write", "failed"),
}


class OtelMetricsHandler:
    """Event-bus handler recording run/asset/destination metrics.

    Docker/k8s hosts re-emit child-container events with deterministic
    ids, and hosts author their own terminal events under the same ids —
    the handler therefore dedupes on ``event.id``. Tracking stores are
    size-capped (oldest-first eviction) because start events can lose
    their terminal counterpart.
    """

    #: The event types this handler consumes — pass to ``EventBus.subscribe``.
    EVENT_TYPES: ClassVar[list[EventType]] = [
        EventType.RUN_STARTED,
        *_RUN_STATUS,
        EventType.ASSET_STARTED,
        *_ASSET_STATUS,
        *_DEST_STATUS,
    ]

    def __init__(self, max_tracked: int = 4096) -> None:
        """Create the handler and its instruments.

        Args:
            max_tracked: Cap for the dedupe and in-flight tracking stores.
        """
        m = meter()
        self._runs = m.create_counter("interloper.runs", unit="{run}", description="Finished runs")
        self._run_duration = m.create_histogram("interloper.run.duration", unit="s", description="Run duration")
        self._assets = m.create_counter("interloper.assets", unit="{execution}", description="Finished assets")
        self._asset_duration = m.create_histogram(
            "interloper.asset.duration", unit="s", description="Asset execution duration"
        )
        self._dest_io = m.create_counter(
            "interloper.destination.io", unit="{operation}", description="Destination read/write operations"
        )
        self._max_tracked = max_tracked
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._started: OrderedDict[tuple[str, ...], dt.datetime] = OrderedDict()

    def __call__(self, event: Event) -> None:
        """Record the event's metrics.

        The bus swallows handler exceptions, so failures are logged here
        (debug — a broken exporter must not spam the run output).

        Args:
            event: The lifecycle event to record.
        """
        try:
            self._handle(event)
        except Exception:
            logger.debug("Failed to record metrics for event %s", event.type, exc_info=True)

    def _handle(self, event: Event) -> None:
        if event.id in self._seen:
            return
        self._remember(self._seen, event.id, None)

        metadata = event.metadata
        run_id = str(metadata.get("run_id", ""))

        if event.type is EventType.RUN_STARTED:
            self._remember(self._started, ("run", run_id), event.timestamp)
        elif event.type in _RUN_STATUS:
            attrs = {"status": _RUN_STATUS[event.type]}
            self._runs.add(1, attrs)
            started = self._started.pop(("run", run_id), None)
            if started is not None:
                self._run_duration.record((event.timestamp - started).total_seconds(), attrs)
        elif event.type is EventType.ASSET_STARTED:
            self._remember(self._started, ("asset", run_id, str(metadata.get("asset_id", ""))), event.timestamp)
        elif event.type in _ASSET_STATUS:
            attrs = {"status": _ASSET_STATUS[event.type], "asset_key": str(metadata.get("asset_key", ""))}
            self._assets.add(1, attrs)
            started = self._started.pop(("asset", run_id, str(metadata.get("asset_id", ""))), None)
            if started is not None:
                self._asset_duration.record((event.timestamp - started).total_seconds(), attrs)
        elif event.type in _DEST_STATUS:
            operation, status = _DEST_STATUS[event.type]
            attrs = {"operation": operation, "status": status}
            destination_key = metadata.get("destination_key")
            if destination_key:
                attrs["destination_key"] = str(destination_key)
            self._dest_io.add(1, attrs)

    def _remember(self, store: OrderedDict, key: object, value: object) -> None:
        store[key] = value
        while len(store) > self._max_tracked:
            store.popitem(last=False)
