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
from typing import Any, ClassVar

from interloper.events.event import Event
from interloper.events.types import EventType
from interloper.telemetry.tracer import meter

logger = logging.getLogger(__name__)


class _Bounded(OrderedDict):
    """Mapping capped at ``cap`` entries; the oldest are evicted first."""

    def __init__(self, cap: int) -> None:
        super().__init__()
        self._cap = cap

    def __setitem__(self, key: Any, value: Any) -> None:
        super().__setitem__(key, value)
        while len(self) > self._cap:
            self.popitem(last=False)


class OtelMetricsHandler:
    """Event-bus handler recording run/asset/destination metrics.

    Docker/k8s hosts re-emit child-container events with deterministic
    ids, and hosts author their own terminal events under the same ids —
    the handler therefore dedupes on ``event.id``. Bookkeeping is bounded
    because a start event can lose its terminal counterpart.
    """

    #: The event types this handler consumes — pass to ``EventBus.subscribe``.
    EVENT_TYPES: ClassVar[list[EventType]] = [
        EventType.RUN_STARTED,
        EventType.RUN_COMPLETED,
        EventType.RUN_FAILED,
        EventType.ASSET_STARTED,
        EventType.ASSET_COMPLETED,
        EventType.ASSET_FAILED,
        EventType.ASSET_CANCELED,
        EventType.DEST_READ_COMPLETED,
        EventType.DEST_READ_FAILED,
        EventType.DEST_WRITE_COMPLETED,
        EventType.DEST_WRITE_FAILED,
    ]

    _RUN_STATUS: ClassVar[dict[EventType, str]] = {
        EventType.RUN_COMPLETED: "completed",
        EventType.RUN_FAILED: "failed",
    }

    _ASSET_STATUS: ClassVar[dict[EventType, str]] = {
        EventType.ASSET_COMPLETED: "completed",
        EventType.ASSET_FAILED: "failed",
        EventType.ASSET_CANCELED: "canceled",
    }

    _DEST_STATUS: ClassVar[dict[EventType, tuple[str, str]]] = {
        EventType.DEST_READ_COMPLETED: ("read", "completed"),
        EventType.DEST_READ_FAILED: ("read", "failed"),
        EventType.DEST_WRITE_COMPLETED: ("write", "completed"),
        EventType.DEST_WRITE_FAILED: ("write", "failed"),
    }

    def __init__(self, max_tracked: int = 4096) -> None:
        """Create the handler and its instruments.

        Args:
            max_tracked: Cap for the dedupe and start-time stores.
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
        self._seen = _Bounded(max_tracked)
        self._started = _Bounded(max_tracked)

    def __call__(self, event: Event) -> None:
        """Record the event's metrics.

        The bus swallows handler exceptions, so failures are logged here
        (debug — a broken exporter must not spam the run output).

        Args:
            event: The lifecycle event to record.
        """
        try:
            if event.id in self._seen:
                return
            self._seen[event.id] = None
            self._record(event)
        except Exception:
            logger.debug("Failed to record metrics for event %s", event.type, exc_info=True)

    def _record(self, event: Event) -> None:
        if event.type is EventType.RUN_STARTED or event.type in self._RUN_STATUS:
            self._record_run(event)
        elif event.type is EventType.ASSET_STARTED or event.type in self._ASSET_STATUS:
            self._record_asset(event)
        elif event.type in self._DEST_STATUS:
            self._record_destination_io(event)

    def _record_run(self, event: Event) -> None:
        key = ("run", str(event.metadata.get("run_id", "")))
        if event.type is EventType.RUN_STARTED:
            self._started[key] = event.timestamp
            return

        IDENTITY_KEYS = ("org_id", "target_kind", "target_key")
        identity = {key: str(event.metadata[key]) for key in IDENTITY_KEYS if event.metadata.get(key)}

        attributes = {
            "status": self._RUN_STATUS[event.type],
            **identity,
        }
        self._runs.add(1, attributes)
        if (seconds := self._elapsed(key, until=event.timestamp)) is not None:
            self._run_duration.record(seconds, attributes)

    def _record_asset(self, event: Event) -> None:
        metadata = event.metadata
        key = ("asset", str(metadata.get("run_id", "")), str(metadata.get("asset_id", "")))
        if event.type is EventType.ASSET_STARTED:
            self._started[key] = event.timestamp
            return

        attributes = {
            "status": self._ASSET_STATUS[event.type],
            "asset_key": str(metadata.get("asset_key", "")),
        }
        self._assets.add(1, attributes)
        if (seconds := self._elapsed(key, until=event.timestamp)) is not None:
            self._asset_duration.record(seconds, attributes)

    def _record_destination_io(self, event: Event) -> None:
        operation, status = self._DEST_STATUS[event.type]
        attributes = {
            "operation": operation,
            "status": status,
        }
        if destination_key := event.metadata.get("destination_key"):
            attributes["destination_key"] = str(destination_key)
        self._dest_io.add(1, attributes)

    def _elapsed(self, key: tuple[str, ...], *, until: dt.datetime) -> float | None:
        started = self._started.pop(key, None)
        return None if started is None else (until - started).total_seconds()
