"""Event dataclass with serialization support."""

from __future__ import annotations

import datetime as dt
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from interloper.errors import EventError
from interloper.events.types import EventType


@dataclass
class Event:
    """An event emitted during the framework lifecycle.

    Carries a stable unique ``id``, a type, a UTC timestamp, and an
    arbitrary metadata dict.  The ``id`` is assigned once by the producer
    and preserved across serialization, so the same logical event can be
    persisted idempotently even when it is delivered more than once (e.g.
    re-emitted from a child process's log stream and also written directly).

    Supports JSON and dict serialization for forwarding and persistence.
    """

    type: EventType
    timestamp: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __str__(self) -> str:
        """Return a human-readable summary line for logging."""
        m = self.metadata
        ts = self.timestamp.strftime("%H:%M:%S.%f")[:-3]
        asset_key = m.get("asset_qualified_key") or m.get("asset_key") or "-"
        message = m.get("message") or m.get("error") or "-"
        label = self.type.value.upper()
        if self.type is EventType.LOG and m.get("level"):
            label = f"LOG.{m['level']}"
        return f"{ts}  {label:<30}  {asset_key}  {message}"

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a flat dict with ``event_id``, ``type``, ``timestamp``, and metadata.

        Returns:
            Dict with ``event_id``, ``type`` and ``timestamp`` as top-level
            keys, plus all metadata entries inlined.
        """
        return {
            "event_id": self.id,
            "type": self.type.value,
            "timestamp": self.timestamp.isoformat(),
            **self.metadata,
        }

    def to_json(self) -> str:
        """Serialize to a JSON string.

        Returns:
            JSON-encoded string of the event.
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        """Deserialize an Event from a dict.

        Returns:
            The deserialized Event instance.

        Raises:
            EventError: If required fields are missing or have invalid values.
        """
        type_val = data.get("type")
        if type_val is None:
            raise EventError("Missing required field 'type' in Event")
        try:
            event_type = EventType(type_val)
        except ValueError as e:
            raise EventError(f"Invalid event type '{type_val}' in Event") from e

        timestamp_val = data.get("timestamp")
        if timestamp_val is None:
            raise EventError("Missing required field 'timestamp' in Event")
        if isinstance(timestamp_val, str):
            try:
                timestamp = dt.datetime.fromisoformat(timestamp_val)
            except ValueError as e:
                raise EventError(f"Invalid timestamp format: {timestamp_val!r}") from e
        elif isinstance(timestamp_val, dt.datetime):
            timestamp = timestamp_val
        else:
            raise EventError(f"Invalid timestamp value for Event: {timestamp_val!r}")

        metadata = {k: v for k, v in data.items() if k not in ("event_id", "type", "timestamp")}

        event_id = data.get("event_id")
        if event_id is not None:
            return cls(type=event_type, timestamp=timestamp, metadata=metadata, id=str(event_id))
        return cls(type=event_type, timestamp=timestamp, metadata=metadata)

    @classmethod
    def from_json(cls, json_str: str) -> Event:
        """Deserialize an Event from a JSON string.

        Returns:
            The deserialized Event instance.
        """
        return cls.from_dict(json.loads(json_str))


def parse_event_from_log_line(line: str) -> Event | None:
    """Try to parse an :class:`Event` from a log line.

    Recognises two formats:

    1. **Prefixed** — ``@EVENT:{...}`` (written by
       :class:`~interloper.events.StderrEventHandler`).
    2. **Bare JSON** — ``{...}`` (legacy / testing convenience).

    Lines that match neither pattern, or whose JSON is malformed, are
    silently ignored and ``None`` is returned.

    Args:
        line: A single log line (may include trailing newline).

    Returns:
        The parsed Event, or ``None`` if the line is not an event.
    """
    from interloper.events.stderr import EVENT_LINE_PREFIX

    line = line.strip()
    if not line:
        return None

    if line.startswith(EVENT_LINE_PREFIX):
        line = line[len(EVENT_LINE_PREFIX):]
    elif not line.startswith("{"):
        return None

    try:
        return Event.from_json(line)
    except (json.JSONDecodeError, Exception):  # noqa: BLE001
        return None
