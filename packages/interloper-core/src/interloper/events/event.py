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
    reported by a child process and authored by its host).

    Supports JSON and dict serialization for forwarding and persistence.
    """

    type: EventType
    timestamp: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def subject(self) -> str:
        """What the event is about: its qualified key, else its component key, else ``"-"``.

        Returns:
            The key the event names, or ``"-"``.
        """
        return self.metadata.get("qualified_key") or self.metadata.get("component_key") or "-"

    @property
    def summary(self) -> str:
        """What the event says: its message, else its error, else ``"-"``.

        Returns:
            The message the event carries, or ``"-"``.
        """
        return self.metadata.get("message") or self.metadata.get("error") or "-"

    def __str__(self) -> str:
        """Return a human-readable summary line for logging.

        Returns:
            A single line of ``time  TYPE  component_key  message``.
        """
        timestamp = self.timestamp.strftime("%H:%M:%S.%f")[:-3]
        label = self.type.value.upper()
        if self.type is EventType.LOG and self.metadata.get("level"):
            label = f"LOG.{self.metadata['level']}"
        return f"{timestamp}  {label:<30}  {self.subject}  {self.summary}"

    # -- Serialization ---------------------------------------------------------

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

        Args:
            data: Flat mapping as produced by `to_dict` — ``type`` and
                ``timestamp`` are required, ``event_id`` is optional (a fresh
                id is generated when absent), and every other key becomes
                metadata.

        Returns:
            The deserialized Event instance.

        Raises:
            EventError: If required fields are missing or have invalid values.
        """
        type_value = data.get("type")
        if type_value is None:
            raise EventError("Missing required field 'type' in Event")
        try:
            event_type = EventType(type_value)
        except ValueError as error:
            raise EventError(f"Invalid event type '{type_value}' in Event") from error

        timestamp_value = data.get("timestamp")
        if timestamp_value is None:
            raise EventError("Missing required field 'timestamp' in Event")
        if isinstance(timestamp_value, str):
            try:
                timestamp = dt.datetime.fromisoformat(timestamp_value)
            except ValueError as error:
                raise EventError(f"Invalid timestamp format: {timestamp_value!r}") from error
        elif isinstance(timestamp_value, dt.datetime):
            timestamp = timestamp_value
        else:
            raise EventError(f"Invalid timestamp value for Event: {timestamp_value!r}")

        metadata = {key: value for key, value in data.items() if key not in ("event_id", "type", "timestamp")}

        event_id = data.get("event_id")
        if event_id is not None:
            return cls(type=event_type, timestamp=timestamp, metadata=metadata, id=str(event_id))
        return cls(type=event_type, timestamp=timestamp, metadata=metadata)

    @classmethod
    def from_json(cls, json_str: str) -> Event:
        """Deserialize an Event from a JSON string.

        Args:
            json_str: JSON object as produced by `to_json`.

        Returns:
            The deserialized Event instance.
        """
        return cls.from_dict(json.loads(json_str))
