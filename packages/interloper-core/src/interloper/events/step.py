"""Step: one observed lifecycle step, a started, completed and failed event triplet around a span."""

from __future__ import annotations

import traceback
from enum import Enum
from types import TracebackType
from typing import Any

from typing_extensions import Self

from interloper.errors import format_exception
from interloper.events.bus import EventBus
from interloper.events.types import EventType
from interloper.telemetry import attributes as telemetry_attributes
from interloper.telemetry.tracer import tracer


class StepKind(Enum):
    """The lifecycle steps the framework observes, each owning its events, span and wording."""

    ASSET_DATA = "asset_data"
    DEST_READ = "dest_read"
    DEST_WRITE = "dest_write"

    @property
    def started(self) -> EventType:
        """The event emitted when the step begins."""
        return EventType(f"{self.value}_started")

    @property
    def completed(self) -> EventType:
        """The event emitted when the step's body returns."""
        return EventType(f"{self.value}_completed")

    @property
    def failed(self) -> EventType:
        """The event emitted when the step's body raises."""
        return EventType(f"{self.value}_failed")

    @property
    def span(self) -> str:
        """The name of the span the step opens."""
        return _SPANS[self]

    def message(self, phase: str, subject: str, error: str | None = None) -> str:
        """Render the event message for one phase of the step.

        Args:
            phase: ``"started"``, ``"completed"`` or ``"failed"``.
            subject: What the step acts on, quoted as the message shows it.
            error: The formatted error, for the failed phase.

        Returns:
            The message.
        """
        started, completed, failed = _PHRASING[self]
        if phase == "started":
            return started.format(subject=subject)
        if phase == "completed":
            return completed.format(subject=subject)
        return failed.format(subject=subject, error=error)


_SPANS = {
    StepKind.ASSET_DATA: "interloper.asset.data",
    StepKind.DEST_READ: "interloper.destination.read",
    StepKind.DEST_WRITE: "interloper.destination.write",
}

_PHRASING = {
    StepKind.ASSET_DATA: ("Executing {subject}", "Executed {subject}", "Execution of {subject} failed: {error}"),
    StepKind.DEST_READ: ("Reading {subject}", "Read {subject}", "Failed to read {subject}: {error}"),
    StepKind.DEST_WRITE: ("Writing {subject}", "Wrote {subject}", "Failed to write {subject}: {error}"),
}


class Step:
    """One observed step: its started event and span on entry, its completed or failed event on exit.

    A context manager wrapping the body of a lifecycle step. Entering emits the kind's started
    event and opens its span with the metadata mapped to attributes; a body that returns emits
    the completed event carrying whatever `record` added; a body that raises emits the failed
    event with the formatted error and the traceback, then re-raises.

    ```py
    with Step(StepKind.DEST_WRITE, asset.event_metadata(metadata, window), subject=f"'{asset.key}'") as step:
        await invoke(destination.write, context, data)
        step.record(rows=len(data))
    ```
    """

    def __init__(
        self,
        kind: StepKind,
        metadata: dict[str, Any],
        *,
        subject: str,
        attributes: dict[str, str] | None = None,
    ) -> None:
        """Describe the step before entering it.

        Args:
            kind: Which step this is.
            metadata: The event metadata, as `Component.event_metadata` builds it.
            subject: What the step acts on, quoted as the messages show it.
            attributes: Span attributes beyond the ones the metadata maps to.
        """
        self._kind = kind
        self._metadata = metadata
        self._subject = subject
        self._attributes = {**telemetry_attributes.from_metadata(metadata), **(attributes or {})}
        self._extra: dict[str, Any] = {}
        self._span: Any = None

    def record(self, **extra: Any) -> None:
        """Add metadata to the completed event.

        Args:
            **extra: Keys the completed event carries on top of the step's metadata.
        """
        self._extra.update(extra)

    def __enter__(self) -> Self:
        """Emit the started event and open the span.

        Returns:
            The step, for `record`.
        """
        message = self._kind.message("started", self._subject)
        EventBus.emit(self._kind.started, metadata={**self._metadata, "message": message})
        self._span = tracer().start_as_current_span(self._kind.span, attributes=self._attributes)
        self._span.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        """Close the span and emit the completed or failed event.

        Args:
            exc_type: The exception type, if the body raised.
            exc: The exception, if the body raised.
            tb: The traceback, if the body raised.

        Returns:
            ``False``, so an exception propagates.
        """
        self._span.__exit__(exc_type, exc, tb)
        if exc is None:
            message = self._kind.message("completed", self._subject)
            EventBus.emit(self._kind.completed, metadata={**self._metadata, **self._extra, "message": message})
            return False
        error = format_exception(exc)
        EventBus.emit(
            self._kind.failed,
            metadata={
                **self._metadata,
                "error": error,
                "traceback": traceback.format_exc(),
                "message": self._kind.message("failed", self._subject, error),
            },
        )
        return False
