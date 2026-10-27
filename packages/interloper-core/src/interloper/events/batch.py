"""EventBatch: what a run delivers to the platform in one go."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from interloper.errors import EventError
from interloper.events.event import Event

if TYPE_CHECKING:
    from interloper.operation.base import Effect


@dataclass(frozen=True)
class EventBatch:
    """A run's events in emission order and, with its own ending, its operations' effects.

    The one shape every delivery takes, wherever it travels: the store's
    ingest folds it, the API's ingest route reads it, and the client that
    reports a pod's run posts it. Effects never ride inside events, since they
    may carry rotated credentials.
    """

    events: list[Event] = field(default_factory=list)
    effects: dict[str, Effect] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the batch to its JSON shape.

        Returns:
            The events as dicts and the effects keyed by component id, ``None`` for none.
        """
        effects = (
            {component_id: asdict(effect) for component_id, effect in self.effects.items()} if self.effects else None
        )
        return {"events": [event.to_dict() for event in self.events], "effects": effects}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventBatch:
        """Parse a batch from its JSON shape.

        Args:
            data: The batch, as `to_dict` writes it.

        Returns:
            The batch.

        Raises:
            EventError: If the batch, one of its events or one of its effects is malformed.
        """
        from interloper.operation.base import Effect

        try:
            events = [Event.from_dict(item) for item in data["events"]]
            effects = {
                str(component_id): Effect.from_dict(effect)
                for component_id, effect in (data.get("effects") or {}).items()
            }
        except (KeyError, TypeError, AttributeError) as error:
            raise EventError(f"Malformed event batch: {error}") from error
        return cls(events=events, effects=effects or None)
