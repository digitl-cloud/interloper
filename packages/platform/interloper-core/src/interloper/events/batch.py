"""EventBatch: what a run delivers to the platform in one go."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

import httpx2

from interloper.errors import DeliveryError, EventError
from interloper.events.event import Event

if TYPE_CHECKING:
    from interloper.operation.base import Effect

# A refused token or batch: delivering it again cannot help.
_FINAL_STATUSES = frozenset({401, 422})


@dataclass(frozen=True)
class EventBatch:
    """A run's events in emission order and, with its own ending, its operations' effects.

    The one shape every delivery takes, wherever it travels: the store's
    ingest folds it, the API's ingest route reads it, and a launched run's
    process posts it there (`post`). Effects never ride inside events, since
    they may carry rotated credentials.
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

    def post(self, client: httpx2.Client, run_id: str) -> bool:
        """Deliver the batch to the API's ``POST /api/runs/{run_id}/events``, as the run's token.

        A refused token or batch (401, 422) is final. Anything else is for the
        recorder to retry, a 404 included: the only way a run's process meets
        one is an API replica that has not rolled to the version serving the
        route yet.

        Args:
            client: A client of the API, authenticated with the run's token.
            run_id: The run the batch belongs to.

        Returns:
            Whether the run was live when the batch arrived.

        Raises:
            DeliveryError: If the platform did not take the batch; its
                ``retryable`` says whether trying again can help.
        """
        try:
            response = client.post(f"/api/runs/{run_id}/events", json=self.to_dict())
        except httpx2.HTTPError as error:
            raise DeliveryError(f"{type(error).__name__}: {error}") from error
        if response.is_error:
            raise DeliveryError(f"HTTP {response.status_code}", retryable=response.status_code not in _FINAL_STATUSES)
        return bool(response.json()["live"])
