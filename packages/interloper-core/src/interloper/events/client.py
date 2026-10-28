"""EventClient: how a run's pod delivers its events to the platform."""

from __future__ import annotations

import httpx2

from interloper.errors import DeliveryError
from interloper.events.batch import EventBatch

_FINAL_STATUSES = frozenset({401, 422})


class EventClient:
    """Post a run's batches to ``POST /api/runs/{run_id}/events``, with the run's token as the bearer.

    The one call a run's pod makes. A refused token or batch (401, 422) is
    final. Anything else is for the recorder to retry, a 404 included: the
    only way a pod meets one is an API replica that has not rolled to the
    version serving the route yet.
    """

    def __init__(self, url: str, token: str, run_id: str, *, transport: httpx2.BaseTransport | None = None) -> None:
        """Bind the client to the platform and the run.

        Args:
            url: The API's base URL, as reached from inside the deployment.
            token: The run's token.
            run_id: The run whose events are delivered.
            transport: An httpx2 transport, for tests; ``None`` uses the network.
        """
        self._client = httpx2.Client(
            base_url=url, headers={"Authorization": f"Bearer {token}"}, timeout=30, transport=transport
        )
        self._path = f"/api/runs/{run_id}/events"

    def ingest(self, batch: EventBatch) -> bool:
        """Deliver one batch.

        Args:
            batch: The events, in emission order and possibly none, and the
                effects that travel with the run's own ending.

        Returns:
            Whether the run was live when the batch arrived.

        Raises:
            DeliveryError: If the platform did not take the batch; its
                ``retryable`` says whether trying again can help.
        """
        try:
            response = self._client.post(self._path, json=batch.to_dict())
        except httpx2.HTTPError as error:
            raise DeliveryError(f"{type(error).__name__}: {error}") from error
        if response.is_error:
            raise DeliveryError(f"HTTP {response.status_code}", retryable=response.status_code not in _FINAL_STATUSES)
        return bool(response.json()["live"])
