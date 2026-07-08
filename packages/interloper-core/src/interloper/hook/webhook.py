"""Webhook hook: POST event metadata to a URL."""

from __future__ import annotations

from typing import Any

from interloper.hook.base import Hook, HookContext
from interloper.resource.fields import InputField


class WebhookHook(Hook):
    """POSTs a JSON document describing the event to a URL.

    The payload is a fixed metadata document — the event type, the watched
    component, the run — so receivers integrate against one stable shape.
    """

    url: str = InputField(description="URL that receives the event POST")
    timeout: float = InputField(default=10.0, description="Request timeout in seconds")

    def fire(self, context: HookContext) -> None:
        """POST the event payload to the configured URL.

        An error status from the receiver raises ``httpx.HTTPStatusError``.
        """
        import httpx

        response = httpx.post(self.url, json=self._payload(context), timeout=self.timeout)
        response.raise_for_status()

    def _payload(self, context: HookContext) -> dict[str, Any]:
        """Build the fixed event document.

        Returns:
            The JSON-able payload.
        """
        return {
            "event_type": context.event_type,
            "component_id": context.component_id,
            "run_id": context.run_id,
            "partition_date": context.partition_date,
            "hook_id": self.id,
            "metadata": context.metadata,
        }
