"""Slack hook: post a run outcome to a channel."""

from __future__ import annotations

from typing import Any, ClassVar

from interloper.errors import ConfigError
from interloper.hook import Hook, HookContext
from interloper.resource.fields import FetchField, InputField

from interloper_slack.connection import SlackConnection

#: Event type → (emoji, past-tense verb) for the headline.
_OUTCOMES: dict[str, tuple[str, str]] = {
    "run_completed": (":white_check_mark:", "completed"),
    "run_failed": (":x:", "failed"),
}


class SlackHook(Hook):
    """Posts a run outcome to a Slack channel.

    The notification counterpart to ``WebhookHook``: same events, but the
    payload is a message a human reads rather than a document a service
    parses. Defaults to ``run_failed`` like every hook, which is the alert
    most teams want; add ``run_completed`` for a full-chatter channel.

    The bot must be a member of the target channel — Slack rejects
    ``chat.postMessage`` with ``not_in_channel`` otherwise, and the failure
    is recorded on the firing claim rather than retried.
    """

    name: ClassVar[str] = "Slack"
    icon: ClassVar[str] = "devicon:slack"
    tags: ClassVar[list[str]] = ["Communication"]

    connection: SlackConnection

    channel: str = FetchField(
        provider="connection.channels",
        label_key="name",
        value_key="id",
        description="Channel that receives the notification",
        discriminator=True,
    )
    timeout: float = InputField(default=10.0, description="Request timeout in seconds")

    def fire(self, context: HookContext) -> None:
        """Post the event as a message to the configured channel.

        Args:
            context: The hook context carrying the event and its run.

        Raises:
            ConfigError: If no Slack connection is attached.
            RuntimeError: If Slack rejects the message — it answers a refusal
                with 200 and ``ok: false``, so the status alone is not enough.
        """
        if self.connection is None:
            raise ConfigError(f"SlackHook '{self.id}' fired without a Slack connection")

        response = self.connection.client.post(
            "/chat.postMessage",
            json=self._message(context),
            timeout=self.timeout,
        )
        response.raise_for_status()
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(f"Slack API error: {body.get('error')}")

    def _message(self, context: HookContext) -> dict[str, Any]:
        """Build the ``chat.postMessage`` payload.

        ``text`` carries the headline on its own so notification previews and
        screen readers get the outcome without parsing blocks.

        Args:
            context: The hook context the message describes.

        Returns:
            The JSON-able message payload.
        """
        emoji, verb = _OUTCOMES.get(context.event_type, (":bell:", context.event_type))
        subject = context.metadata.get("component_name") or context.component_id
        headline = f"{emoji} *{subject}* {verb}"

        lines = [headline]
        if details := self._details(context):
            lines.append(details)
        if error := context.metadata.get("error"):
            lines.append(f"```{error}```")

        return {
            "channel": self.channel,
            "text": f"{subject} {verb}",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}],
        }

    def _details(self, context: HookContext) -> str:
        """Render the run/partition context line.

        Args:
            context: The hook context whose run and partition are rendered.

        Returns:
            The line, or ``""`` when the context carries neither.
        """
        parts = []
        if context.run_id:
            parts.append(f"Run `{context.run_id}`")
        if context.partition_key:
            parts.append(f"partition `{context.partition_key}`")
        return " · ".join(parts)
