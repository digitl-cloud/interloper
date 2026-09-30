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
    "backfill_completed": (":white_check_mark:", "backfill completed"),
    "backfill_failed": (":x:", "backfill failed"),
}

#: Status → the word a count of partitions reads with, in the order they are listed.
_COUNT_LABELS: dict[str, str] = {
    "success": "succeeded",
    "failed": "failed",
    "canceled": "canceled",
    "running": "running",
    "dispatched": "running",
    "queued": "queued",
    "pending": "pending",
}

#: How many failed partitions a message lists before summarizing the rest.
_MAX_LISTED_PARTITIONS = 10


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
        screen readers get the outcome without parsing blocks. The subject is
        prefixed with its organisation when the operator names it, since one
        channel may hear from several. A context that knows its page in the
        app gets a link button under the message.

        Args:
            context: The hook context the message describes.

        Returns:
            The JSON-able message payload.
        """
        emoji, verb = _OUTCOMES.get(context.event_type, (":bell:", context.event_type))
        subject = context.metadata.get("component_name") or context.component_id
        if organisation := context.metadata.get("organisation_name"):
            subject = f"{organisation} · {subject}"
        headline = f"{emoji} *{subject}* {verb}"

        lines = [headline]
        if details := self._details(context):
            lines.append(details)
        if error := context.metadata.get("error"):
            lines.append(f"```{error}```")
        lines.extend(self._failed_partitions(context))

        blocks: list[dict[str, Any]] = [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}]
        if context.url:
            button = {
                "type": "button",
                "text": {"type": "plain_text", "text": "View in interloper"},
                "url": context.url,
            }
            blocks.append({"type": "actions", "elements": [button]})

        return {"channel": self.channel, "text": f"{subject} {verb}", "blocks": blocks}

    def _details(self, context: HookContext) -> str:
        """Render the context line: a run and its partition, or a backfill's range and counts.

        Args:
            context: The hook context whose subject is rendered.

        Returns:
            The line, or ``""`` when the context carries none of it.
        """
        parts = []
        if context.backfill_id:
            parts.append(f"`{context.start_key}` → `{context.end_key}`")
            counts = context.metadata.get("counts") or {}
            summary = ", ".join(
                f"{counts[status]} {label}" for status, label in _COUNT_LABELS.items() if counts.get(status)
            )
            if summary:
                parts.append(summary)
        if context.run_id:
            parts.append(f"Run `{context.run_id}`")
        if context.partition_key:
            parts.append(f"partition `{context.partition_key}`")
        return " · ".join(parts)

    def _failed_partitions(self, context: HookContext) -> list[str]:
        """Render a backfill's failed partitions, one bullet each, capped.

        Args:
            context: The hook context whose ``failed_partitions`` metadata is rendered.

        Returns:
            The bullet lines, then an "and N more" line past the cap; empty
            when the event carries none.
        """
        failed = context.metadata.get("failed_partitions") or []
        lines = [
            f"• `{partition_key}`: {error}" if error else f"• `{partition_key}`"
            for partition_key, error in failed[:_MAX_LISTED_PARTITIONS]
        ]
        if len(failed) > _MAX_LISTED_PARTITIONS:
            lines.append(f"and {len(failed) - _MAX_LISTED_PARTITIONS} more")
        return lines
