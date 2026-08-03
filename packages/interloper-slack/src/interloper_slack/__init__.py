"""Interloper Slack integration: notification hook and connection."""

from interloper_slack.connection import SlackConnection
from interloper_slack.hook import SlackHook

__all__ = [
    "SlackConnection",
    "SlackHook",
]
