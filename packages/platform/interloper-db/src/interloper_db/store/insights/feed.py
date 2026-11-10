"""The activity feed: what happened in an organisation, derived from the rows that exist."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ActivityEntry:
    """One event in an organisation's derived activity feed.

    Attributes:
        kind: What happened (``org_created``, ``member_joined``, ...).
        when: When it happened, aware UTC.
        subject: Who or what it happened to, when the kind names one.
        extra: A detail the kind carries (a role, an inviter), or ``None``.
    """

    kind: str
    when: datetime
    subject: str | None = None
    extra: str | None = None
