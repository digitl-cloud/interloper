"""Insights: the reads derived from runs, events and components, one definition each.

- :mod:`.health`: what is failing, what needs a person, what each job does next
- :mod:`.outcomes`: how runs ended, per hour and per job
- :mod:`.failures`: failed attempts grouped by classified cause
- :mod:`.coverage`: what each partition of each asset came to, and which are missing
- :mod:`.feed`: what happened in an organisation, as a feed
- :mod:`.base`: :class:`InsightStore` (``store.insights``), which reads them
"""

from interloper_db.store.insights.base import InsightStore
from interloper_db.store.insights.coverage import (
    DayCoverage,
    GroupCoverage,
    JobCoverage,
    KeyCoverage,
    PartitionResult,
    Period,
)
from interloper_db.store.insights.failures import GROUP_KEYS, ErrorCause, ErrorGroup, ErrorGroups
from interloper_db.store.insights.feed import ActivityEntry
from interloper_db.store.insights.health import Attention, JobHealth, KindInventory, OrgHealth
from interloper_db.store.insights.outcomes import (
    Activity,
    BackfillProgress,
    FinishedRuns,
    HourBucket,
    InFlight,
    JobOutcome,
)

__all__ = [
    "GROUP_KEYS",
    "Activity",
    "ActivityEntry",
    "Attention",
    "BackfillProgress",
    "DayCoverage",
    "ErrorCause",
    "ErrorGroup",
    "ErrorGroups",
    "FinishedRuns",
    "GroupCoverage",
    "HourBucket",
    "InFlight",
    "InsightStore",
    "JobCoverage",
    "JobHealth",
    "JobOutcome",
    "KeyCoverage",
    "KindInventory",
    "OrgHealth",
    "PartitionResult",
    "Period",
]
