"""Per-organisation quota limits, their enforcement gates, and the usage ledger.

Three layers, each independent of the one above it:

- :mod:`.metering` — the ``usage`` ledger: what a period has consumed. Knows
  nothing about limits.
- :mod:`.definitions` — what a quota *is*: one :class:`QuotaDefinition` per
  registered key, owning how its usage is measured and compared.
- :mod:`.base` — :class:`QuotaStore` (``store.quotas``): resolves the effective
  limit for an organisation and runs the gates.
"""

from interloper_db.store.quotas.base import QuotaStore
from interloper_db.store.quotas.definitions import (
    QUOTA_MAX_ASSETS_PER_SOURCE,
    QUOTA_MAX_BACKFILL_PARTITIONS,
    QUOTA_MAX_SOURCES,
    QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH,
    QUOTAS,
    BoundQuota,
    CapacityQuota,
    ConsumptionQuota,
    QuotaDefinition,
)
from interloper_db.store.quotas.metering import METRIC_SUCCESSFUL_RUNS, METRICS, UsageLedger

__all__ = [
    "METRICS",
    "METRIC_SUCCESSFUL_RUNS",
    "QUOTAS",
    "QUOTA_MAX_ASSETS_PER_SOURCE",
    "QUOTA_MAX_BACKFILL_PARTITIONS",
    "QUOTA_MAX_SOURCES",
    "QUOTA_MAX_SUCCESSFUL_RUNS_PER_MONTH",
    "BoundQuota",
    "CapacityQuota",
    "ConsumptionQuota",
    "QuotaDefinition",
    "QuotaStore",
    "UsageLedger",
]
