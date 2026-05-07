import logging
from typing import Any

import interloper as il
import pandas as pd

from interloper_assets.fake import fake_data
from interloper_assets.impact.connection import ImpactConnection
from interloper_assets.impact.schemas import (
    Actions,
    ActionUpdates,
)

logger = logging.getLogger(__name__)

_RECORD_TYPE = dict[str, Any]


# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": ImpactConnection},
    tags=["Affiliate"],
    icon="fluent:connector-24-filled",
)
class Impact(il.Source):
    """Impact.com affiliate and partnership platform integration."""

    @il.asset(
        schema=Actions,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def actions(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Conversion actions including revenue, commissions, and attribution data."""
        return fake_data(Actions, partition_column="date", partition_date=context.partition_date)

    @il.asset(
        schema=ActionUpdates,
        partitioning=il.TimePartitionConfig(column="date"),
        tags=["Report"],
    )
    def action_updates(self, context: il.ExecutionContext, connection: ImpactConnection) -> pd.DataFrame:
        """Action updates tracking changes to conversion actions over time."""
        return fake_data(ActionUpdates, partition_column="date", partition_date=context.partition_date)
