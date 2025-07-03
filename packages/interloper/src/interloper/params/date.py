"""This module contains the date asset parameter classes."""

import datetime as dt

from interloper.execution.context import AssetExecutionContext
from interloper.params.base import ContextualAssetParam
from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow


class Date(ContextualAssetParam[dt.date]):
    """An asset parameter that resolves to the date of the partition."""

    def resolve(self, context: AssetExecutionContext) -> dt.date:
        """Resolve the value of the parameter.

        Args:
            context: The execution context.

        Returns:
            The date of the partition.

        Raises:
            ValueError: If the asset is not partitioned or the partition is not a TimePartition.
        """
        if not context.executed_asset.is_partitioned:
            raise ValueError("Asset param of type Date requires the executed asset to support partitioning")

        if not context.partition or not isinstance(context.partition, TimePartition):
            raise ValueError(
                "Asset param of type Date requires the execution context to have a TimePartition"
                f"{' (has ' + context.partition.__class__.__name__ + ')' if context.partition else ''}"
            )

        return context.partition.value


class DateWindow(ContextualAssetParam[tuple[dt.date, dt.date]]):
    """An asset parameter that resolves to the date window of the partition."""

    def resolve(self, context: AssetExecutionContext) -> tuple[dt.date, dt.date]:
        """Resolve the value of the parameter.

        Args:
            context: The execution context.

        Returns:
            The date window of the partition.

        Raises:
            ValueError: If the asset is not partitioned or the partition is not a TimePartition or TimePartitionWindow.
        """
        if not context.executed_asset.is_partitioned:
            raise ValueError("Asset param of type DateWindow requires the executed asset to support partitioning")

        if not context.partition or not isinstance(context.partition, TimePartition | TimePartitionWindow):
            raise ValueError(
                "Asset param of type DateWindow requires the context to have a TimePartition or TimePartitionWindow"
            )

        if isinstance(context.partition, TimePartitionWindow):
            return context.partition.start, context.partition.end
        return context.partition.value, context.partition.value
