"""This module contains the asset parameter classes."""
import datetime as dt
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from interloper import errors
from interloper.execution.context import AssetExecutionContext
from interloper.io.base import IO, IOContext
from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow

logger = logging.getLogger(__name__)
T = TypeVar("T")


class AssetParam(ABC, Generic[T]):
    """An abstract class for asset parameters."""

    # Forces an AssetParam's instance to be of type T
    def __new__(cls) -> T:
        """Create a new instance of the asset parameter."""
        return super().__new__(cls)  # type: ignore

    @abstractmethod
    def resolve(self) -> T:
        """Resolve the value of the parameter."""
        ...


class ContextualAssetParam(AssetParam[T], Generic[T]):
    """An abstract class for contextual asset parameters."""

    @abstractmethod
    def resolve(self, context: AssetExecutionContext) -> T:
        """Resolve the value of the parameter.

        Args:
            context: The execution context.
        """
        ...


class Env(AssetParam[str]):
    """An asset parameter that resolves to an environment variable."""

    # Forces an Env's instance to be of type str
    def __new__(cls, value: str, default: str | None = None) -> str:
        """Create a new instance of the environment variable parameter."""
        return super().__new__(cls)  # type: ignore

    def __init__(self, key: str, default: str | None = None) -> None:
        """Initialize the environment variable parameter.

        Args:
            key: The name of the environment variable.
            default: The default value to use if the environment variable is not set.
        """
        self.key = key
        self.default = default

    def resolve(self) -> Any:
        """Resolve the value of the parameter.

        Returns:
            The value of the environment variable.

        Raises:
            AssetParamResolutionError: If the environment variable is not set and no default is provided.
        """
        value = os.environ.get(self.key, self.default)
        if value is None:
            raise errors.AssetParamResolutionError(f"Environment variable {self.key} is not set")
        return value


class UpstreamAsset(ContextualAssetParam[T], Generic[T]):
    """An asset parameter that resolves to the output of an upstream asset."""

    def __new__(
        cls,
        key: str,
        type: type[T] | None = None,
    ) -> T:
        """Create a new instance of the upstream asset parameter."""
        return super().__new__(cls)  # type: ignore

    def __init__(
        self,
        key: str,
        type: type[T] | None = None,
    ) -> None:
        """Initialize the upstream asset parameter.

        Args:
            key: The key of the upstream asset in the dependencies dictionary.
            type: The expected type of the upstream asset's output.
        """
        self.key = key
        self.type = type

    def resolve(self, context: AssetExecutionContext) -> T:
        """Resolve the value of the parameter.

        Args:
            context: The execution context.

        Returns:
            The output of the upstream asset.

        Raises:
            UpstreamAssetError: If the upstream asset cannot be resolved.
            TypeError: If the output of the upstream asset is not of the expected type.
        """
        if self.key not in context.executed_asset.deps:
            raise errors.UpstreamAssetError(
                f"Upstream asset param with key {self.key} is not a dependency of asset {context.executed_asset.id}"
            )
        upstream_asset = context.executed_asset.deps[self.key]

        if upstream_asset.id not in context.assets:
            raise errors.UpstreamAssetError(
                f"Upstream asset {upstream_asset.id} is not found among the assets of the execution context"
            )

        # The upstream asset must have at least one IO configured to be loaded
        if not upstream_asset.has_io:
            raise errors.UpstreamAssetError(
                f"Cannot resolve upstream asset {upstream_asset.id} for asset {context.executed_asset.id} "
                "because it does not have any IO configured"
            )

        if isinstance(upstream_asset.io, IO):
            io = upstream_asset.io

        # If the upstream asset has a default IO key, use it
        elif upstream_asset.default_io_key is not None:
            try:
                io = upstream_asset.io[upstream_asset.default_io_key]
            except KeyError:
                raise errors.UpstreamAssetError(
                    f"Cannot resolve upstream asset {upstream_asset.id} for asset {context.executed_asset.id} "
                    f"because it does not have an IO configuration for IO key {upstream_asset.default_io_key}"
                )

        # If the upstream asset has no default IO key, it must have exactly one IO config
        else:
            if len(upstream_asset.io) > 1:
                raise errors.UpstreamAssetError(
                    f"Cannot resolve upstream asset {upstream_asset.id} for asset {context.executed_asset.id} "
                    "because it has multiple IO configurations. A default IO key must be set"
                )
            io = next(iter(upstream_asset.io.values()))

        # Partitioning
        partition = None
        if context and context.partition:
            # A non-partitioned asset cannot have a partitioned upstream asset
            # TODO: support this case?
            if not context.executed_asset.is_partitioned and upstream_asset.is_partitioned:
                raise errors.UpstreamAssetError(
                    f"Cannot resolve upstream asset {upstream_asset.id} for asset {context.executed_asset.id} "
                    "because a non-partitioned asset cannot have a partitioned upstream asset"
                )
            # A partitioned asset can have a partitioned upstream asset
            elif context.executed_asset.is_partitioned and upstream_asset.is_partitioned:
                partition = context.partition

            # (Else) Partitioned asset with a non-partitioned upstream asset: no partition used in the IO context

        # Load the asset
        try:
            io_context = IOContext(upstream_asset, partition)
            data = io.read(io_context)
        except Exception as e:
            raise errors.UpstreamAssetError(
                f"Cannot load data from upstream asset {upstream_asset.id} for asset {context.executed_asset.id}: {e}"
            )

        # If the upstream asset has a type, check that the data is of the correct type
        if self.type is not None and not isinstance(data, self.type):
            raise TypeError(
                f"Expected data of type {self.type.__name__} from upstream asset {upstream_asset.id}, "
                f"but got {type(data).__name__}"
            )

        logger.debug(f"Upstream asset {upstream_asset.id} resolved (Type check passed ✔)")
        return data  # type: ignore


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
