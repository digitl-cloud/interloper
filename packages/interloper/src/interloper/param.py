import datetime as dt
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from interloper import errors
from interloper.execution.context import AssetExecutionContext
from interloper.io.base import IOContext
from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow

logger = logging.getLogger(__name__)
T = TypeVar("T")


class AssetParam(ABC, Generic[T]):
    # Forces an AssetParam's instance to be of type T
    def __new__(cls) -> T:
        return super().__new__(cls)  # type: ignore

    @abstractmethod
    def resolve(self) -> T: ...


class ContextualAssetParam(AssetParam[T], Generic[T]):
    @abstractmethod
    def resolve(self, context: AssetExecutionContext) -> T: ...


class Env(AssetParam[str]):
    # Forces an Env's instance to be of type str
    def __new__(cls, value: str, default: str | None = None) -> str:
        return super().__new__(cls)  # type: ignore

    def __init__(self, key: str, default: str | None = None) -> None:
        self.key = key
        self.default = default

    def resolve(self) -> Any:
        value = os.environ.get(self.key, self.default)
        if value is None:
            raise errors.AssetParamResolutionError(f"Environment variable {self.key} is not set")
        return value


class UpstreamAsset(ContextualAssetParam[T], Generic[T]):
    def __new__(
        cls,
        key: str,
        type: type[T] | None = None,
    ) -> T:
        return super().__new__(cls)  # type: ignore

    def __init__(
        self,
        key: str,
        type: type[T] | None = None,
    ) -> None:
        self.key = key
        self.type = type

    def resolve(self, context: AssetExecutionContext) -> T:
        if self.key not in context.executed_asset.deps:
            raise errors.UpstreamAssetError(
                f"Upstream asset param with key {self.key} is not a dependency of asset {context.executed_asset.id}"
            )
        upstream_asset_id = context.executed_asset.deps[self.key]

        if upstream_asset_id not in context.assets:
            raise errors.UpstreamAssetError(
                f"Upstream asset {upstream_asset_id} is not found among the assets of the execution context"
            )
        upstream_asset = context.assets[upstream_asset_id]

        # The upstream asset must have at least one IO configured to be loaded
        if not upstream_asset.has_io:
            raise errors.UpstreamAssetError(
                f"Cannot resolve upstream asset {upstream_asset.id} for asset {context.executed_asset.id} "
                "because it does not have any IO configured"
            )

        # If the upstream asset has a default IO key, use it
        if upstream_asset.default_io_key is not None:
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
                f"Expected data of type {self.type.__name__} from upstream asset {upstream_asset_id}, "
                f"but got {type(data).__name__}"
            )

        logger.debug(f"Upstream asset {upstream_asset_id} resolved (Type check passed ✔)")
        return data  # type: ignore


class Date(ContextualAssetParam[dt.date]):
    def resolve(self, context: AssetExecutionContext) -> dt.date:
        if not context.executed_asset.is_partitioned:
            raise ValueError("Asset param of type Date requires the executed asset to support partitioning")

        if not context.partition or not isinstance(context.partition, TimePartition):
            raise ValueError(
                "Asset param of type Date requires the execution context to have a TimePartition"
                f"{' (has ' + context.partition.__class__.__name__ + ')' if context.partition else ''}"
            )

        return context.partition.value


class DateWindow(ContextualAssetParam[tuple[dt.date, dt.date]]):
    def resolve(self, context: AssetExecutionContext) -> tuple[dt.date, dt.date]:
        if not context.executed_asset.is_partitioned:
            raise ValueError("Asset param of type DateWindow requires the executed asset to support partitioning")

        if not context.partition or not isinstance(context.partition, TimePartition | TimePartitionWindow):
            raise ValueError(
                "Asset param of type DateWindow requires the context to have a TimePartition or TimePartitionWindow"
            )

        if isinstance(context.partition, TimePartitionWindow):
            return context.partition.start, context.partition.end
        return context.partition.value, context.partition.value
