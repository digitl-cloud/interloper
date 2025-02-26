import datetime as dt
import logging
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from interloper.core.io import IOContext
from interloper.core.partitioning import PartitionRange, TimePartition

if TYPE_CHECKING:
    from interloper.core.pipeline import ExecutionContext

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
    def resolve(self, context: "ExecutionContext") -> T: ...


class Env(AssetParam[str]):
    # Forces an Env's instance to be of type str
    def __new__(cls, value: str) -> str:
        return super().__new__(cls)  # type: ignore

    def __init__(self, key: str, default: str | None = None) -> None:
        self.key = key
        self.default = default

    def resolve(self) -> Any:
        return os.environ.get(self.key, self.default)


class UpstreamAsset(ContextualAssetParam[T], Generic[T]):
    def __new__(
        cls,
        name: str,
        asset_type: type[T] | None = None,
    ) -> T:
        return super().__new__(cls)  # type: ignore

    def __init__(
        self,
        name: str,
        asset_type: type[T] | None = None,
    ) -> None:
        self.name = name
        self.asset_type = asset_type

    def resolve(self, context: "ExecutionContext") -> T:
        upstream_name = context.executed_asset.deps[self.name]
        upstream_asset = context.assets[upstream_name]

        if not upstream_asset.has_io:
            raise ValueError(f"Upstream asset {upstream_asset.name} does not have any IO configured")

        if upstream_asset.default_io_key is not None:
            io = upstream_asset.io[upstream_asset.default_io_key]
        else:
            if len(upstream_asset.io) > 1:
                raise ValueError(
                    f"Upstream asset {upstream_asset.name} has multiple IO configurations. "
                    "A default IO key must be set."
                )
            io = next(iter(upstream_asset.io.values()))

        data = io.read(IOContext(upstream_asset))
        if self.asset_type is not None and not isinstance(data, self.asset_type):
            raise TypeError(
                f"Expected data of type {self.asset_type.__name__} from upstream asset {upstream_name}, "
                f"but got {type(data).__name__}"
            )

        logger.debug(f"Upstream asset {upstream_name} resolved (Type check passed ✔)")

        return data


class TimeAssetParam(ContextualAssetParam[T], Generic[T]): ...


# TODO: default yesterday?
class Date(TimeAssetParam[dt.date]):
    def resolve(self, context: "ExecutionContext") -> dt.date:
        if not context.partition or not isinstance(context.partition, TimePartition):
            raise ValueError("Date asset parameter requires the context to have a TimePartition")

        return context.partition.value


class DateWindow(TimeAssetParam[tuple[dt.date, dt.date]]):
    def resolve(self, context: "ExecutionContext") -> tuple[dt.date, dt.date]:
        # if not context.executed_asset.allows_partition_range:
        #     raise ValueError("DateWindow asset parameter requires the executed asset to allow partition ranges")
        # if not context.partition or not isinstance(context.partition, TimePartitionRange):
        #     raise ValueError("DateWindow asset parameter requires the context to have a TimePartitionRange")

        if not context.partition or not isinstance(context.partition, TimePartition | PartitionRange):
            raise ValueError("Date asset parameter requires the context to have a TimePartition or PartitionRange")

        if isinstance(context.partition, PartitionRange):
            return context.partition.start, context.partition.end
        return context.partition.value, context.partition.value


class ActivePartition(ContextualAssetParam[T], Generic[T]):
    def resolve(self, context: "ExecutionContext") -> T:
        if not context.partition:
            raise ValueError("ActivePartition asset param requires the current execution context to have a Partition")
        if isinstance(context.partition, PartitionRange):
            raise ValueError("ActivePartition asset param does not support PartitionRange")

        return context.partition.value
