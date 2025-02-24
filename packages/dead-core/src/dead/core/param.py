import datetime as dt
import logging
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from dead.core.io import IOContext
from dead.core.partitioning import TimePartition

if TYPE_CHECKING:
    from dead.core.pipeline import ExecutionContext

logger = logging.getLogger(__name__)
T = TypeVar("T")


class AssetParam(ABC):
    @abstractmethod
    def resolve(self) -> Any: ...


class ContextualAssetParam(AssetParam):
    @abstractmethod
    def resolve(self, context: "ExecutionContext") -> Any: ...


class Env(AssetParam, str):
    def __init__(self, key: str, default: str | None = None) -> None:
        self.key = key
        self.default = default

    def resolve(self) -> Any:
        return os.environ.get(self.key, self.default)


class UpstreamAsset(ContextualAssetParam, Generic[T]):
    # Forces an UpstreamAsset's instance to be of type T
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
                f"Expected data of type {self.asset_type} from upstream asset {upstream_name}, but got {type(data)}"
            )

        logger.debug(f"Upstream asset {upstream_name} resolved (Type check passed ✔)")

        return data


class TimeAssetParam(ContextualAssetParam): ...


# TODO: default yesterday?
class Date(TimeAssetParam):
    # Forces an Date's instance to be of type datetime.date
    def __new__(cls) -> dt.date:
        return super().__new__(cls)  # type: ignore

    def resolve(self, context: "ExecutionContext") -> dt.date:
        if not context.partition or not isinstance(context.partition, TimePartition):
            raise ValueError("Date AssetParam requires the context to have a TimePartition")

        return context.partition.date


# class DateWindow(TimeAssetParam):
#     # Forces an Date's instance to be of type datetime.date
#     def __new__(cls) -> tuple[dt.date, dt.date]:
#         return super().__new__(cls)  # type: ignore

#     def resolve(self, context: "ExecutionContext") -> tuple[dt.date, dt.date]:
#         if not context.date_window:
#             raise ValueError("DateWindow asset_param requires a materialization date window")

#         return context.date_window
