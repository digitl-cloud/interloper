import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from dead.io import IOContext

if TYPE_CHECKING:
    from dead.pipeline import ExecutionContext

T = TypeVar("T")


class Sentinel(ABC):
    @abstractmethod
    def resolve(self) -> Any: ...


class RunnableSentinel(Sentinel):
    @abstractmethod
    def resolve(self, context: "ExecutionContext") -> Any: ...


class Env(Sentinel, str):
    def __init__(self, key: str, default: str | None = None) -> None:
        self.key = key
        self.default = default

    def resolve(self) -> Any:
        return os.environ.get(self.key, self.default)


# TODO: dataclass?
class UpstreamAsset(RunnableSentinel, Generic[T]):
    # Hack to make UpstreamAsset's instance type to be T
    def __new__(
        cls,
        name: str,
        asset_type: type[T] | None = None,
        io_key: str | None = None,
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

        data = io.read(IOContext(upstream_asset.name))
        if self.asset_type is not None and not isinstance(data, self.asset_type):
            raise TypeError(
                f"Expected data of type {self.asset_type} from upstream asset {upstream_name}, but got {type(data)}"
            )

        return data
