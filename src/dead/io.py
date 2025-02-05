import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class IOContext:
    asset_name: str


class IO(ABC):
    @abstractmethod
    def write(self, context: IOContext, data: Any) -> None:
        pass

    @abstractmethod
    def read(self, context: IOContext) -> Any:
        pass


class FileIO(IO):
    def __init__(self, base_dir: str) -> None:
        self.folder = base_dir

    def write(self, context: IOContext, data: Any) -> None:
        with open(Path(self.folder) / context.asset_name, "wb") as f:
            f.write(pickle.dumps(list(data)))
        print(f"Asset {context.asset_name} written to {self.folder}/{context.asset_name}")

    def read(self, context: IOContext) -> Any:
        with open(Path(self.folder) / context.asset_name, "rb") as f:
            data = pickle.loads(f.read())
        print(f"Asset {context.asset_name} read from {self.folder}/{context.asset_name}")
        return data
