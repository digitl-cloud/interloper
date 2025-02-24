import re
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from dead.core.utils import to_snake_case

T = TypeVar("T")


class Sanitizer(ABC, Generic[T]):
    @abstractmethod
    def sanitize(self, data: T, inplace: bool = False) -> T: ...

    def column_name(self, name: str) -> str:
        name = to_snake_case(name)
        name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
        name = re.sub(r"^_+", "", name)
        name = re.sub(r"_+$", "", name)
        return name
