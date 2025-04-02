import json
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any

from interloper.errors import AssetNormalizationError
from interloper.schema import AssetSchema
from interloper.utils.strings import to_snake_case


class Normalizer(ABC):
    @abstractmethod
    def normalize(self, data: Any) -> Any: ...

    @abstractmethod
    def infer_schema(self, data: Any) -> Any: ...

    def column_name(self, name: str) -> str:
        name = to_snake_case(name)
        # Replace % character by pct
        name = re.sub(r"%", "_pct", name)
        # Remove special characters
        name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
        # Remove leading and trailing underscores
        name = re.sub(r"^_+", "", name)
        name = re.sub(r"_+$", "", name)
        return name


@dataclass
class JSONNormalizer(Normalizer):
    separator: str = "_"
    max_level: int = 0
    add_missing_columns: bool = field(default=True, kw_only=True)
    rename_columns: bool = field(default=True, kw_only=True)

    def normalize(self, data: Any) -> list[dict[str, Any]]:
        data = self._validate(data)

        if self.max_level > 0:
            data = [self._flatten(row) for row in data]

        if self.add_missing_columns:
            columns = self._list_columns(data)
            data = [{column: row.get(column) for column in columns} for row in data]

        if self.rename_columns:
            data = [{self.column_name(k): v for k, v in row.items()} for row in data]

        return data

    def infer_schema(self, data: list[dict[str, Any]], sample_size: int = 1000) -> type[AssetSchema]:
        if not isinstance(data, list) and not all(isinstance(row, dict) for row in data[:sample_size]):
            raise AssetNormalizationError("Cannot infer schema from data: unexpected data type")

        type_map = defaultdict(set)

        for row in data[:sample_size]:
            for key, value in row.items():
                type_map[key].add(type(value))

        def resolve_mixed_types(types: set[type]) -> type:
            if len(types) == 1:
                return types.pop()
            if str in types:
                return str
            if bool in types and int in types and len(types) == 2:
                return int
            return str

        schema_dict = {key: resolve_mixed_types(types) for key, types in type_map.items()}
        return AssetSchema.from_dict(schema_dict, name="Inferred")

    def _validate(self, data: Any) -> list:
        if isinstance(data, Generator):
            data = list(data)

        try:
            json.dumps(data)
        except Exception:
            raise AssetNormalizationError("Data is not JSON-serializable")

        if isinstance(data, dict):
            data = [data]

        if not isinstance(data, list):
            raise AssetNormalizationError("Unexpected data type")

        return data

    def _flatten(
        self,
        data: dict[str, Any],
        parent_key: str = "",
        level: int = 0,
    ) -> dict[str, Any]:
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{self.separator}{k}" if parent_key else k
            if isinstance(v, dict) and (self.max_level is None or level < self.max_level):
                items.extend(self._flatten(v, new_key, level + 1).items())
            elif isinstance(v, list) and (self.max_level is None or level < self.max_level):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(self._flatten(item, f"{new_key}{self.separator}{i}", level + 1).items())
                    else:
                        items.append((f"{new_key}{self.separator}{i}", item))
            else:
                items.append((new_key, v))
        return dict(items)

    def _list_columns(self, data: list[dict[str, Any]]) -> list[str]:
        columns = set()
        for item in data:
            columns.update(item.keys())
        return list(columns)
