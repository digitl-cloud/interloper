"""Shared test fixtures."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from interloper.representation import REPRESENTATIONS, Representation, RowsRepresentation
from interloper.schema import Schema


@dataclass
class Columnar:
    """A table held column by column, the table type core's tests give a representation of their own."""

    data: dict[str, list[Any]] = field(default_factory=dict)


class ColumnarRepresentation(Representation):
    """The representation of `Columnar` tables: each operation goes through rows."""

    key = "columnar"
    _rows = RowsRepresentation()

    def matches(self, data: Any) -> bool:
        return isinstance(data, Columnar)

    def to_rows(self, data: Columnar) -> list[dict[str, Any]]:
        return [dict(zip(data.data, values)) for values in zip(*data.data.values())]

    def from_rows(self, rows: list[dict[str, Any]]) -> Columnar:
        return Columnar({name: [row.get(name) for row in rows] for name in self._rows.columns(rows)})

    def columns(self, data: Columnar) -> list[str]:
        return list(data.data)

    def filter_eq(self, data: Columnar, column: str, value: Any) -> Columnar:
        return self.from_rows(self._rows.filter_eq(self.to_rows(data), column, value))

    def filter_range(self, data: Columnar, column: str, start: Any, end: Any) -> Columnar:
        return self.from_rows(self._rows.filter_range(self.to_rows(data), column, start, end))

    def reconcile(self, data: Columnar, schema: type[Schema], *, strict: bool = False) -> Columnar:
        return self.from_rows(self._rows.reconcile(self.to_rows(data), schema, strict=strict))

    def infer(self, data: Columnar) -> type[Schema]:
        return self._rows.infer(self.to_rows(data))


@pytest.fixture
def columnar(monkeypatch: pytest.MonkeyPatch) -> type[Columnar]:
    """Register the columnar representation for one test.

    Core ships one representation, rows; this one stands in for every table
    type an installed package registers (a pandas DataFrame, say), so the
    machinery that handles them is tested without any of them installed.

    Args:
        monkeypatch: Removes the registration once the test finishes.

    Returns:
        The table type the representation recognises.
    """
    monkeypatch.setitem(REPRESENTATIONS._entries, ColumnarRepresentation.key, ColumnarRepresentation())
    return Columnar
