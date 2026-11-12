"""Shared test fixtures."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import Any

import pytest

from interloper.extension import base as extension_base
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


Install = Callable[..., None]


@pytest.fixture
def install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Install]:
    """Install throwaway packages that ship extension manifests, as the only installed ones.

    Args:
        tmp_path: Where the packages are written, prepended to ``sys.path``.
        monkeypatch: Replaces the locator group's metadata with the
            installed packages, in installation order.

    Yields:
        A function taking a package name, the text of its ``extension.yaml``
        (``None`` for a package that ships none) and the source of its
        modules by module name (``__init__`` included), which writes the
        package and adds its locator. The packages' modules are dropped from
        ``sys.modules`` afterwards.
    """
    monkeypatch.syspath_prepend(str(tmp_path))
    locators: list[EntryPoint] = []
    monkeypatch.setattr(extension_base, "entry_points", lambda group: list(locators))

    def install(package: str, manifest: str | None, **modules: str) -> None:
        directory = tmp_path / package
        directory.mkdir()
        (directory / "__init__.py").write_text(modules.pop("__init__", ""))
        for module, source in modules.items():
            (directory / f"{module}.py").write_text(source)
        if manifest is not None:
            (directory / "extension.yaml").write_text(manifest)
        importlib.invalidate_caches()
        locators.append(EntryPoint(package, package, "interloper.extensions"))

    yield install

    for locator in locators:
        for module in [name for name in sys.modules if name.partition(".")[0] == locator.module]:
            del sys.modules[module]
