"""Representation: the seam between core and concrete table types.

A :class:`Representation` answers "what kind of table is this, and how do I
view it generically?" for exactly one data representation. It bundles the
generic table views (records, columns, partition filtering) with the
representation's :class:`~interloper.conformer.Conformer`, so core never
names a concrete dataframe library anywhere.

Every representation — the rows built-in (``list[dict]``) included — is
declared as a package entry point under the ``interloper.representations``
group (core declares rows in its own ``pyproject.toml``)::

    [project.entry-points."interloper.representations"]
    dataframe = "interloper_pandas.representation:DATAFRAME_REPRESENTATION"

The registry is loaded lazily from installed-package metadata, so discovery
works in any process where the integration is installed — no import-order
dependence, no explicit registration calls.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from interloper.conformer import ROWS_CONFORMER, Conformer
from interloper.registry import Registry
from interloper.utils.data import coerce_to_records


def _adopt_representation(_name: str, loaded: Any) -> tuple[str, Representation]:
    """Instantiate a loaded representation entry and key it by its own ``key``.

    Returns:
        The ``(key, representation)`` pair.
    """
    instance: Representation = loaded() if isinstance(loaded, type) else loaded
    return instance.key, instance


REPRESENTATIONS: Registry[Representation] = Registry("interloper.representations", adopt=_adopt_representation)


class Representation(ABC):
    """Generic table views and schema operations for one data representation.

    Representations are pure mechanism: stateless, never serialized, and
    not user-configurable. ``key`` identifies the representation in
    configuration (e.g. a destination's preferred read representation).
    """

    key: ClassVar[str]

    @abstractmethod
    def matches(self, data: Any) -> bool:
        """Return whether *data* is an instance of this representation."""

    @abstractmethod
    def to_records(self, data: Any) -> list[dict[str, Any]]:
        """View *data* as ``list[dict]`` records (missing values as ``None``)."""

    @abstractmethod
    def from_records(self, rows: list[dict[str, Any]]) -> Any:
        """Materialize records into this representation."""

    @abstractmethod
    def columns(self, data: Any) -> list[str]:
        """Return the column names of *data* (empty when not discoverable)."""

    @abstractmethod
    def filter_eq(self, data: Any, column: str, value: Any) -> Any:
        """Return the subset of *data* whose *column* equals *value* (compared as strings)."""

    @property
    @abstractmethod
    def conformer(self) -> Conformer:
        """The schema operations for this representation."""

    @classmethod
    def of(cls, data: Any) -> Representation:
        """Resolve the representation matching *data*.

        Non-rows representations are checked first; everything unmatched
        falls back to rows, whose record coercion rejects non-tabular data
        with a clear error.

        Returns:
            The representation for *data*.
        """
        for key, instance in REPRESENTATIONS.items():
            if key != RowsRepresentation.key and instance.matches(data):
                return instance
        return REPRESENTATIONS[RowsRepresentation.key]


class RowsRepresentation(Representation):
    """The built-in ``list[dict]`` records representation."""

    key: ClassVar[str] = "rows"

    def matches(self, data: Any) -> bool:
        """Return whether *data* is a list (of row dicts).

        Returns:
            ``True`` for lists.
        """
        return isinstance(data, list)

    def to_records(self, data: Any) -> list[dict[str, Any]]:
        """Coerce dict / model / generator shapes to records.

        Returns:
            Data as a list of row dicts.
        """
        return coerce_to_records(data)

    def from_records(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Records are already rows.

        Returns:
            The rows unchanged.
        """
        return rows

    def columns(self, data: list[dict[str, Any]]) -> list[str]:
        """Return the keys of the first row.

        Returns:
            Column names, or ``[]`` when the shape is not discoverable.
        """
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return [str(key) for key in data[0]]
        return []

    def filter_eq(self, data: list[dict[str, Any]], column: str, value: Any) -> list[dict[str, Any]]:
        """Return the rows whose *column* equals *value* (compared as strings).

        Returns:
            The matching rows.
        """
        return [row for row in data if str(row.get(column)) == str(value)]

    @property
    def conformer(self) -> Conformer:
        """The row-wise conformer.

        Returns:
            The shared :class:`RowsConformer` instance.
        """
        return ROWS_CONFORMER
