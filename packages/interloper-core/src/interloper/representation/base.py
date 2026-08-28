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

import datetime as dt
from abc import ABC, abstractmethod
from typing import Any, ClassVar

from interloper.conformer import ROWS_CONFORMER, Conformer
from interloper.registry import Registry
from interloper.utils.data import coerce_to_records


def _adopt_representation(_name: str, loaded: Any) -> tuple[str, Representation]:
    """Instantiate a loaded representation entry and key it by its own ``key``.

    Args:
        _name: The entry-point name, ignored: a representation is keyed by its own ``key``, never by
            the name the entry point happens to be declared under.
        loaded: The loaded entry-point object: either a ``Representation`` class to instantiate or an
            already-built instance.

    Returns:
        The ``(key, representation)`` pair.
    """
    instance: Representation = loaded() if isinstance(loaded, type) else loaded
    return instance.key, instance


REPRESENTATIONS: Registry[Representation] = Registry("interloper.representations", adopt=_adopt_representation)


def iso_label(value: Any) -> str:
    """Normalize a time-partition column value for lexicographic comparison.

    Dates and datetimes render as ISO-8601 with the ``T`` separator; strings
    get their first space replaced by ``T`` (``str(datetime)`` and most SQL
    text renderings use a space). Uniform ISO strings compare correctly as
    strings, including a date against a datetime: the date is a prefix, and a
    half-open range keeps prefix ordering exact at both bounds.

    Args:
        value: A time-partition column value: a ``date``, a ``datetime``, or anything renderable as
            text (``None`` included, which renders as ``"None"``).

    Returns:
        The value as a comparable ISO-8601 string.
    """
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value).replace(" ", "T", 1)


class Representation(ABC):
    """Generic table views and schema operations for one data representation.

    Representations are pure mechanism: stateless, never serialized, and
    not user-configurable. ``key`` identifies the representation in
    configuration (e.g. a destination's preferred read representation).
    """

    key: ClassVar[str]

    @abstractmethod
    def matches(self, data: Any) -> bool:
        """Return whether *data* is an instance of this representation.

        Args:
            data: The object to test, of any type.
        """

    @abstractmethod
    def to_records(self, data: Any) -> list[dict[str, Any]]:
        """View *data* as ``list[dict]`` records (missing values as ``None``).

        Args:
            data: The table to view, in this representation's own type.
        """

    @abstractmethod
    def from_records(self, rows: list[dict[str, Any]]) -> Any:
        """Materialize records into this representation.

        Args:
            rows: The records to materialize, missing values given as ``None``.
        """

    @abstractmethod
    def columns(self, data: Any) -> list[str]:
        """Return the column names of *data* (empty when not discoverable).

        Args:
            data: The table to inspect, in this representation's own type.
        """

    @abstractmethod
    def filter_eq(self, data: Any, column: str, value: Any) -> Any:
        """Return the subset of *data* whose *column* equals *value* (compared as strings).

        Args:
            data: The table to filter, in this representation's own type.
            column: Name of the column to compare; rows missing it compare as ``None``.
            value: The value each kept row's *column* must equal.
        """

    @abstractmethod
    def filter_range(self, data: Any, column: str, start: Any, end: Any) -> Any:
        """Return the rows whose *column* falls in ``[start, end)``.

        Values and bounds are compared as ISO-8601 strings (see
        :func:`iso_label`): the scoping primitive for time partitions, whose
        rows may carry values anywhere inside the period rather than the
        period's start.

        Args:
            data: The table to filter, in this representation's own type.
            column: Name of the column to compare; rows missing it compare as ``None``.
            start: Inclusive lower bound of the range.
            end: Exclusive upper bound of the range.
        """

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

        Args:
            data: The table whose representation to resolve, of any type.

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

        Args:
            data: The object to test, of any type.

        Returns:
            ``True`` for lists.
        """
        return isinstance(data, list)

    def to_records(self, data: Any) -> list[dict[str, Any]]:
        """Coerce dict / model / generator shapes to records.

        Args:
            data: The table to view, in this representation's own type.

        Returns:
            Data as a list of row dicts.
        """
        return coerce_to_records(data)

    def from_records(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Records are already rows.

        Args:
            rows: The records to materialize, missing values given as ``None``.

        Returns:
            The rows unchanged.
        """
        return rows

    def columns(self, data: list[dict[str, Any]]) -> list[str]:
        """Return the keys of the first row.

        Args:
            data: The table to inspect, in this representation's own type.

        Returns:
            Column names, or ``[]`` when the shape is not discoverable.
        """
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return [str(key) for key in data[0]]
        return []

    def filter_eq(self, data: list[dict[str, Any]], column: str, value: Any) -> list[dict[str, Any]]:
        """Return the rows whose *column* equals *value* (compared as strings).

        Args:
            data: The table to filter, in this representation's own type.
            column: Name of the column to compare; rows missing it compare as ``None``.
            value: The value each kept row's *column* must equal.

        Returns:
            The matching rows.
        """
        return [row for row in data if str(row.get(column)) == str(value)]

    def filter_range(
        self, data: list[dict[str, Any]], column: str, start: Any, end: Any
    ) -> list[dict[str, Any]]:
        """Return the rows whose *column* falls in ``[start, end)``.

        Args:
            data: The table to filter, in this representation's own type.
            column: Name of the column to compare; rows missing it compare as ``None``.
            start: Inclusive lower bound of the range.
            end: Exclusive upper bound of the range.

        Returns:
            The matching rows.
        """
        lo, hi = iso_label(start), iso_label(end)
        return [row for row in data if lo <= iso_label(row.get(column)) < hi]

    @property
    def conformer(self) -> Conformer:
        """The row-wise conformer.

        Returns:
            The shared :class:`RowsConformer` instance.
        """
        return ROWS_CONFORMER
