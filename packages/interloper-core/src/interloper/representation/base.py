"""Representation: the seam between core and concrete table types.

A :class:`Representation` answers "what kind of table is this, and how do I
view it generically?" for exactly one data representation. It bundles the
generic table views (records, columns, partition filtering) with the schema
operations on that type (validate, reconcile, infer), so core never names a
concrete dataframe library anywhere. ``Representation.of(data)``
binds the matching representation to the data as a :class:`View`, whose
``to(key)`` converts between registered representations through records.

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
from dataclasses import dataclass
from typing import Any, ClassVar

from interloper.errors import RepresentationError
from interloper.registry import Registry
from interloper.schema import Schema


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
    not user-configurable. ``key`` identifies the representation in the
    registry and in ``View.to(key)``.
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

    @abstractmethod
    def validate(self, data: Any, schema: type[Schema], *, strict: bool = False) -> None:
        """Validate *data* against *schema*; raise :class:`SchemaError` on mismatch.

        Args:
            data: The table to validate, in this representation's own type.
            schema: The schema to validate against.
            strict: When ``True``, columns absent from the schema are a
                mismatch too; defaults to ``False``.
        """

    @abstractmethod
    def reconcile(self, data: Any, schema: type[Schema]) -> Any:
        """Align *data* to *schema* (drop extras, add missing) and coerce values.

        Args:
            data: The table to reconcile, in this representation's own type.
            schema: The schema to align the data to.
        """

    @abstractmethod
    def infer(self, data: Any) -> type[Schema]:
        """Infer a :class:`Schema` from *data*.

        Args:
            data: The table to infer from, in this representation's own type.
        """

    @classmethod
    def of(cls, data: Any) -> View:
        """Resolve the representation matching *data* and bind it to the data.

        Args:
            data: The table whose representation to resolve.

        Returns:
            The data viewed through its representation: ``.records``,
            ``.columns``, ``.conformer``, the partition filters, and
            ``.to(key)`` to convert it to any registered representation.

        Raises:
            RepresentationError: If no registered representation matches the
                data's type, naming the type and the registered keys.
        """
        for instance in REPRESENTATIONS.values():
            if instance.matches(data):
                return View(instance, data)
        keys = ", ".join(sorted(REPRESENTATIONS))
        raise RepresentationError(
            f"No representation matches {type(data).__name__}; registered: {keys}. "
            "Return list[dict] or a registered table type, or register a representation."
        )


@dataclass(frozen=True)
class View:
    """A representation bound to the data it views.

    What :meth:`Representation.of` returns: the generic table views with the
    data already applied, so a caller reads ``view.records`` or converts with
    ``view.to("dataframe")`` without naming the data twice.

    Attributes:
        representation: The representation matching the data (rows when none matched).
        data: The table being viewed.
    """

    representation: Representation
    data: Any

    @property
    def key(self) -> str:
        """The bound representation's registry key.

        Returns:
            The key, e.g. ``"rows"`` or ``"dataframe"``.
        """
        return self.representation.key

    @property
    def records(self) -> list[dict[str, Any]]:
        """The data as ``list[dict]`` records (missing values as ``None``).

        Returns:
            One mapping per row.
        """
        return self.representation.to_records(self.data)

    @property
    def columns(self) -> list[str]:
        """The data's column names.

        Returns:
            The names, empty when not discoverable.
        """
        return self.representation.columns(self.data)

    def validate(self, schema: type[Schema], *, strict: bool = False) -> None:
        """Validate the data against *schema*; raise :class:`SchemaError` on mismatch.

        Args:
            schema: The schema to validate against.
            strict: When ``True``, columns absent from the schema are a
                mismatch too; defaults to ``False``.
        """
        self.representation.validate(self.data, schema, strict=strict)

    def reconcile(self, schema: type[Schema]) -> Any:
        """Align the data to *schema* (drop extras, add missing) and coerce values.

        Args:
            schema: The schema to align the data to.

        Returns:
            The reconciled data, in the data's own representation.
        """
        return self.representation.reconcile(self.data, schema)

    def infer(self) -> type[Schema]:
        """Infer a :class:`Schema` from the data.

        Returns:
            A dynamically created Schema subclass.
        """
        return self.representation.infer(self.data)

    def filter_eq(self, column: str, value: Any) -> Any:
        """Return the subset of the data whose *column* equals *value* (compared as strings).

        Args:
            column: Name of the column to compare; rows missing it compare as ``None``.
            value: The value each kept row's *column* must equal.

        Returns:
            The matching rows, in the data's own representation.
        """
        return self.representation.filter_eq(self.data, column, value)

    def filter_range(self, column: str, start: Any, end: Any) -> Any:
        """Return the rows whose *column* falls in ``[start, end)``.

        Args:
            column: Name of the column to compare; rows missing it compare as ``None``.
            start: Inclusive lower bound of the range.
            end: Exclusive upper bound of the range.

        Returns:
            The matching rows, in the data's own representation.
        """
        return self.representation.filter_range(self.data, column, start, end)

    def to(self, key: str) -> Any:
        """Convert the data to the representation registered under *key*.

        Data already in the target representation is returned as is. Anything
        else is rebuilt from its records view, the one conversion every
        representation implements, so a new representation converts to and
        from every existing one the moment it is registered. An unknown key
        fails with the registry's error naming the registered keys.

        Args:
            key: The target representation's key, e.g. ``"dataframe"``.

        Returns:
            The data in the target representation.
        """
        target = REPRESENTATIONS[key]
        if target is self.representation:
            return self.data
        return target.from_records(self.records)


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

    def to_records(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rows are already records.

        Args:
            data: The table to view, in this representation's own type.

        Returns:
            The rows unchanged.
        """
        return data

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

    def validate(self, data: list[dict[str, Any]], schema: type[Schema], *, strict: bool = False) -> None:
        """Validate each row against the schema.

        Args:
            data: Rows to validate.
            schema: The schema to validate against.
            strict: When ``True``, columns absent from the schema are a
                mismatch too; defaults to ``False``.
        """
        schema.validate_rows(data, strict=strict)

    def reconcile(self, data: list[dict[str, Any]], schema: type[Schema]) -> list[dict[str, Any]]:
        """Reconcile rows against the schema.

        Args:
            data: Rows to reconcile.
            schema: The schema to align the rows to.

        Returns:
            Reconciled rows with columns aligned and values coerced.
        """
        return schema.reconcile(data)

    def infer(self, data: list[dict[str, Any]]) -> type[Schema]:
        """Infer a Schema by scanning row values.

        Args:
            data: Rows whose values are scanned to derive the field types.

        Returns:
            A dynamically created Schema subclass.
        """
        return Schema.infer(data)
