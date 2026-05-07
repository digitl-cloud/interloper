"""Data adapters for converting between typed formats and database rows."""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Generic, TypeVar

from interloper.component import Component
from interloper.errors import AdapterError

T = TypeVar("T")


class DataAdapter(Component, Generic[T]):
    """Converts between a typed data format and database rows (``list[dict]``).

    A ``DataAdapter`` is the bridge between an asset's output type and the
    universal row format that any
    :class:`~interloper.destination.database.DatabaseDestination` works with internally.

    Subclasses implement two methods:

    * :meth:`to_rows` -- serialise typed data **into** rows (used during writes).
    * :meth:`from_rows` -- deserialise rows **back** to typed data (used during reads).
    """

    @abstractmethod
    def to_rows(self, data: T) -> list[dict[str, Any]]:
        """Convert typed data to a list of row dicts for database insertion."""

    @abstractmethod
    def from_rows(self, rows: list[dict[str, Any]]) -> T:
        """Convert database rows back to the typed format."""


class RowAdapter(DataAdapter[list[dict[str, Any]]]):
    """Identity adapter for ``list[dict]`` data.

    Passes rows through unchanged.  This is functionally equivalent to using
    no adapter at all, but can be used to be explicit about the expected data
    format.
    """

    def to_rows(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Pass through row data unchanged.

        Returns:
            The same list of dicts.

        Raises:
            AdapterError: If *data* is not a list.
        """
        if not isinstance(data, list):
            raise AdapterError(f"RowAdapter expects list[dict], got {type(data).__name__}.")
        return data

    def from_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Pass through row data unchanged.

        Returns:
            The same list of dicts.
        """
        return rows
