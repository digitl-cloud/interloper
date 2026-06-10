"""DataFrame adapter for converting between pandas DataFrames and database rows."""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.destination.adapter import DataAdapter
from interloper.errors import AdapterError
from interloper.utils.data import dataframe_to_records


class DataFrameAdapter(DataAdapter["pd.DataFrame"]):
    """Adapter for pandas ``DataFrame``.

    Converts between ``DataFrame`` and ``list[dict]`` row format used by
    :class:`~interloper.destination.database.DatabaseDestination`.
    """

    def can_handle(self, data: Any) -> bool:
        """Return whether *data* is a pandas ``DataFrame``.

        Returns:
            ``True`` for DataFrames.
        """
        return isinstance(data, pd.DataFrame)

    def to_rows(self, data: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert a ``DataFrame`` to a list of row dicts.

        Missing values (``NaN``, ``NaT``, ``pd.NA``) are mapped to ``None`` so
        rows serialize to JSON ``null`` and validate against nullable fields.

        Args:
            data: A pandas ``DataFrame``.

        Returns:
            Rows as list of dicts.

        Raises:
            AdapterError: If *data* is not a ``DataFrame``.
        """
        if not isinstance(data, pd.DataFrame):
            raise AdapterError(f"DataFrameAdapter expects a pandas DataFrame, got {type(data).__name__}.")
        return dataframe_to_records(data)

    def from_rows(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        """Convert rows to a pandas ``DataFrame``.

        Args:
            rows: Raw rows from the database.

        Returns:
            A pandas ``DataFrame``.
        """
        return pd.DataFrame(rows)
