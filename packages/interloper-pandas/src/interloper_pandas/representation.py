"""DataFrame representation: generic table views for pandas DataFrames.

Registered with core through the ``interloper.representations`` entry point
declared in this package's ``pyproject.toml``.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
from interloper.conformer import Conformer
from interloper.representation import Representation

from interloper_pandas.conformer import DATAFRAME_CONFORMER, dataframe_to_records


class DataFrameRepresentation(Representation):
    """Generic table views and schema operations for pandas DataFrames."""

    key: ClassVar[str] = "dataframe"

    def matches(self, data: Any) -> bool:
        """Return whether *data* is a pandas ``DataFrame``.

        Returns:
            ``True`` for DataFrames.
        """
        return isinstance(data, pd.DataFrame)

    def to_records(self, data: pd.DataFrame) -> list[dict[str, Any]]:
        """View the DataFrame as null-safe records (``NaN``/``NaT`` → ``None``).

        Returns:
            Rows as a list of dicts.
        """
        return dataframe_to_records(data)

    def from_records(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        """Materialize records into a DataFrame.

        Returns:
            A pandas ``DataFrame``.
        """
        return pd.DataFrame(rows)

    def columns(self, data: pd.DataFrame) -> list[str]:
        """Return the DataFrame's column names.

        Returns:
            Column names as strings.
        """
        return [str(column) for column in data.columns]

    def filter_eq(self, data: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
        """Return the rows whose *column* equals *value* (compared as strings).

        Returns:
            The matching rows.
        """
        return data[data[column].astype(str) == str(value)]

    @property
    def conformer(self) -> Conformer:
        """The vectorized DataFrame conformer.

        Returns:
            The shared :class:`DataFrameConformer` instance.
        """
        return DATAFRAME_CONFORMER


DATAFRAME_REPRESENTATION = DataFrameRepresentation()
