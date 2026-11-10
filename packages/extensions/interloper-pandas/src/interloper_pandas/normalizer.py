"""DataFrame-native normalizer for pandas DataFrames."""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd
from interloper.errors import NormalizerError
from interloper.normalizer import Normalizer
from pydantic import Field, model_validator
from typing_extensions import Self

from interloper_pandas.representation import DataFrameRepresentation


class DataFrameNormalizer(Normalizer):
    """Type-native normalizer for pandas ``DataFrame`` asset data.

    Accepts a ``DataFrame`` and returns a ``DataFrame`` — all transformations
    are performed using native pandas operations for efficiency.

    Reshaping only: schema operations (validate, reconcile, infer) live in
    the data's representation, resolved from its type.

    Usage::

        @asset(normalizer=DataFrameNormalizer())
        def my_asset(context):
            return pd.DataFrame({"UserName": ["alice"], "Address": ["NYC"]})

    Inherits all configuration fields from :class:`Normalizer`:
    ``normalize_columns``, ``flatten_max_level``, ``flatten_separator``,
    ``fill_missing``, ``infer``, ``snake_case_digits``, ``column_overrides``.

    Attributes:
        epoch_columns: Normalized column names holding instants the vendor
            sends as numbers since the Unix epoch; each becomes a UTC
            datetime. A column absent from the frame is skipped. The conform
            step refuses numbers for datetime columns, since a bare number
            does not say its unit.
        epoch_unit: The unit of every epoch column, ``"s"`` or ``"ms"``;
            required when ``epoch_columns`` is set.
    """

    epoch_columns: list[str] = Field(default_factory=list)
    epoch_unit: Literal["s", "ms"] | None = None

    @model_validator(mode="after")
    def _epoch_unit_is_declared(self) -> Self:
        """Refuse epoch columns without a unit.

        Returns:
            The normalizer, unchanged.

        Raises:
            ValueError: If ``epoch_columns`` is set and ``epoch_unit`` is not.
        """
        if self.epoch_columns and self.epoch_unit is None:
            raise ValueError("epoch_columns needs an epoch_unit: a number since the epoch does not say its unit")
        return self

    def normalize(self, data: Any) -> pd.DataFrame:
        """Normalize *data* to a ``DataFrame`` with configured transformations.

        A ``DataFrame`` is operated on directly; rows are converted first.

        Args:
            data: The rows or ``DataFrame`` to normalize.

        Returns:
            Normalized ``DataFrame``.

        Raises:
            NormalizerError: If *data* is neither a list of rows nor a ``DataFrame``.
        """
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, list):
            df = DataFrameRepresentation().from_rows(data)
        else:
            raise NormalizerError(f"DataFrameNormalizer expects list[dict] or a DataFrame, got {type(data).__name__}.")

        if df.empty:
            return df

        if self.replace_empty_dicts:
            df = df.map(lambda v: None if isinstance(v, dict) and len(v) == 0 else v)

        if self.flatten_max_level is None or self.flatten_max_level > 0:
            df = self._flatten_dataframe(df)

        if self.replace_empty_strings:
            df = df.replace("", None)

        if self.drop_na_columns:
            df = df.dropna(axis=1, how="all")

        if self.normalize_columns_names:
            df = df.rename(columns=self.column_name)

        for column in self.epoch_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column], unit=self.epoch_unit, utc=True)

        return df

    def _flatten_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flatten nested dicts in DataFrame cells using separator-joined keys.

        Any cell value that is a ``dict`` is expanded into separate columns
        with keys joined by :attr:`flatten_separator`.

        Args:
            df: Input ``DataFrame`` potentially containing dict-valued cells.

        Returns:
            Flattened ``DataFrame``.
        """
        dict_columns = [col for col in df.columns if df[col].map(lambda v: isinstance(v, dict)).any()]
        if not dict_columns:
            return df
        rows = df.to_dict("records")
        flattened = [self._flatten_dict(row) for row in rows]
        return DataFrameRepresentation().from_rows(flattened)
