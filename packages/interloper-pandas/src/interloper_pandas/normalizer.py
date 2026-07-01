"""DataFrame-native normalizer for pandas DataFrames."""

from __future__ import annotations

from typing import Any

import pandas as pd
from interloper.normalizer import Normalizer


class DataFrameNormalizer(Normalizer):
    """Type-native normalizer for pandas ``DataFrame`` asset data.

    Accepts a ``DataFrame`` and returns a ``DataFrame`` — all transformations
    are performed using native pandas operations for efficiency.

    Reshaping only: schema operations (validate, reconcile, infer) live in
    :mod:`interloper.conformer`, resolved from the data type.

    Usage::

        @asset(normalizer=DataFrameNormalizer())
        def my_asset(context):
            return pd.DataFrame({"UserName": ["alice"], "Address": ["NYC"]})

    Inherits all configuration fields from :class:`Normalizer`:
    ``normalize_columns``, ``flatten_max_level``, ``flatten_separator``,
    ``fill_missing``, ``infer``, ``snake_case_digits``, ``column_overrides``.
    """

    def normalize(self, data: Any) -> pd.DataFrame:
        """Normalize *data* to a ``DataFrame`` with configured transformations.

        If the input is already a ``DataFrame``, operates on it directly.
        Otherwise, coerces to ``list[dict]`` first, then converts to
        ``DataFrame``.

        Args:
            data: Raw asset output (``DataFrame`` or any type supported by
                the base :class:`Normalizer`).

        Returns:
            Normalized ``DataFrame``.
        """
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            rows = self._coerce(data)
            df = pd.DataFrame(rows)

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
        return pd.DataFrame(flattened)
