"""DataFrame-native normalizer for pandas DataFrames."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from interloper.normalizer import Normalizer
from interloper.schema import Schema


@dataclass
class DataFrameNormalizer(Normalizer):
    """Type-native normalizer for pandas ``DataFrame`` asset data.

    Accepts a ``DataFrame`` and returns a ``DataFrame`` — all transformations
    are performed using native pandas operations for efficiency.

    Usage::

        @asset(normalizer=DataFrameNormalizer())
        def my_asset(context):
            return pd.DataFrame({"UserName": ["alice"], "Address": ["NYC"]})

    Inherits all configuration fields from :class:`Normalizer`:
    ``normalize_columns``, ``flatten_max_level``, ``flatten_separator``,
    ``fill_missing``, ``infer``.
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
            # Coerce to list[dict] using base class, then convert to DataFrame
            rows = self._coerce(data)
            df = pd.DataFrame(rows)

        if df.empty:
            return df

        if self.flatten_max_level is None or self.flatten_max_level > 0:
            df = self._flatten_dataframe(df)

        if self.drop_na_columns:
            df = df.dropna(axis=1, how="all")

        if self.normalize_columns_names:
            df = df.rename(columns=self.column_name)

        return df

    def infer_schema(self, data: pd.DataFrame) -> type[Schema]:
        """Infer a Schema subclass from a ``DataFrame``.

        Converts the DataFrame to records and delegates to
        :meth:`Schema.infer`.

        Args:
            data: Normalized ``DataFrame`` (output of :meth:`normalize`).

        Returns:
            A dynamically created ``Schema`` subclass.
        """
        rows = data.to_dict("records")
        return Schema.infer(rows)

    def validate_schema(
        self,
        data: pd.DataFrame,
        schema: type[Schema],
        *,
        strict: bool = False,
    ) -> None:
        """Validate a ``DataFrame`` against a Schema.

        Converts the DataFrame to records and validates each row.

        Args:
            data: Normalized ``DataFrame``.
            schema: Schema class to validate against.
            strict: When ``True``, reject extra and missing required fields.
        """
        rows = data.to_dict("records")
        schema.validate_rows(rows, strict=strict)

    def reconcile(
        self,
        data: pd.DataFrame,
        schema: type[Schema],
    ) -> pd.DataFrame:
        """Reconcile a ``DataFrame`` against a Schema.

        Aligns columns to the schema (drops extras, adds missing) and
        coerces values to the schema's types via Pydantic ``model_validate``.

        Args:
            data: Normalized ``DataFrame``.
            schema: Schema class describing the target shape.

        Returns:
            Reconciled ``DataFrame``.
        """
        rows = data.to_dict("records")
        reconciled = schema.reconcile(rows)
        return pd.DataFrame(reconciled)

    def _flatten_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flatten nested dicts in DataFrame cells using separator-joined keys.

        Any cell value that is a ``dict`` is expanded into separate columns
        with keys joined by :attr:`flatten_separator`.

        Args:
            df: Input ``DataFrame`` potentially containing dict-valued cells.

        Returns:
            Flattened ``DataFrame``.
        """
        rows = df.to_dict("records")
        flattened = [self._flatten_dict(row) for row in rows]
        return pd.DataFrame(flattened)
