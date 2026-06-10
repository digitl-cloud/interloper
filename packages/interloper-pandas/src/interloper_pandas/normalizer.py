"""DataFrame-native normalizer for pandas DataFrames."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pandas as pd
from interloper.errors import SchemaError
from interloper.normalizer import Normalizer
from interloper.schema import FieldSpec, Schema
from interloper.utils.data import dataframe_to_records
from pydantic import create_model


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
    ``fill_missing``, ``infer``, ``snake_case_digits``.
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
        """Infer a Schema subclass from a ``DataFrame``'s dtypes.

        Column types are read from the dtypes directly (no row materialization);
        ``object`` columns fall back to the type of the first non-null value.
        All fields are optional, mirroring :meth:`Schema.infer`.

        Args:
            data: Normalized ``DataFrame`` (output of :meth:`normalize`).

        Returns:
            A dynamically created ``Schema`` subclass.

        Raises:
            SchemaError: If *data* has no columns.
        """
        if len(data.columns) == 0:
            raise SchemaError("Cannot infer schema from a DataFrame with no columns.")

        field_definitions: dict[str, Any] = {}
        for column in data.columns:
            py_type = _dtype_to_py_type(data[column])
            field_definitions[str(column)] = (py_type | None if py_type is not Any else Any, None)
        return create_model("InferredSchema", __base__=Schema, **field_definitions)

    def validate_schema(
        self,
        data: pd.DataFrame,
        schema: type[Schema],
        *,
        strict: bool = False,
    ) -> None:
        """Validate a ``DataFrame`` against a Schema.

        Uses a null-safe records view (``NaN``/``NaT`` → ``None``) so missing
        values in numeric columns validate correctly against nullable fields.

        Args:
            data: Normalized ``DataFrame``.
            schema: Schema class to validate against.
            strict: When ``True``, reject extra and missing required fields.
        """
        rows = dataframe_to_records(data)
        schema.validate_rows(rows, strict=strict)

    def reconcile(
        self,
        data: pd.DataFrame,
        schema: type[Schema],
    ) -> pd.DataFrame:
        """Reconcile a ``DataFrame`` against a Schema using vectorized casts.

        Columns are aligned to the schema (extras dropped, missing added as
        nulls) and cast column-wise from the schema's field specs — no per-row
        Pydantic validation, so this scales to warehouse-sized frames.

        Args:
            data: Normalized ``DataFrame``.
            schema: Schema class describing the target shape.

        Returns:
            Reconciled ``DataFrame`` with nullable pandas dtypes.

        Raises:
            SchemaError: If a column cannot be cast to its declared type, or a
                required non-nullable column is missing.
        """
        columns: dict[str, pd.Series] = {}
        for spec in schema.field_specs():
            if spec.name in data.columns:
                series = data[spec.name]
            elif spec.nullable:
                series = pd.Series([None] * len(data), index=data.index, dtype=object)
            else:
                raise SchemaError(f"Reconciliation failed: required column '{spec.name}' is missing.")
            columns[spec.name] = _cast_series(series, spec)
        return pd.DataFrame(columns, index=data.index)

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


def _dtype_to_py_type(series: pd.Series) -> Any:
    """Map a Series' dtype to a Python type for schema inference.

    ``object`` columns are resolved from the first non-null value; an all-null
    column maps to ``Any``.

    Returns:
        The resolved Python type, or ``Any`` when unknown.
    """
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return bool
    if pd.api.types.is_integer_dtype(dtype):
        return int
    if pd.api.types.is_float_dtype(dtype):
        return float
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return datetime.datetime
    if pd.api.types.is_string_dtype(dtype) and not pd.api.types.is_object_dtype(dtype):
        return str
    non_null = series.dropna()
    if non_null.empty:
        return Any
    return type(non_null.iloc[0])


def _cast_series(series: pd.Series, spec: FieldSpec) -> pd.Series:
    """Cast a Series to the pandas dtype matching a field spec.

    Uses nullable pandas dtypes (``Int64``, ``Float64``, ``boolean``,
    ``string``) so missing values survive as ``pd.NA`` instead of mutating
    the column type.

    Returns:
        The cast Series.

    Raises:
        SchemaError: If the values cannot be cast to the declared type.
    """
    # Nested / repeated / unknown types pass through untouched.
    if spec.repeated or spec.fields is not None or spec.type is Any:
        return series

    try:
        if spec.type is bool:
            return series.astype("boolean")
        if spec.type is int:
            return pd.to_numeric(series, errors="raise").astype("Int64")
        if spec.type is float:
            return pd.to_numeric(series, errors="raise").astype("Float64")
        if spec.type is str:
            return series.astype("string")
        if spec.type is datetime.datetime:
            return pd.to_datetime(series, errors="raise")
        if spec.type is datetime.date:
            return pd.to_datetime(series, errors="raise").dt.date
        if spec.type is Decimal:
            return series.map(lambda v: v if isinstance(v, Decimal) or pd.isna(v) else Decimal(str(v)))
    except (ValueError, TypeError) as e:
        raise SchemaError(f"Reconciliation failed for column '{spec.name}': cannot cast to {spec.type}: {e}") from e

    return series
