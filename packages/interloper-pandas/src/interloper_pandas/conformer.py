"""DataFrame conformer: vectorized schema operations over field specs."""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from interloper.conformer import Conformer
from interloper.errors import SchemaError
from interloper.schema import FieldSpec, Schema
from pydantic import create_model


class DataFrameConformer(Conformer):
    """Schema operations on pandas DataFrames, vectorized over field specs.

    Reached through :class:`interloper_pandas.representation.DataFrameRepresentation`
    whenever the materialized data is a DataFrame.
    """

    def prepare(self, data: pd.DataFrame) -> pd.DataFrame:
        """DataFrames are already canonical.

        Returns:
            The DataFrame unchanged.
        """
        return data

    def validate(self, data: pd.DataFrame, schema: type[Schema], *, strict: bool = False) -> None:
        """Validate via a null-safe records view (``NaN``/``NaT`` → ``None``).

        Missing values in numeric columns validate correctly against
        nullable fields instead of failing as ``nan`` floats.
        """
        schema.validate_rows(dataframe_to_records(data), strict=strict)

    def reconcile(self, data: pd.DataFrame, schema: type[Schema]) -> pd.DataFrame:
        """Reconcile using vectorized column-wise casts from the field specs.

        No per-row pydantic validation — this scales to warehouse-sized
        frames. Uses nullable pandas dtypes (``Int64``, ``Float64``,
        ``boolean``, ``string``) so missing values survive as ``pd.NA``.

        Returns:
            Reconciled DataFrame.

        Raises:
            SchemaError: If a column cannot be cast to its declared type, or
                a required non-nullable column is missing.
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

    def infer(self, data: pd.DataFrame) -> type[Schema]:
        """Infer a Schema from the DataFrame's dtypes (no row materialization).

        ``object`` columns fall back to the type of the first non-null
        value. All fields are optional, mirroring :meth:`Schema.infer`.

        Returns:
            A dynamically created Schema subclass.

        Raises:
            SchemaError: If the DataFrame has no columns.
        """
        if len(data.columns) == 0:
            raise SchemaError("Cannot infer schema from a DataFrame with no columns.")

        field_definitions: dict[str, Any] = {}
        for column in data.columns:
            py_type = _dtype_to_py_type(data[column])
            field_definitions[str(column)] = (py_type | None if py_type is not Any else Any, None)
        return create_model("InferredSchema", __base__=Schema, **field_definitions)


DATAFRAME_CONFORMER = DataFrameConformer()


def _dtype_to_py_type(series: pd.Series) -> Any:
    """Map a Series' dtype to a Python type for schema inference.

    Returns:
        The resolved Python type, or ``Any`` when unknown (e.g. an all-null
        object column).
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


def _to_datetime(series: pd.Series) -> pd.Series:
    """Parse a Series to datetime, normalizing to UTC when offsets are mixed.

    Plain ``to_datetime`` raises on mixed-timezone strings (common in API
    responses spanning a DST boundary or differing offsets); retry with
    ``utc=True`` to coerce those to a single UTC timezone.
    """
    try:
        return pd.to_datetime(series, errors="raise")
    except ValueError:
        return pd.to_datetime(series, errors="raise", utc=True)


def _cast_series(series: pd.Series, spec: FieldSpec) -> pd.Series:
    """Cast a Series to the pandas dtype matching a field spec.

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
            return _to_datetime(series)
        if spec.type is datetime.date:
            return _to_datetime(series).dt.date
        if spec.type is Decimal:
            return series.map(lambda v: v if isinstance(v, Decimal) or pd.isna(v) else Decimal(str(v)))
    except (ValueError, TypeError) as e:
        raise SchemaError(f"Reconciliation failed for column '{spec.name}': cannot cast to {spec.type}: {e}") from e

    return series


def dataframe_to_records(data: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame to a null-safe ``list[dict]`` records view.

    Unlike ``DataFrame.to_dict("records")``, missing values (``NaN``, ``NaT``,
    ``pd.NA``) are mapped to ``None`` so the rows are valid against nullable
    schema fields and serialize to JSON ``null``.

    Returns:
        Rows as a list of dicts with ``None`` for missing values.
    """
    return data.astype(object).where(pd.notnull(data), None).to_dict("records")
