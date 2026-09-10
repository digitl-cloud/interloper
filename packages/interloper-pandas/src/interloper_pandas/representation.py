"""DataFrame representation: table views and vectorized schema operations for pandas DataFrames.

Registered with core through the ``interloper.representations`` entry point
declared in this package's ``pyproject.toml``.
"""

from __future__ import annotations

import datetime
import json
import logging
from decimal import Decimal
from typing import Any, ClassVar

import pandas as pd
from interloper.errors import SchemaError
from interloper.representation import Representation, iso_label
from interloper.schema import FieldSpec, Schema
from pydantic import create_model

logger = logging.getLogger(__name__)


class DataFrameRepresentation(Representation):
    """Generic table views and schema operations for pandas DataFrames.

    Schema operations are vectorized over field specs, so they scale to
    warehouse-sized frames without per-row pydantic validation.
    """

    key: ClassVar[str] = "dataframe"

    def matches(self, data: Any) -> bool:
        """Return whether *data* is a pandas ``DataFrame``.

        Args:
            data: The value to test.

        Returns:
            ``True`` for DataFrames.
        """
        return isinstance(data, pd.DataFrame)

    def to_records(self, data: pd.DataFrame) -> list[dict[str, Any]]:
        """View the DataFrame as null-safe records (``NaN``/``NaT`` → ``None``).

        Args:
            data: The DataFrame to view.

        Returns:
            Rows as a list of dicts.
        """
        return dataframe_to_records(data)

    def from_records(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        """Materialize records into a DataFrame.

        Args:
            rows: The records to materialize.

        Returns:
            A pandas ``DataFrame``.
        """
        return pd.DataFrame(rows)

    def columns(self, data: pd.DataFrame) -> list[str]:
        """Return the DataFrame's column names.

        Args:
            data: The DataFrame to read.

        Returns:
            Column names as strings.
        """
        return [str(column) for column in data.columns]

    def filter_eq(self, data: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
        """Return the rows whose *column* equals *value* (compared as strings).

        Args:
            data: The DataFrame to filter.
            column: The column to compare.
            value: The value to match.

        Returns:
            The matching rows.
        """
        return data[data[column].astype(str) == str(value)]

    def filter_range(self, data: pd.DataFrame, column: str, start: Any, end: Any) -> pd.DataFrame:
        """Return the rows whose *column* falls in ``[start, end)``.

        Args:
            data: The DataFrame to filter.
            column: The column to compare, mapped through ``iso_label``.
            start: Inclusive lower bound.
            end: Exclusive upper bound.

        Returns:
            The matching rows.
        """
        labels = data[column].map(iso_label)
        return data[(labels >= iso_label(start)) & (labels < iso_label(end))]

    def reconcile(self, data: pd.DataFrame, schema: type[Schema], *, strict: bool = False) -> pd.DataFrame:
        """Reconcile using vectorized column-wise casts from the field specs.

        Non-strict runs no per-row pydantic validation, so it scales to
        warehouse-sized frames. Strict first validates a null-safe records
        view (``NaN``/``NaT`` as ``None``, so missing numeric values pass
        nullable fields) and refuses extra or missing columns. Uses nullable
        pandas dtypes (``Int64``, ``Float64``, ``boolean``, ``string``) so
        missing values survive as ``pd.NA``.

        Args:
            data: The DataFrame to reconcile.
            schema: The schema whose field specs drive the casts.
            strict: Refuse mismatches instead of repairing them; defaults to ``False``.

        Returns:
            Reconciled DataFrame.

        Raises:
            SchemaError: If a column cannot be cast to its declared type, a
                required non-nullable column is missing, or a non-nullable
                column contains nulls.
        """
        data = _encode_json_str_columns(data, schema)
        if strict:
            schema.validate_rows(dataframe_to_records(data), strict=True)
        specs = schema.field_specs()
        dropped = set(data.columns) - {spec.name for spec in specs}
        if dropped:
            logger.warning(
                "Reconciliation to schema '%s' dropped columns not in the schema: %s", schema.__name__, sorted(dropped)
            )
        columns: dict[str, pd.Series] = {}
        for spec in specs:
            if spec.name in data.columns:
                series = data[spec.name]
            elif spec.nullable:
                series = pd.Series([None] * len(data), index=data.index, dtype=object)
            else:
                raise SchemaError(f"Reconciliation failed: required column '{spec.name}' is missing.")
            cast = _cast_series(series, spec)
            if not spec.nullable and cast.isna().any():
                raise SchemaError(f"Reconciliation failed: non-nullable column '{spec.name}' contains null values.")
            columns[spec.name] = cast
        return pd.DataFrame(columns, index=data.index)

    def infer(self, data: pd.DataFrame) -> type[Schema]:
        """Infer a Schema from the DataFrame's dtypes (no row materialization).

        ``object`` columns fall back to the type of the first non-null
        value. All fields are optional, mirroring :meth:`Schema.infer`.

        Args:
            data: The DataFrame to infer from.

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


DATAFRAME_REPRESENTATION = DataFrameRepresentation()


def _dtype_to_py_type(series: pd.Series) -> Any:
    """Map a Series' dtype to a Python type for schema inference.

    Args:
        series: The Series whose dtype is mapped.

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

    Args:
        series: The Series to parse.

    Returns:
        The parsed Series, in UTC when the input mixed offsets.
    """
    try:
        return pd.to_datetime(series, errors="raise")
    except ValueError:
        return pd.to_datetime(series, errors="raise", utc=True)


def _json_encode_value(value: Any) -> Any:
    """JSON-encode a ``list``/``dict``; pass scalars (and ``NA``) through.

    Args:
        value: The value to encode.

    Returns:
        The JSON string for a nested value, else the value unchanged.
    """
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return value


def _encode_json_str_columns(data: pd.DataFrame, schema: type[Schema]) -> pd.DataFrame:
    """JSON-encode nested values in columns declared as a scalar ``str``.

    A field typed ``str`` that receives a ``list``/``dict`` (e.g. a Facebook
    ``tracking_specs`` array or TikTok ``ad_texts``) is serialized to a JSON
    string so it satisfies the declared type — without this, ``validate``
    rejects the row and ``reconcile``'s ``astype("string")`` would store the
    Python ``repr`` instead of valid JSON. Repeated/nested fields are left
    untouched. Only ``object``-dtype columns are scanned, preserving the
    vectorized fast path; the input frame is copied only when a column is
    actually rewritten.

    Args:
        data: The DataFrame to scan.
        schema: The schema whose ``str`` fields decide what is encoded.

    Returns:
        The frame with nested ``str``-field cells JSON-encoded (the original
        when nothing needed encoding).
    """
    encoded = data
    for spec in schema.field_specs():
        if spec.repeated or spec.fields is not None or spec.type is not str:
            continue
        if spec.name not in encoded.columns:
            continue
        series = encoded[spec.name]
        if series.dtype != object or not series.map(lambda v: isinstance(v, (list, dict))).any():
            continue
        if encoded is data:
            encoded = data.copy()
        encoded[spec.name] = series.map(_json_encode_value)
    return encoded


def _cast_series(series: pd.Series, spec: FieldSpec) -> pd.Series:
    """Cast a Series to the pandas dtype matching a field spec.

    Args:
        series: The Series to cast.
        spec: The field spec whose declared type drives the cast.

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

    Args:
        data: The DataFrame to convert.

    Returns:
        Rows as a list of dicts with ``None`` for missing values.
    """
    return data.astype(object).where(pd.notnull(data), None).to_dict("records")
