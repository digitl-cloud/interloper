"""File formats for the GCS destination: records ⇄ bytes, one class per format."""

from __future__ import annotations

import csv
import datetime
import io
import json
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
from interloper.schema import FieldSpec

from interloper_google_cloud.serialization import json_default, replace_non_finite


class FileFormat(ABC):
    """Serialization strategy for one file format.

    ``serialize`` receives records (and the effective field specs, when a
    schema is known) and returns the object payload; ``deserialize`` parses a
    payload back into records.  Type restoration from text formats is the
    caller's concern (``Schema.reconcile``) — formats only deal in bytes.
    """

    key: ClassVar[str]
    extension: ClassVar[str]
    content_type: ClassVar[str]

    @abstractmethod
    def serialize(self, rows: list[dict[str, Any]], specs: list[FieldSpec] | None) -> bytes:
        """Serialize records into the format's byte payload."""

    @abstractmethod
    def deserialize(self, payload: bytes) -> list[dict[str, Any]]:
        """Parse a byte payload back into records."""


class JSONLFormat(FileFormat):
    """Newline-delimited JSON — the load format BigQuery and most lakes expect."""

    key = "jsonl"
    extension = "jsonl"
    content_type = "application/jsonl"

    def serialize(self, rows: list[dict[str, Any]], specs: list[FieldSpec] | None) -> bytes:
        """Serialize records as one JSON object per line.

        Non-finite floats become ``null`` (invalid JSON otherwise); dates and
        decimals serialize via :func:`json_default`.

        Returns:
            The NDJSON payload.
        """
        lines = (json.dumps(replace_non_finite(row), default=json_default) for row in rows)
        return "\n".join(lines).encode()

    def deserialize(self, payload: bytes) -> list[dict[str, Any]]:
        """Parse NDJSON lines into records.

        Returns:
            One dict per non-empty line.
        """
        return [json.loads(line) for line in payload.decode().splitlines() if line.strip()]


class CSVFormat(FileFormat):
    """CSV with a header row; every value is stored as a string."""

    key = "csv"
    extension = "csv"
    content_type = "text/csv"

    def serialize(self, rows: list[dict[str, Any]], specs: list[FieldSpec] | None) -> bytes:
        """Serialize records as CSV (the first row's keys are the headers).

        Returns:
            The CSV payload; empty for no rows.
        """
        if not rows:
            return b""
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        return buffer.getvalue().encode()

    def deserialize(self, payload: bytes) -> list[dict[str, Any]]:
        """Parse CSV into records, reading empty cells as ``None``.

        Returns:
            One dict per data row.
        """
        reader = csv.DictReader(io.StringIO(payload.decode()))
        return [{k: (None if v == "" else v) for k, v in row.items()} for row in reader]


class ParquetFormat(FileFormat):
    """Parquet via pyarrow — full type fidelity, ideal for external tables."""

    key = "parquet"
    extension = "parquet"
    content_type = "application/vnd.apache.parquet"

    def serialize(self, rows: list[dict[str, Any]], specs: list[FieldSpec] | None) -> bytes:
        """Serialize records as a Parquet file.

        With field specs, the Arrow schema is built from them so the file
        shape is deterministic and stable across partitions; without, pyarrow
        infers it from the data.

        Returns:
            The Parquet payload.
        """
        if specs is not None:
            rows = _stringify_untyped(rows, specs)
            table = pa.Table.from_pylist(rows, schema=pa.schema([_spec_to_arrow_field(spec) for spec in specs]))
        else:
            table = pa.Table.from_pylist(rows)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        return buffer.getvalue().to_pybytes()

    def deserialize(self, payload: bytes) -> list[dict[str, Any]]:
        """Read a Parquet payload into records (types preserved).

        Returns:
            One dict per row.
        """
        return pq.read_table(pa.BufferReader(payload)).to_pylist()


FORMATS: dict[str, FileFormat] = {fmt.key: fmt for fmt in (ParquetFormat(), JSONLFormat(), CSVFormat())}


def _spec_to_arrow_field(spec: FieldSpec) -> pa.Field:
    """Map a field spec to an Arrow field (nested records as structs).

    Returns:
        The Arrow field, wrapped in ``list_`` when the spec is repeated.
    """
    if spec.fields is not None:
        arrow_type: pa.DataType = pa.struct([_spec_to_arrow_field(sub) for sub in spec.fields])
    else:
        arrow_type = _py_type_to_arrow_type(spec.type)
    if spec.repeated:
        arrow_type = pa.list_(arrow_type)
    return pa.field(spec.name, arrow_type, nullable=spec.nullable)


def _is_concrete_type(py_type: Any) -> bool:
    """Return whether *py_type* is a real class the Arrow mapping can use.

    ``typing.Any`` is excluded explicitly — on Python 3.11+ it *is* an
    ``isinstance(..., type)``, but it types nothing.
    """
    return py_type is not Any and isinstance(py_type, type)


def _py_type_to_arrow_type(py_type: Any) -> pa.DataType:
    """Map a Python *type* (from a FieldSpec) to an Arrow type.

    Mirrors the BigQuery mapping (``Decimal`` → NUMERIC precision/scale) so
    parquet files load into BigQuery without casts.
    """
    if not _is_concrete_type(py_type):
        return pa.string()  # typing.Any or unresolvable annotations
    if issubclass(py_type, bool):
        return pa.bool_()
    if issubclass(py_type, int):
        return pa.int64()
    if issubclass(py_type, float):
        return pa.float64()
    if issubclass(py_type, Decimal):
        return pa.decimal128(38, 9)
    if issubclass(py_type, datetime.datetime):
        return pa.timestamp("us")
    if issubclass(py_type, datetime.date):
        return pa.date32()
    if issubclass(py_type, bytes):
        return pa.binary()
    return pa.string()


def _stringify_untyped(rows: list[dict[str, Any]], specs: list[FieldSpec]) -> list[dict[str, Any]]:
    """Stringify values of untyped (``Any``) scalar fields.

    Those fields map to Arrow strings, and ``from_pylist`` does not coerce
    e.g. ints into a string column — pre-convert so the write cannot fail on
    a column the schema could not type.

    Returns:
        The rows, copied only when a conversion applies.
    """
    untyped = {
        spec.name for spec in specs if spec.fields is None and not spec.repeated and not _is_concrete_type(spec.type)
    }
    if not untyped:
        return rows
    return [
        {k: (str(v) if k in untyped and v is not None and not isinstance(v, str) else v) for k, v in row.items()}
        for row in rows
    ]
