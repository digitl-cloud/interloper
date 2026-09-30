"""Google Sheets destination implementation."""

from __future__ import annotations

import datetime
import json
import math
import numbers
import warnings
from decimal import Decimal
from functools import cached_property
from typing import Any, get_origin
from urllib.parse import quote

import google.auth
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from interloper.destination import IOContext, destination
from interloper.destination.database import DatabaseDestination, PartitionFilter
from interloper.errors import ConfigError, DataNotFoundError
from interloper.partitioning import Partition
from interloper.representation import Representation
from interloper.resource.fields import InputField
from interloper.rest import RESTClient
from interloper.schema import FieldSpec
from interloper.utils.json import json_default, replace_non_finite

from interloper_google_cloud.connection import GoogleCloudConnection

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"

_API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
_MAX_TITLE_LENGTH = 100
_APPEND_CHUNK_ROWS = 10_000
_TIMEOUT = 60.0


def _credentials(connection: GoogleCloudConnection | None) -> Credentials:
    """Build Sheets-scoped credentials for a connection.

    Args:
        connection: The connection whose service-account key signs requests;
            ``None`` or an empty key falls back to ambient credentials
            (workload identity in-cluster).

    Returns:
        Credentials carrying the spreadsheets scope.
    """
    if connection and connection.service_account_key:
        key_info = json.loads(connection.service_account_key)
        return service_account.Credentials.from_service_account_info(key_info).with_scopes([SHEETS_SCOPE])
    credentials, _ = google.auth.default(scopes=[SHEETS_SCOPE])
    return credentials


class _SheetsAPI:
    """The Sheets REST API v4 calls one spreadsheet needs, over httpx2."""

    def __init__(self, spreadsheet_id: str, credentials: Credentials, **client_kwargs: Any) -> None:
        """Bind the API to a spreadsheet.

        Args:
            spreadsheet_id: The spreadsheet every call addresses.
            credentials: Credentials whose bearer token authorizes each call.
            **client_kwargs: Forwarded to the :class:`RESTClient` (a ``transport`` in tests).
        """
        self.spreadsheet_id = spreadsheet_id
        self._credentials = credentials
        self._path = "/" + quote(spreadsheet_id, safe="")
        self._client = RESTClient(_API_BASE, timeout=_TIMEOUT, **client_kwargs)

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Send one authorized request, refreshing the token when it has expired.

        Args:
            method: The HTTP method.
            path: The suffix to the spreadsheet's path (``""`` for the spreadsheet itself).
            **kwargs: Forwarded to the client (``params``, ``json``).

        Returns:
            The decoded JSON response body.
        """
        if not self._credentials.valid:
            self._credentials.refresh(Request())
        response = self._client.request(
            method, self._path + path, headers={"Authorization": f"Bearer {self._credentials.token}"}, **kwargs
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _range(title: str) -> str:
        """Render a tab title as a URL-safe A1 range covering the whole tab.

        Args:
            title: The tab title.

        Returns:
            The quoted, percent-encoded range.
        """
        return quote("'" + title.replace("'", "''") + "'", safe="")

    def sheet_titles(self) -> set[str]:
        """List the spreadsheet's tab titles.

        Returns:
            Every tab title.
        """
        body = self._request("GET", "", params={"fields": "sheets.properties.title"})
        return {sheet["properties"]["title"] for sheet in body.get("sheets", [])}

    def values(self, title: str) -> list[list[Any]]:
        """Read every row of a tab, header first.

        Values come back unformatted so numbers keep their precision; rows
        are ragged, the API omitting trailing empty cells.

        Args:
            title: The tab title.

        Returns:
            The tab's rows.
        """
        body = self._request("GET", f"/values/{self._range(title)}", params={"valueRenderOption": "UNFORMATTED_VALUE"})
        return body.get("values", [])

    def append(self, title: str, rows: list[list[Any]]) -> None:
        """Append rows after a tab's last row, in chunks.

        Args:
            title: The tab title.
            rows: The rows to append.
        """
        for start in range(0, len(rows), _APPEND_CHUNK_ROWS):
            self._request(
                "POST",
                f"/values/{self._range(title)}:append",
                params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"},
                json={"values": rows[start : start + _APPEND_CHUNK_ROWS]},
            )

    def rewrite(self, title: str, rows: list[list[Any]]) -> None:
        """Replace a tab's contents with rows.

        Args:
            title: The tab title.
            rows: The rows the tab holds afterwards, header first.
        """
        self._request("POST", f"/values/{self._range(title)}:clear", json={})
        self.append(title, rows)

    def add_sheet(self, title: str) -> None:
        """Add an empty tab to the spreadsheet.

        Args:
            title: The new tab's title.
        """
        self._request("POST", ":batchUpdate", json={"requests": [{"addSheet": {"properties": {"title": title}}}]})


@destination(
    key="google_sheets_destination",
    name="Google Sheets",
    icon="icon:google_sheets",
    tags=["Cloud"],
)
class GoogleSheetsDestination(DatabaseDestination):
    """Google Sheets destination.

    The spreadsheet is the database and each asset is a tab, titled after its
    table (``{dataset}.{table}`` when the asset has a dataset). The first row
    is the header, fixed when the tab is created; every later row is a record
    carrying the partition column like any other.

    Sheets has no transactions: replacing a partition deletes its rows (a
    read, then a rewrite of the rows kept) and then appends the new ones, so a
    failure in between leaves the partition empty until the next write, and
    concurrent writes to one tab are not safe.
    """

    connection: GoogleCloudConnection

    spreadsheet_id: str = InputField(
        label="Spreadsheet ID",
        description="The id in the spreadsheet's URL",
        info="Share the spreadsheet with the service account's email address (editor).",
        discriminator=True,
    )

    @cached_property
    def api(self) -> _SheetsAPI:
        """The Sheets API every read and write goes through.

        Returns:
            The API bound to the spreadsheet, cached per destination instance.
        """
        return _SheetsAPI(self.spreadsheet_id, _credentials(self.connection))

    # -- Helpers ---------------------------------------------------------------

    @staticmethod
    def _title(table: str, dataset: str | None) -> str:
        """Return the tab title an asset's table maps to.

        Args:
            table: Target table name.
            dataset: The asset's dataset, or ``None``.

        Returns:
            ``table``, or ``{dataset}.{table}`` when there is a dataset.

        Raises:
            ConfigError: If the title exceeds Sheets' 100-character limit.
        """
        title = f"{dataset}.{table}" if dataset else table
        if len(title) > _MAX_TITLE_LENGTH:
            raise ConfigError(
                f"Sheet title '{title}' is {len(title)} characters long; Google Sheets allows {_MAX_TITLE_LENGTH}."
            )
        return title

    # -- DatabaseDestination hooks ---------------------------------------------

    def insert(self, table: str, dataset: str | None, data: Any, context: IOContext) -> None:
        """Append data to its tab, creating the tab and its header on first write.

        A new tab's header is the effective schema's columns (inferred from
        the data when the context carries none); an existing tab keeps its
        own. Rows are aligned to the header: header columns the data lacks
        are left empty, and columns the header lacks are dropped with a
        warning.

        Args:
            table: Target table name.
            dataset: The asset's dataset, or ``None``.
            data: The data in its native representation.
            context: IO context carrying the effective schema.
        """
        title = self._title(table, dataset)
        view = Representation.of(data)
        schema = context.schema if context.schema is not None else view.infer()
        columns = [spec.name for spec in schema.field_specs()]

        rows: list[list[Any]] = []
        if title not in self.api.sheet_titles():
            self.api.add_sheet(title)
            existing = []
        else:
            existing = self.api.values(title)
        header = [str(cell) for cell in existing[0]] if existing else columns
        if not existing:
            rows.append(header)

        extras = [column for column in dict.fromkeys([*columns, *view.columns]) if column not in header]
        if extras:
            warnings.warn(
                f"Columns {extras} are not in the schema for '{title}' and will not be written.",
                UserWarning,
                stacklevel=2,
            )
        rows.extend([_cell(record.get(column)) for column in header] for record in view.records)
        self.api.append(title, rows)

    def delete(self, table: str, dataset: str | None, where: PartitionFilter | None) -> None:
        """Delete the rows a filter selects, or every row but the header.

        A tab that does not exist has nothing to delete, and a tab left
        unchanged by the filter is not rewritten.

        Args:
            table: Target table name.
            dataset: The asset's dataset, or ``None``.
            where: The rows to delete; ``None`` for the whole table.
        """
        title = self._title(table, dataset)
        if title not in self.api.sheet_titles():
            return
        existing = self.api.values(title)
        if not existing:
            return
        header, body = existing[0], existing[1:]
        kept = [] if where is None else [row for row in body if not _matches(header, row, where)]
        if len(kept) < len(body):
            self.api.rewrite(title, [header, *kept])

    def select(self, table: str, dataset: str | None, where: PartitionFilter | None) -> list[dict[str, Any]]:
        """Select the rows a filter selects, or every row, as records.

        Empty cells read back as ``None``; every other value comes back as
        Sheets stores it (numbers and booleans native, the rest strings).

        Args:
            table: Target table name.
            dataset: The asset's dataset, or ``None``.
            where: The rows to select; ``None`` for the whole table.

        Returns:
            The selected rows.

        Raises:
            DataNotFoundError: If the tab does not exist.
        """
        title = self._title(table, dataset)
        if title not in self.api.sheet_titles():
            raise DataNotFoundError(
                f"Sheet '{title}' does not exist in spreadsheet '{self.spreadsheet_id}'. "
                "Has the asset been materialized?"
            )
        existing = self.api.values(title)
        if not existing:
            return []
        header, body = [str(cell) for cell in existing[0]], existing[1:]
        return [
            {column: _read(row[i] if i < len(row) else "") for i, column in enumerate(header)}
            for row in body
            if where is None or _matches(header, row, where)
        ]

    def read_partition(self, context: IOContext, partition: Partition | None) -> list[dict[str, Any]]:
        """Load one partition, restoring the schema's types when the context carries one.

        Dates and decimals are stored as strings and nested values as JSON,
        so rows are reconciled against ``context.schema`` (JSON cells decoded
        first) rather than handed back as Sheets holds them.

        Args:
            context: IO context carrying the asset and the effective schema.
            partition: The partition to load, or ``None`` for the whole table.

        Returns:
            The partition's rows.
        """
        rows = super().read_partition(context, partition)
        if context.schema is None:
            return rows
        specs = context.schema.field_specs()
        json_columns = [spec.name for spec in specs if _is_json(spec)]
        for row in rows:
            for column in json_columns:
                if isinstance(row.get(column), str):
                    row[column] = json.loads(row[column])
        return context.schema.reconcile(rows)

    def count(self, table: str, dataset: str | None, column: str) -> dict[str, int]:
        """Return row counts grouped by a column's stored value.

        Args:
            table: Target table name.
            dataset: The asset's dataset, or ``None``.
            column: The column to group by.

        Returns:
            Each distinct value, as a string, to its row count.

        Raises:
            DataNotFoundError: If the tab does not exist.
        """
        title = self._title(table, dataset)
        if title not in self.api.sheet_titles():
            raise DataNotFoundError(
                f"Sheet '{title}' does not exist in spreadsheet '{self.spreadsheet_id}'. "
                "Has the asset been materialized?"
            )
        existing = self.api.values(title)
        if not existing or column not in existing[0]:
            return {}
        index = existing[0].index(column)
        counts: dict[str, int] = {}
        for row in existing[1:]:
            value = _text(row[index] if index < len(row) else "")
            counts[value] = counts.get(value, 0) + 1
        return counts


# -- Cell encoding -------------------------------------------------------------


def _cell(value: Any) -> Any:
    """Encode a value as a Sheets cell.

    Args:
        value: The value to encode.

    Returns:
        ``""`` for ``None`` and non-finite floats, booleans and numbers as
        is, decimals as strings, dates and datetimes as ISO 8601, dicts and
        lists as JSON, anything else as its string.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        number = float(value)
        return number if math.isfinite(number) else ""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(replace_non_finite(value), default=json_default)
    return str(value)


def _read(cell: Any) -> Any:
    """Decode a stored cell, an empty one reading as ``None``.

    Args:
        cell: The cell as the API returns it.

    Returns:
        The cell's value.
    """
    return None if cell == "" else cell


def _text(cell: Any) -> str:
    """Render a cell as the string it compares and groups by.

    Args:
        cell: A cell as stored or as :func:`_cell` encodes it.

    Returns:
        Strings as is, anything else as JSON (``5``, ``1.5``, ``true``).
    """
    return cell if isinstance(cell, str) else json.dumps(cell)


def _matches(header: list[Any], row: list[Any], where: PartitionFilter) -> bool:
    """Return whether a stored row falls in a partition filter.

    Bounds compare as strings, which is chronological because the partition
    column is stored as ISO 8601 (a date is a prefix of the datetimes it
    contains).

    Args:
        header: The tab's header row.
        row: A stored row, possibly ragged.
        where: The filter to apply.

    Returns:
        ``True`` if the row's partition cell matches.
    """
    if where.column not in header:
        return False
    index = header.index(where.column)
    cell = _text(row[index] if index < len(row) else "")
    if where.bounds is None:
        return cell == _text(_cell(where.value))
    start, end = where.bounds
    return _text(_cell(start)) <= cell < _text(_cell(end))


def _is_json(spec: FieldSpec) -> bool:
    """Return whether a field is stored as JSON text.

    Args:
        spec: The field spec.

    Returns:
        ``True`` for nested and repeated fields, and ``dict`` or ``list`` typed ones.
    """
    return spec.repeated or spec.fields is not None or (get_origin(spec.type) or spec.type) in (dict, list)
