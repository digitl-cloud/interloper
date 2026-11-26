"""Assets API: an asset's rows, read through its own destinations, and an editable one's rewritten whole.

Rows are reached through the asset's destinations and their connections, as
a run reaches them, never around them: they are read through the default
destination's `Destination.head`, so a read stops after the rows it needs,
and an editable `interloper.Reference`'s are rewritten in every
destination the asset has. Reading needs the viewer role, writing the
editor role.

Every read stops at `ROW_CAP` rows. A page without a search reads only up to
its own end; a searched page, and any read of an editable asset, reads up to
the cap and searches in Python, so a search matches within the first
`ROW_CAP` rows only, which the response says.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

import interloper as il
from fastapi import APIRouter, HTTPException, Query
from interloper.errors import ConfigError, DataNotFoundError, SchemaError
from interloper.representation import TableData
from interloper_db import ComponentDriftError, Store
from pydantic import BaseModel
from pydantic_core import to_jsonable_python

from interloper_api.dependencies import OrgDep, StoreDep, load_authorized

org_router = APIRouter(prefix="/assets", tags=["assets"])

ROW_CAP = 10_000
PAGE_SIZE = 100


# -- Request/Response models ---------------------------------------------------


class AssetRowsResponse(BaseModel):
    """A page of an asset's rows, every value JSON-safe.

    Dates and instants are ISO text, bytes base64 text, a missing value
    (``NaN``, ``NaT``) ``null``; an asset that declares a schema has its
    columns and its types. ``total`` counts the rows matching within what was read, exact
    unless ``truncated``: the read hit `ROW_CAP`, or, without a search, more
    rows lie beyond the page, and a surface then says "more than ``total``".
    ``version`` is an editable asset's, a hash of every row that a write
    presents to prove it replaces what it read; ``None`` for any other asset.
    """

    columns: list[str]
    rows: list[dict[str, Any]]
    total: int
    truncated: bool
    version: str | None = None


class AssetRowsRequest(BaseModel):
    """An editable asset's new rows, and the version of the rows they replace."""

    rows: list[dict[str, Any]]
    version: str


# -- Rows ----------------------------------------------------------------------


@dataclass(frozen=True)
class AssetRows:
    """One asset's rows, as the API reads them and, for an editable asset, writes them.

    Attributes:
        asset: The live asset, with its destinations bound.
    """

    asset: il.Asset

    @classmethod
    def from_store(cls, store: Store, asset_id: UUID) -> AssetRows:
        """Load the asset a component row holds.

        Args:
            store: The store the component is loaded from.
            asset_id: The component's UUID.

        Returns:
            The asset's rows.

        Raises:
            HTTPException: 404 if the component has drifted from the catalog,
                400 if it is not an asset or has no destination to read.
        """
        try:
            component = store.components.load(asset_id)
        except ComponentDriftError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        if not isinstance(component, il.Asset):
            raise HTTPException(status_code=400, detail="Component is not an asset")
        if not component.destinations:
            raise HTTPException(status_code=400, detail=f"Asset '{component.qualified_key}' has no destination")
        return cls(component)

    @property
    def editable(self) -> bool:
        """Whether the API may rewrite the asset's rows.

        Returns:
            True for an editable reference.
        """
        return isinstance(self.asset, il.Reference) and self.asset.editable

    def read(self, limit: int, offset: int, search: str | None) -> AssetRowsResponse:
        """Read one page of the rows, searched when a search is given.

        Args:
            limit: The most rows the page holds.
            offset: How many matching rows precede the page.
            search: Text one of a row's text cells must contain, case
                ignored; ``None`` or empty for every row.

        Returns:
            The page.
        """
        cap = ROW_CAP if search or self.editable else min(offset + limit, ROW_CAP)
        columns, rows = self._head(cap + 1)
        kept = rows[:cap]
        if search:
            needle = search.casefold()
            kept = [row for row in kept if self._contains(row, needle)]
        return AssetRowsResponse(
            columns=columns,
            rows=self._json(kept[offset : offset + limit]),
            total=len(kept),
            truncated=len(rows) > cap,
            version=self._version(self._json(rows)) if self.editable else None,
        )

    def write(self, rows: list[dict[str, Any]], version: str) -> AssetRowsResponse:
        """Replace an editable asset's rows in every destination of the asset.

        The rows are checked against the schema strictly (no column it does
        not declare, every required one present) and written in its types.
        An empty list empties the table: the write goes through
        `Destination.write_partition`, which, unlike `Destination.write`,
        never skips empty data.

        Args:
            rows: The table's new rows, all of them.
            version: The version of the rows the caller read, which must
                still be the table's.

        Returns:
            The first page of the new rows, with their version.

        Raises:
            HTTPException: 400 if the asset is not editable; 422 if
                there are more than `ROW_CAP` rows or a row fails the schema;
                409 if the table holds more rows than `ROW_CAP`, which a save
                would drop, or changed since it was read; 502 naming the
                destination whose write failed and those already written.
        """
        schema = self.asset.schema
        if not self.editable or schema is None:
            raise HTTPException(status_code=400, detail=f"Asset '{self.asset.qualified_key}' is not editable")
        if len(rows) > ROW_CAP:
            raise HTTPException(status_code=422, detail=f"An editable asset holds at most {ROW_CAP:,} rows")
        try:
            typed = TableData.from_data(rows).reconcile(schema, strict=True)
        except SchemaError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        _, current = self._head(ROW_CAP + 1)
        if len(current) > ROW_CAP:
            raise HTTPException(
                status_code=409,
                detail=f"The table holds more than {ROW_CAP:,} rows and cannot be edited in the app",
            )
        if self._version(self._json(current)) != version:
            raise HTTPException(status_code=409, detail="The table changed since it was read")
        context = il.IOContext(asset=self.asset, schema=schema)
        written: list[str] = []
        for destination in self._write_order():
            try:
                destination.write_partition(context, None, typed)
            except Exception as error:  # a destination failure is a 502, never a traceback
                already = ", ".join(written) or "none"
                detail = f"Writing to {self._name(destination)} failed: {error}. Already written: {already}"
                raise HTTPException(status_code=502, detail=detail) from error
            written.append(self._name(destination))
        return self.read(PAGE_SIZE, 0, None)

    # -- Internals -------------------------------------------------------------

    def _head(self, limit: int) -> tuple[list[str], list[dict[str, Any]]]:
        """Read the first rows through the default destination, shaped by the schema when the asset declares one.

        A declared schema keeps its own columns, in its order, and converts
        the values to its types leniently (`TableData.reconcile`): a day a
        warehouse reads back as an instant is a day again, and a column the
        schema no longer declares is gone, so what the preview shows, what
        the editor saves and what the version hashes are the same rows.

        Args:
            limit: The most rows to read.

        Returns:
            The columns, the schema's when the asset declares one, else the
            data's; and the rows, in their types. No rows for a table never
            written.

        Raises:
            HTTPException: 400 if the destination cannot preview rows; 502
                with the destination's message if the read fails.
        """
        schema = self.asset.schema
        try:
            table = TableData.from_data(
                self.asset.default_destination.head(il.IOContext(asset=self.asset, schema=schema), limit)
            )
            if schema is not None:
                table = TableData.from_data(table.reconcile(schema))
            columns, rows = table.columns, table.rows
        except DataNotFoundError:
            columns, rows = [], []
        except ConfigError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error
        except Exception as error:  # a destination failure is a 502, never a traceback
            raise HTTPException(status_code=502, detail=str(error)) from error
        if schema is not None:
            columns = [spec.name for spec in schema.field_specs()]
        return columns, rows

    def _write_order(self) -> list[il.Destination]:
        """The asset's destinations in the order a save writes them: the default, which reads come from, last.

        A save that fails part way then leaves the rows the app reads, and
        their version, as they were, so saving again is safe.

        Returns:
            The destinations, the default last.
        """
        default = self.asset.default_destination
        return sorted(self.asset.destinations, key=lambda destination: destination is default)

    @staticmethod
    def _name(destination: il.Destination) -> str:
        """Name a destination in a message: its key, and its id when it has one.

        Args:
            destination: The destination.

        Returns:
            The name.
        """
        return f"'{destination.key}' ({destination.id})" if destination.id else f"'{destination.key}'"

    @classmethod
    def _json(cls, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert rows to JSON-safe values.

        Days and instants become ISO text, ``NaN`` null, bytes base64 text,
        numpy values Python ones.

        Args:
            rows: The rows, in their types.

        Returns:
            The rows, JSON-safe.

        Raises:
            HTTPException: 502 if a value cannot be converted.
        """
        try:
            return [
                to_jsonable_python(row, inf_nan_mode="null", bytes_mode="base64", fallback=cls._plain) for row in rows
            ]
        except Exception as error:  # a value JSON cannot carry is a 502, never a traceback
            raise HTTPException(status_code=502, detail=str(error)) from error

    @staticmethod
    def _plain(value: Any) -> Any:
        """Stand in for a value JSON has no type for: a numpy scalar or array as Python, anything else as text.

        Args:
            value: The value the serializer does not know.

        Returns:
            A value it does.
        """
        return value.tolist() if hasattr(value, "tolist") else str(value)

    @staticmethod
    def _contains(row: dict[str, Any], needle: str) -> bool:
        """Whether one of a row's text cells contains the searched text.

        Args:
            row: The row, its values typed.
            needle: The searched text, casefolded.

        Returns:
            True when a text cell contains it, case ignored.
        """
        return any(isinstance(value, str) and needle in value.casefold() for value in row.values())

    @staticmethod
    def _version(rows: list[dict[str, Any]]) -> str:
        """A table's version: what its rows hold, whatever their order.

        Args:
            rows: Every row, JSON-safe.

        Returns:
            The first 16 hex characters of the sha256 of the rows' canonical
            JSON, keys sorted and rows sorted by their JSON text.
        """
        texts = sorted(json.dumps(row, sort_keys=True) for row in rows)
        return hashlib.sha256(f"[{','.join(texts)}]".encode()).hexdigest()[:16]


# -- Routes --------------------------------------------------------------------


@org_router.get("/{asset_id}/rows")
def get_asset_rows(
    asset_id: UUID,
    scope: OrgDep,
    store: StoreDep,
    limit: Annotated[int, Query(ge=1, le=ROW_CAP)] = PAGE_SIZE,
    offset: Annotated[int, Query(ge=0)] = 0,
    search: str | None = None,
) -> AssetRowsResponse:
    """Read one page of an asset's rows.

    Args:
        asset_id: The asset component's UUID.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.
        limit: The most rows the page holds.
        offset: How many matching rows precede the page.
        search: Text one of a row's text cells must contain, case ignored.

    Returns:
        The page.
    """
    load_authorized(store.components.get, asset_id, scope, label="Asset")
    return AssetRows.from_store(store, asset_id).read(limit, offset, search)


@org_router.put("/{asset_id}/rows")
def put_asset_rows(asset_id: UUID, body: AssetRowsRequest, scope: OrgDep, store: StoreDep) -> AssetRowsResponse:
    """Replace an editable asset's rows in every destination of the asset.

    Args:
        asset_id: The asset component's UUID.
        body: The new rows and the version of the rows they replace.
        scope: The organisation the path names and the caller's role there.
        store: The Store instance.

    Returns:
        The first page of the new rows, with their version.
    """
    load_authorized(store.components.get, asset_id, scope, label="Asset", minimum="editor")
    return AssetRows.from_store(store, asset_id).write(body.rows, body.version)
