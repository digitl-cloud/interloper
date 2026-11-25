"""Paging: the one shape every store listing takes and returns.

A listing is filtered by a query object and answered with a :class:`Page`.
Query objects extend :class:`PageQuery`, and the API binds them straight to
query-string parameters, so a filter is declared once for both. ``limit`` is
capped for every caller that reaches the store through HTTP; only in-process
callers can pass ``None`` and read the whole matching set.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field
from sqlmodel import Session, func, select
from sqlmodel.sql.expression import Select, SelectOfScalar

T = TypeVar("T")
U = TypeVar("U")

MAX_PAGE_SIZE = 500


class PageQuery(BaseModel):
    """The window of a listing: how many rows, from which offset.

    ``limit=None`` reads every matching row. Query strings cannot carry
    ``None``, so it is an in-process option only.

    This and the query objects extending it carry a reader's own choices,
    bound from a query string or a tool's parameters and passed through.
    Outside interloper-db, code never assembles one to ask a question of its
    own: a question the code asks is a named store method.
    """

    limit: int | None = Field(default=50, ge=1, le=MAX_PAGE_SIZE)
    offset: int = Field(default=0, ge=0)


class Page(BaseModel, Generic[T]):
    """One window of a listing, with the size of the whole matching set."""

    items: list[T]
    total: int

    @classmethod
    def read(cls, session: Session, statement: Select[Any] | SelectOfScalar[Any], query: PageQuery) -> Page[Any]:
        """Run a listing statement over the window *query* asks for.

        The total is counted over the same statement without its ordering and
        window, so the two can never disagree. A page that came back short
        already knows it: the matching set ends inside it, so the total is
        its offset plus its rows and the count is skipped.

        Args:
            session: Open session the statement runs in.
            statement: The filtered, ordered selection of the listing: entities
                come back as entities, columns as rows.
            query: The window to read.

        Returns:
            The page of rows.
        """
        window = statement.offset(query.offset).limit(query.limit)
        if any(description["entity"] is not None for description in statement.column_descriptions):
            items = list(session.exec(window).all())
        else:
            # Columns alone are read off the connection: the ORM adds nothing to them but a cost per row.
            items = list(session.connection().execute(window).all())
        if (query.limit is None or len(items) < query.limit) and (items or query.offset == 0):
            return cls(items=items, total=query.offset + len(items))
        counted = select(func.count()).select_from(statement.order_by(None).subquery())
        return cls(items=items, total=session.exec(counted).one())

    @classmethod
    def window(cls, items: list[Any], query: PageQuery) -> Page[Any]:
        """Window a listing that is assembled in memory rather than queried.

        Args:
            items: Every matching item, already in listing order.
            query: The window to keep.

        Returns:
            The page of items.
        """
        end = None if query.limit is None else query.offset + query.limit
        return cls(items=items[query.offset : end], total=len(items))

    def map(self, convert: Callable[[T], U]) -> Page[U]:
        """The same page with every item converted, typically into its response model.

        Args:
            convert: The conversion applied to each item.

        Returns:
            A page of the converted items and the same total.
        """
        return Page[U](items=[convert(item) for item in self.items], total=self.total)
