"""Hand out the changes a SQLite database notifies, as Postgres's ``LISTEN`` receives them."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import JSON, Column, Connection, Engine, event
from sqlalchemy.dialects import sqlite
from sqlmodel import SQLModel

from interloper_db.sqlite.connection import SQLiteConnection

logger = logging.getLogger(__name__)


class SQLiteListener:
    """``LISTEN table_changes`` on SQLite: hand each change to a callback as its transaction commits.

    Values are rendered as Postgres's JSON renders them. Only this process's
    writes are heard, which under ``interloper dev`` is every write.
    """

    def __init__(self, engine: Engine, callback: Callable[[dict[str, Any]], None]) -> None:
        """Listen to the transactions the engine commits.

        Args:
            engine: An engine on a SQLite database, set up by `SQLiteConnection`.
            callback: Called with each change, in the committing thread.
        """
        self._callback = callback
        self._dialect = sqlite.dialect()
        event.listen(engine, "commit", self._commit)

    def decode(self, payload: str) -> dict[str, Any]:
        """Turn a notified payload into the payload Postgres sends for the same change.

        Args:
            payload: The JSON a trigger notified.

        Returns:
            The change, its organisation and record values rendered as Postgres renders them.
        """
        change = json.loads(payload)
        table = SQLModel.metadata.tables[change["table"]]
        change["org_id"] = self._render(table.c.org_id, change["org_id"])
        change["record"] = {
            name: self._render(table.c[name], value) if name in table.c else value
            for name, value in change["record"].items()
        }
        return change

    # -- Internals -------------------------------------------------------------

    def _commit(self, connection: Connection) -> None:
        """Hand out what the transaction committing on a connection notified.

        A failing change is logged and skipped: raising here would fail the
        commit itself.

        Args:
            connection: The connection whose transaction commits.
        """
        notifications: list[str] = connection.connection.info[SQLiteConnection.NOTIFICATIONS]
        for payload in notifications:
            try:
                self._callback(self.decode(payload))
            except Exception:
                logger.exception("A change notified on SQLite could not be handed out; skipped")
        notifications.clear()

    def _render(self, column: Column[Any], value: Any) -> Any:
        """Read a stored value through its column's type, then render it as Postgres's JSON does.

        Args:
            column: The column the value was stored in.
            value: The value as SQLite stored it; a JSON column's arrives nested already.

        Returns:
            The value as Postgres renders it in JSON.
        """
        if value is None or isinstance(column.type, JSON):
            return value
        processor = column.type.dialect_impl(self._dialect).result_processor(self._dialect, None)
        value = processor(value) if processor is not None else value
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value
