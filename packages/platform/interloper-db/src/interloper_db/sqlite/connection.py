"""How every connection to a SQLite database is set up, and how its transactions begin."""

from __future__ import annotations

import sqlite3
from typing import Any, ClassVar, cast
from uuid import uuid4

from sqlalchemy import Connection, Engine, event


class SQLiteConnection:
    """The settings every connection to a SQLite database runs with, installed on its engine.

    * WAL, so reading never waits on the writer.
    * Foreign keys, which SQLite leaves off, so ``ON DELETE`` behaves as on
      Postgres.
    * A busy timeout, so a transaction waits for the write lock rather than
      failing at once.
    * Every transaction begins ``IMMEDIATE``, taking the write lock before its
      first read. SQLite ignores ``FOR UPDATE`` and ``SKIP LOCKED``, so this
      is what keeps two claims of one run, or two retries of one failure,
      from both reading it free, as Postgres's row locks do.
    * ``gen_random_uuid()``, which the models' server defaults call, as a
      function, so those defaults work unchanged.
    * ``notify(payload)`` for the notifying triggers: it holds each payload
      on the connection until `SQLiteListener` takes it at commit.
    """

    BUSY_TIMEOUT_MILLISECONDS: ClassVar[int] = 30_000
    NOTIFICATIONS: ClassVar[str] = "notifications"

    def install(self, engine: Engine) -> None:
        """Set up each connection the engine opens, and open each of its transactions.

        Args:
            engine: An engine on a SQLite database.
        """
        event.listen(engine, "connect", self.configure)
        event.listen(engine, "begin", self.begin)

    def configure(self, dbapi_connection: sqlite3.Connection, connection_record: Any) -> None:
        """Set a new connection's pragmas and functions.

        Args:
            dbapi_connection: The sqlite3 connection just opened.
            connection_record: The pool's record of it.
        """
        # sqlite3 would open its own transactions, deferred and only before a write; `begin` opens every one.
        dbapi_connection.isolation_level = None
        for pragma in ("journal_mode=WAL", "foreign_keys=ON", f"busy_timeout={self.BUSY_TIMEOUT_MILLISECONDS}"):
            dbapi_connection.execute(f"PRAGMA {pragma}")
        dbapi_connection.create_function("gen_random_uuid", 0, self.random_uuid)
        notifications: list[str] = connection_record.info.setdefault(self.NOTIFICATIONS, [])
        dbapi_connection.create_function("notify", 1, notifications.append)

    def begin(self, connection: Connection) -> None:
        """Open a transaction holding the write lock, dropping what a rolled-back one notified.

        It is sent through the driver's connection, as psycopg sends Postgres's
        ``BEGIN``, so neither shows among the statements SQLAlchemy executes.

        Args:
            connection: The connection a transaction begins on.
        """
        dbapi_connection = cast("sqlite3.Connection", connection.connection.dbapi_connection)
        # Sessions sharing one connection (a single-connection pool) join the transaction already open on it.
        if not dbapi_connection.in_transaction:
            connection.connection.info[self.NOTIFICATIONS].clear()
            dbapi_connection.execute("BEGIN IMMEDIATE")

    @staticmethod
    def random_uuid() -> str:
        """A random id, as SQLite stores the models' ids.

        Returns:
            A version 4 UUID as 32 hexadecimal digits.
        """
        return uuid4().hex
