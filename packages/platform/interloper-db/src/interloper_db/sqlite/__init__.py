"""SQLite, which ``interloper dev`` runs on when no Postgres is configured: what it does that Postgres does otherwise.

Postgres keeps its migrations, functions and triggers; each piece here is
SQLite's own equivalent, reached only on a SQLite URL.
"""

from interloper_db.sqlite.base import SQLiteSchema
from interloper_db.sqlite.connection import SQLiteConnection
from interloper_db.sqlite.folds import SQLiteFolds

__all__ = ["SQLiteConnection", "SQLiteFolds", "SQLiteSchema"]
