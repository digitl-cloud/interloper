"""Interloper SQL integration: a SQLAlchemy-backed destination and its connection."""

from interloper_sql.connection import SQLConnection
from interloper_sql.destination import SQLDestination

__all__ = [
    "SQLConnection",
    "SQLDestination",
]
