"""Interloper DuckDB integration: DuckDB and MotherDuck destination and connection."""

from interloper_duckdb.connection import DuckDBConnection
from interloper_duckdb.destination import DuckDBDestination

__all__ = [
    "DuckDBConnection",
    "DuckDBDestination",
]
