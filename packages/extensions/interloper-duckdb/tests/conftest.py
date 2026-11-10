"""Shared DuckDB fixtures: a real database file per test."""

from __future__ import annotations

from pathlib import Path

import pytest

from interloper_duckdb import DuckDBConnection, DuckDBDestination


@pytest.fixture
def connection(tmp_path: Path) -> DuckDBConnection:
    """A connection to a fresh database file under the test's temporary directory.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The connection.
    """
    return DuckDBConnection(id="duckdb", database=str(tmp_path / "test.duckdb"))


@pytest.fixture
def destination(connection: DuckDBConnection) -> DuckDBDestination:
    """A destination writing through the test's connection.

    Args:
        connection: The test's DuckDB connection.

    Returns:
        The destination.
    """
    return DuckDBDestination(id="duckdb", connection=connection)
