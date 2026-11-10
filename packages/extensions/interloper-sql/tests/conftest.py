"""Shared fixtures: a real SQLite database per test, through SQLAlchemy."""

import pytest

from interloper_sql import SQLConnection, SQLDestination


@pytest.fixture
def connection(tmp_path):
    return SQLConnection(id="sql", url=f"sqlite:///{tmp_path / 'test.db'}")


@pytest.fixture
def destination(connection):
    return SQLDestination(id="dest", connection=connection)
