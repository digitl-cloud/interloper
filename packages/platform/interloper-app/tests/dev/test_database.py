"""Tests for ``interloper_app.dev.database``: which database ``interloper dev`` keeps its state in."""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Any

import pytest
from interloper_db import engine as engine_module
from interloper_db.settings import PostgresSettings
from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.exc import OperationalError

from interloper_app.dev import database as database_module
from interloper_app.dev.database import DevDatabase

ROOT = Path("/work/acme")


def _unconfigured() -> PostgresSettings:
    return PostgresSettings.model_construct()


def _postgres(**fields: Any) -> PostgresSettings:
    # Built from these fields alone: the repository's interloper.yaml configures a Postgres of its own.
    return PostgresSettings.model_construct(**fields)


class _Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


def _bring_up(database: DevDatabase, *, files: bool = False) -> Engine:
    try:
        return database.bootstrap(files=files)
    finally:
        engine_module._engine = None


def _outdated(database: DevDatabase, *, files: bool = False) -> None:
    """Leave the database at revision 0002, holding a table no fresh one has."""
    engine = _bring_up(database, files=files)
    with engine.begin() as connection:
        connection.exec_driver_sql("UPDATE alembic_version SET version_num = '0002'")
        connection.exec_driver_sql("CREATE TABLE kept (id INTEGER)")
    engine.dispose()


def _tables(path: Path) -> list[str]:
    engine = create_engine(f"sqlite:///{path}")
    try:
        return inspect(engine).get_table_names()
    finally:
        engine.dispose()


def _revision(path: Path) -> str:
    engine = create_engine(f"sqlite:///{path}")
    try:
        with engine.connect() as connection:
            return connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one()
    finally:
        engine.dispose()


class TestUrl:
    """SQLite files in the project unless a Postgres is configured."""

    def test_without_a_postgres_the_app_s_collection_is_a_sqlite_file_in_the_project(self) -> None:
        assert DevDatabase(ROOT, _unconfigured()).url() == "sqlite:////work/acme/.interloper/interloper.db"

    def test_without_a_postgres_file_mode_has_a_file_of_its_own(self) -> None:
        assert DevDatabase(ROOT, _unconfigured()).url(files=True) == "sqlite:////work/acme/.interloper/files.db"

    def test_a_configured_postgres_holds_the_app_s_collection(self) -> None:
        postgres = _postgres(host="db", user="dev", password="secret", database="interloper")

        assert DevDatabase(ROOT, postgres).url() == "postgresql+psycopg://dev:secret@db:5432/interloper"

    def test_on_postgres_each_checkout_serves_its_files_from_a_database_of_its_own(self) -> None:
        postgres = _postgres(database="interloper")

        url = DevDatabase(ROOT, postgres).url(files=True)

        assert url.rsplit("/", 1)[1].startswith("interloper_files_")
        assert url == DevDatabase(ROOT, postgres).url(files=True)
        assert url != DevDatabase(Path("/work/acme-worktree"), postgres).url(files=True)

    def test_a_long_configured_name_keeps_file_mode_s_database_within_postgres_s_limit(self) -> None:
        name = DevDatabase(ROOT, _postgres(database="d" * 70)).url(files=True).rsplit("/", 1)[1]
        short = DevDatabase(ROOT, _postgres(database="d")).url(files=True).rsplit("/", 1)[1]

        assert len(name) == 63
        assert name.endswith(short[-15:])

    def test_the_choice_follows_the_settings_this_process_runs_with(self) -> None:
        PostgresSettings.activate(_postgres(database="configured"))
        try:
            assert DevDatabase.from_settings(ROOT).url().endswith("/configured")
        finally:
            PostgresSettings.clear_active()


class TestBootstrap:
    """The database brought up, or where it was looked for named."""

    def test_a_sqlite_database_is_brought_up_in_a_folder_git_leaves_out(self, tmp_path: Path) -> None:
        try:
            DevDatabase(tmp_path, _unconfigured()).bootstrap().dispose()
        finally:
            engine_module._engine = None

        assert (tmp_path / ".interloper" / "interloper.db").is_file()
        assert (tmp_path / ".interloper" / ".gitignore").read_text() == "*\n"

    def test_a_gitignore_already_in_the_folder_is_kept(self, tmp_path: Path) -> None:
        (tmp_path / ".interloper").mkdir()
        (tmp_path / ".interloper" / ".gitignore").write_text("*.db\n")
        try:
            DevDatabase(tmp_path, _unconfigured()).bootstrap(files=True).dispose()
        finally:
            engine_module._engine = None

        assert (tmp_path / ".interloper" / "files.db").is_file()
        assert (tmp_path / ".interloper" / ".gitignore").read_text() == "*.db\n"

    def test_a_configured_postgres_writes_nothing_in_the_project(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        brought_up: list[str] = []
        monkeypatch.setattr(database_module, "bootstrap", lambda url, **kwargs: brought_up.append(url))

        DevDatabase(tmp_path, _postgres(database="interloper")).bootstrap()

        assert brought_up == ["postgresql+psycopg://:@localhost:5432/interloper"]
        assert not (tmp_path / ".interloper").exists()

    def test_an_unreachable_postgres_is_named(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def refuse(*args: Any, **kwargs: Any) -> None:
            raise OperationalError("connect", {}, Exception("connection refused"))

        monkeypatch.setattr(database_module, "bootstrap", refuse)

        with pytest.raises(SystemExit, match=r"Postgres at localhost:5432 \(database 'interloper'\) does not answer"):
            DevDatabase(ROOT, _postgres(database="interloper")).bootstrap()

    def test_a_sqlite_file_that_cannot_be_opened_is_named(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def refuse(*args: Any, **kwargs: Any) -> None:
            raise OperationalError("connect", {}, Exception("unable to open database file"))

        monkeypatch.setattr(database_module, "bootstrap", refuse)

        with pytest.raises(SystemExit, match=r"the SQLite database in .*\.interloper cannot be opened"):
            DevDatabase(tmp_path, _unconfigured()).bootstrap()


class TestAnotherRevision:
    """A SQLite file at another revision, recreated on the person's word and kept otherwise."""

    @pytest.fixture
    def database(self, tmp_path: Path) -> DevDatabase:
        """A project whose app database is at revision 0002.

        Args:
            tmp_path: Pytest's temporary directory, the project root.

        Returns:
            The project's database.
        """
        database = DevDatabase(tmp_path, _unconfigured())
        _outdated(database)
        return database

    def test_on_yes_it_is_deleted_and_brought_up_afresh(
        self, database: DevDatabase, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        asked: list[str] = []
        monkeypatch.setattr(sys, "stdin", _Terminal())
        monkeypatch.setattr("builtins.input", lambda prompt: asked.append(prompt) or "y")

        _bring_up(database).dispose()

        head = _revision(database.path())
        assert "kept" not in _tables(database.path())
        assert head != "0002"
        assert asked == [
            (
                f"The SQLite database {database.path()} is at revision 0002, and this release needs {head}. SQLite "
                "databases are never migrated: starting afresh deletes it, and with it the components made in the "
                "app and the run history. What a spec file or a target declares is applied again on start.\nDelete "
                "it and start afresh? [y/N] "
            )
        ]

    def test_on_no_dev_stops_and_the_file_is_kept(self, database: DevDatabase, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "stdin", _Terminal())
        monkeypatch.setattr("builtins.input", lambda prompt: "n")

        with pytest.raises(SystemExit, match=r"^Error: the SQLite database .* is at revision 0002.*delete it"):
            _bring_up(database)

        assert "kept" in _tables(database.path())

    def test_without_a_terminal_dev_stops_unasked_and_the_file_is_kept(
        self, database: DevDatabase, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sys, "stdin", io.StringIO())
        monkeypatch.setattr("builtins.input", lambda prompt: pytest.fail("asked without a terminal"))

        with pytest.raises(SystemExit, match=r"^Error: the SQLite database .* is at revision 0002.*delete it"):
            _bring_up(database)

        assert "kept" in _tables(database.path())


class TestPrepare:
    """Every database dev keeps brought up before anything is served."""

    @pytest.fixture
    def brought_up(self, monkeypatch: pytest.MonkeyPatch) -> list[bool]:
        """Record which database each bring-up is for, without bringing any up.

        Args:
            monkeypatch: Pytest's monkeypatch, which replaces `DevDatabase.bootstrap`.

        Returns:
            Whether each bring-up was file mode's, in order.
        """
        calls: list[bool] = []
        engine = create_engine("sqlite://")
        monkeypatch.setattr(DevDatabase, "bootstrap", lambda self, files=False: calls.append(files) or engine)
        return calls

    def test_the_app_s_database_is_brought_up(self, tmp_path: Path, brought_up: list[bool]) -> None:
        DevDatabase(tmp_path, _unconfigured()).prepare()

        assert brought_up == [False]

    def test_file_mode_s_sqlite_file_is_brought_up_when_there_is_one(
        self, tmp_path: Path, brought_up: list[bool]
    ) -> None:
        database = DevDatabase(tmp_path, _unconfigured())
        database.path(files=True).parent.mkdir()
        database.path(files=True).touch()

        database.prepare()

        assert brought_up == [False, True]

    def test_on_postgres_file_mode_s_database_is_left_to_the_platform(
        self, tmp_path: Path, brought_up: list[bool]
    ) -> None:
        DevDatabase(tmp_path, _postgres(database="interloper")).prepare()

        assert brought_up == [False]

    def test_an_outdated_file_mode_database_is_recreated_up_front(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        database = DevDatabase(tmp_path, _unconfigured())
        _outdated(database, files=True)
        monkeypatch.setattr(sys, "stdin", _Terminal())
        monkeypatch.setattr("builtins.input", lambda prompt: "yes")

        try:
            database.prepare()
        finally:
            engine_module._engine = None

        assert "kept" not in _tables(database.path(files=True))


def test_connect_binds_the_app_s_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    connected: list[str] = []
    monkeypatch.setattr(database_module, "connect", lambda url, **kwargs: connected.append(url))

    DevDatabase(ROOT, _unconfigured()).connect()

    assert connected == ["sqlite:////work/acme/.interloper/interloper.db"]
