"""Tests for ``interloper_db.cli``."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from interloper_db import cli as db_command
from interloper_db.settings import PostgresSettings


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
    """Replace every database procedure the commands reach for.

    Args:
        monkeypatch: Fixture used to swap the module-level names.

    Returns:
        Recorded arguments, keyed by procedure name.
    """
    recorded: dict[str, list[Any]] = {
        "bootstrap": [],
        "init_engine": [],
        "drop_database": [],
        "upgrade": [],
        "downgrade": [],
    }
    monkeypatch.setattr(db_command, "bootstrap", lambda dsn, **kwargs: recorded["bootstrap"].append((dsn, kwargs)))
    monkeypatch.setattr(db_command, "init_engine", lambda dsn: recorded["init_engine"].append(dsn))
    monkeypatch.setattr(db_command, "drop_database", lambda dsn: recorded["drop_database"].append(dsn))
    monkeypatch.setattr(db_command, "upgrade", lambda revision: recorded["upgrade"].append(revision))
    monkeypatch.setattr(db_command, "downgrade", lambda revision: recorded["downgrade"].append(revision))
    return recorded


def _parse(*argv: str) -> argparse.Namespace:
    """Parse ``db`` argv through the real registration.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace, carrying the handler.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    db_command.DatabaseCommand().attach(parser.add_subparsers(dest="command"), "db")
    return parser.parse_args(argv)


class TestCommands:
    """Parser wiring for the ``db`` command group."""

    def test_subcommands_bind_their_handlers(self) -> None:
        assert isinstance(_parse("db", "init").handler.__self__, db_command.DatabaseInitCommand)
        assert isinstance(_parse("db", "reset").handler.__self__, db_command.DatabaseResetCommand)
        assert isinstance(_parse("db", "upgrade").handler.__self__, db_command.DatabaseUpgradeCommand)
        assert isinstance(_parse("db", "downgrade").handler.__self__, db_command.DatabaseDowngradeCommand)

    def test_bare_db_prints_its_help(self, capsys: pytest.CaptureFixture[str]) -> None:
        args = _parse("db")

        args.handler(args)

        out = capsys.readouterr().out
        assert "usage: interloper db" in out
        assert "upgrade" in out

    def test_revision_defaults(self) -> None:
        assert _parse("db", "upgrade").revision == "head"
        assert _parse("db", "downgrade").revision == "-1"

    def test_explicit_revision_is_taken(self) -> None:
        assert _parse("db", "upgrade", "abc123").revision == "abc123"
        assert _parse("db", "downgrade", "base").revision == "base"

    def test_reset_confirmation_flag(self) -> None:
        assert _parse("db", "reset").yes is False
        assert _parse("db", "reset", "-y").yes is True


class TestInit:
    """``interloper db init``."""

    def test_bootstraps_the_database(self, calls: dict[str, list[Any]], capsys: pytest.CaptureFixture[str]) -> None:
        db_command.DatabaseInitCommand().run(_parse("db", "init"))

        assert calls["bootstrap"] == [(PostgresSettings.get().dsn, {})]
        assert "Done." in capsys.readouterr().out

    def test_reads_the_postgres_settings_alone(
        self, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        postgres = PostgresSettings(host="db", database="x")
        PostgresSettings.activate(postgres)

        db_command.DatabaseInitCommand().run(_parse("db", "init"))

        assert calls["bootstrap"] == [(postgres.dsn, {})]

    def test_init_never_prints_the_password(
        self,
        calls: dict[str, list[Any]],
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        PostgresSettings.activate(PostgresSettings(user="me", password="s3cret-pw", host="db", database="x"))

        db_command.DatabaseInitCommand().run(_parse("db", "init"))

        out = capsys.readouterr().out
        assert "s3cret-pw" not in out
        assert "db:5432/x" in out


class TestReset:
    """``interloper db reset``."""

    def test_drops_and_bootstraps_when_confirmed_by_flag(
        self, calls: dict[str, list[Any]], capsys: pytest.CaptureFixture[str]
    ) -> None:
        db_command.DatabaseResetCommand().run(_parse("db", "reset", "--yes"))

        assert len(calls["drop_database"]) == 1
        assert len(calls["bootstrap"]) == 1
        assert "Done." in capsys.readouterr().out

    def test_reset_never_prints_the_password(
        self,
        calls: dict[str, list[Any]],
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        PostgresSettings.activate(PostgresSettings(user="me", password="s3cret-pw", host="db", database="x"))
        prompts: list[str] = []
        monkeypatch.setattr("builtins.input", lambda prompt: prompts.append(prompt) or "y")

        db_command.DatabaseResetCommand().run(_parse("db", "reset"))

        assert "s3cret-pw" not in prompts[0] + capsys.readouterr().out
        assert "db:5432/x" in prompts[0]

    @pytest.mark.parametrize("answer", ["y", "YES", "Yes"])
    def test_prompt_accepts_affirmative_answers(
        self, answer: str, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _prompt: answer)

        db_command.DatabaseResetCommand().run(_parse("db", "reset"))

        assert len(calls["drop_database"]) == 1

    @pytest.mark.parametrize("answer", ["", "n", "no", "maybe"])
    def test_anything_else_aborts_without_touching_the_database(
        self,
        answer: str,
        calls: dict[str, list[Any]],
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _prompt: answer)

        with pytest.raises(SystemExit) as excinfo:
            db_command.DatabaseResetCommand().run(_parse("db", "reset"))

        assert excinfo.value.code == 0
        assert calls["drop_database"] == []
        assert "Aborted." in capsys.readouterr().out


class TestUpgradeDowngrade:
    """``interloper db upgrade`` / ``downgrade``."""

    def test_upgrade_targets_head_by_default(self, calls: dict[str, list[Any]]) -> None:
        db_command.DatabaseUpgradeCommand().run(_parse("db", "upgrade"))

        assert calls["upgrade"] == ["head"]
        assert len(calls["init_engine"]) == 1

    def test_upgrade_targets_an_explicit_revision(self, calls: dict[str, list[Any]]) -> None:
        db_command.DatabaseUpgradeCommand().run(_parse("db", "upgrade", "0007"))

        assert calls["upgrade"] == ["0007"]

    def test_downgrade_steps_back_one_by_default(self, calls: dict[str, list[Any]]) -> None:
        db_command.DatabaseDowngradeCommand().run(_parse("db", "downgrade"))

        assert calls["downgrade"] == ["-1"]

    def test_downgrade_targets_an_explicit_revision(self, calls: dict[str, list[Any]]) -> None:
        db_command.DatabaseDowngradeCommand().run(_parse("db", "downgrade", "base"))

        assert calls["downgrade"] == ["base"]
