"""Tests for ``interloper.cli.commands.db``."""

from __future__ import annotations

import argparse
from typing import Any

import interloper_db
import interloper_db.provision
import pytest

from interloper.cli.commands import db as db_command


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[Any]]:
    """Replace every ``interloper_db`` entry point the commands reach for.

    Args:
        monkeypatch: Fixture used to swap the module-level functions.

    Returns:
        Recorded arguments, keyed by function name.
    """
    recorded: dict[str, list[Any]] = {
        "ensure_database": [],
        "init_engine": [],
        "create_all": [],
        "drop_database": [],
        "upgrade": [],
        "downgrade": [],
    }
    engine = object()

    monkeypatch.setattr(interloper_db, "ensure_database", lambda dsn: recorded["ensure_database"].append(dsn))
    monkeypatch.setattr(interloper_db, "init_engine", lambda dsn: (recorded["init_engine"].append(dsn), engine)[1])
    monkeypatch.setattr(interloper_db, "create_all", lambda *a: recorded["create_all"].append(a))
    monkeypatch.setattr(interloper_db, "upgrade", lambda revision: recorded["upgrade"].append(revision))
    monkeypatch.setattr(interloper_db, "downgrade", lambda revision: recorded["downgrade"].append(revision))
    monkeypatch.setattr(
        interloper_db.provision, "drop_database", lambda dsn: recorded["drop_database"].append(dsn)
    )
    recorded["engine"] = [engine]
    return recorded


def _parse(*argv: str) -> argparse.Namespace:
    """Parse ``db`` argv through the real registration.

    Args:
        argv: The argument vector after the ``interloper`` program name.

    Returns:
        The parsed namespace, carrying the handler and requirement defaults.
    """
    parser = argparse.ArgumentParser(prog="interloper")
    db_command.register(parser.add_subparsers(dest="command"))
    return parser.parse_args(argv)


class TestRegister:
    """Parser wiring for the ``db`` command group."""

    def test_every_subcommand_declares_the_db_requirement(self) -> None:
        for argv in (["db"], ["db", "init"], ["db", "reset"], ["db", "upgrade"], ["db", "downgrade"]):
            assert _parse(*argv).requires == ["interloper_db"]

    def test_subcommands_bind_their_handlers(self) -> None:
        assert _parse("db", "init").handler is db_command._cmd_init
        assert _parse("db", "reset").handler is db_command._cmd_reset
        assert _parse("db", "upgrade").handler is db_command._cmd_upgrade
        assert _parse("db", "downgrade").handler is db_command._cmd_downgrade

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

    def test_ensures_the_database_then_creates_tables(
        self, calls: dict[str, list[Any]], capsys: pytest.CaptureFixture[str]
    ) -> None:
        db_command._cmd_init(_parse("db", "init"))

        assert len(calls["ensure_database"]) == 1
        assert calls["init_engine"] == calls["ensure_database"]
        assert calls["create_all"] == [(calls["engine"][0],)]
        assert "Done." in capsys.readouterr().out


class TestReset:
    """``interloper db reset``."""

    def test_drops_and_recreates_when_confirmed_by_flag(
        self, calls: dict[str, list[Any]], capsys: pytest.CaptureFixture[str]
    ) -> None:
        db_command._cmd_reset(_parse("db", "reset", "--yes"))

        assert len(calls["drop_database"]) == 1
        assert len(calls["ensure_database"]) == 1
        assert calls["create_all"] == [(calls["engine"][0],)]
        assert "Done." in capsys.readouterr().out

    @pytest.mark.parametrize("answer", ["y", "YES", "Yes"])
    def test_prompt_accepts_affirmative_answers(
        self, answer: str, calls: dict[str, list[Any]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("builtins.input", lambda _prompt: answer)

        db_command._cmd_reset(_parse("db", "reset"))

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
            db_command._cmd_reset(_parse("db", "reset"))

        assert excinfo.value.code == 0
        assert calls["drop_database"] == []
        assert "Aborted." in capsys.readouterr().out


class TestUpgradeDowngrade:
    """``interloper db upgrade`` / ``downgrade``."""

    def test_upgrade_targets_head_by_default(self, calls: dict[str, list[Any]]) -> None:
        db_command._cmd_upgrade(_parse("db", "upgrade"))

        assert calls["upgrade"] == ["head"]
        assert len(calls["init_engine"]) == 1

    def test_upgrade_targets_an_explicit_revision(self, calls: dict[str, list[Any]]) -> None:
        db_command._cmd_upgrade(_parse("db", "upgrade", "0007"))

        assert calls["upgrade"] == ["0007"]

    def test_downgrade_steps_back_one_by_default(self, calls: dict[str, list[Any]]) -> None:
        db_command._cmd_downgrade(_parse("db", "downgrade"))

        assert calls["downgrade"] == ["-1"]

    def test_downgrade_targets_an_explicit_revision(self, calls: dict[str, list[Any]]) -> None:
        db_command._cmd_downgrade(_parse("db", "downgrade", "base"))

        assert calls["downgrade"] == ["base"]
