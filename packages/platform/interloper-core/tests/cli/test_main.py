"""Tests for ``interloper.cli.main``."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest

from interloper.cli import Command
from interloper.cli import main as cli_main
from interloper.cli.commands.run import RunCommand
from interloper.extension import ExtensionEntry, Manifests
from interloper.registry import Registry
from interloper.settings import ServerSettings, Settings


class TestLoadDotenv:
    """``.env`` loading is resolved from the working directory, not the package."""

    def test_reads_env_from_the_working_directory(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        (tmp_path / ".env").write_text("INTERLOPER_CLI_DOTENV_PROBE=from-cwd\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("INTERLOPER_CLI_DOTENV_PROBE", raising=False)

        cli_main._load_dotenv()

        import os

        assert os.environ["INTERLOPER_CLI_DOTENV_PROBE"] == "from-cwd"
        monkeypatch.delenv("INTERLOPER_CLI_DOTENV_PROBE", raising=False)

    def test_missing_dotenv_dependency_is_tolerated(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # An install without python-dotenv must still start the CLI.
        monkeypatch.setitem(sys.modules, "dotenv", None)
        cli_main._load_dotenv()


class TestMain:
    """End-to-end argv dispatch."""

    @pytest.fixture(autouse=True)
    def isolated_process_state(self, monkeypatch: pytest.MonkeyPatch) -> dict[str, list[bool]]:
        """Keep ``main`` from reaching the two pieces of global state it owns.

        ``interloper.yaml`` enables otel, so a real ``init_telemetry`` would
        replace the process-wide providers the telemetry suites assert
        against, and ``_load_dotenv`` would inject the repo's ``.env`` into
        ``os.environ`` for every test that follows.

        Args:
            monkeypatch: Fixture used to swap the two entry points.

        Returns:
            The recorded telemetry calls.
        """
        recorded: dict[str, list[bool]] = {"init": [], "shutdown": []}
        monkeypatch.setattr(cli_main, "_load_dotenv", lambda: None)
        monkeypatch.setattr("interloper.telemetry.init_telemetry", lambda settings: recorded["init"].append(True))
        monkeypatch.setattr("interloper.telemetry.shutdown_telemetry", lambda: recorded["shutdown"].append(True))
        return recorded

    def test_bare_invocation_prints_help_and_exits_zero(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setattr(sys, "argv", ["interloper"])

        with pytest.raises(SystemExit) as excinfo:
            cli_main.main()

        assert excinfo.value.code == 0
        assert "usage: interloper" in capsys.readouterr().out

    def test_dispatches_to_the_command_handler(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[argparse.Namespace] = []
        monkeypatch.setattr(sys, "argv", ["interloper", "run", "pkg.Asset"])
        monkeypatch.setattr(RunCommand, "run", lambda self, args: seen.append(args))

        cli_main.main()

        assert len(seen) == 1
        assert seen[0].target == ["pkg.Asset"]

    def test_the_settings_a_command_ran_with_are_forgotten_when_it_ends(self, monkeypatch: pytest.MonkeyPatch) -> None:
        server = ServerSettings(port=4321)
        monkeypatch.setattr(sys, "argv", ["interloper", "run", "pkg.Asset"])
        monkeypatch.setattr(RunCommand, "run", lambda self, args: ServerSettings.activate(server))

        cli_main.main()

        assert ServerSettings.get() is not server

    def test_telemetry_is_shut_down_even_when_the_handler_raises(
        self, monkeypatch: pytest.MonkeyPatch, isolated_process_state: dict[str, list[bool]]
    ) -> None:
        monkeypatch.setattr(sys, "argv", ["interloper", "run", "pkg.Asset"])
        monkeypatch.setattr(RunCommand, "run", lambda self, args: (_ for _ in ()).throw(RuntimeError("boom")))

        with pytest.raises(RuntimeError, match="boom"):
            cli_main.main()

        assert isolated_process_state["init"] == [True]
        assert isolated_process_state["shutdown"] == [True]
        assert not Settings._active


class TestBuildParser:
    """The parser the CLI runs on, and the documentation renders from."""

    def test_the_parser_lists_every_installed_command_sorted(self) -> None:
        parser = cli_main.build_parser()
        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))

        assert list(subparsers.choices) == sorted(subparsers.choices)
        assert {"app", "db", "extension", "launch", "run", "scheduler"} <= set(subparsers.choices)

    def test_the_list_names_the_declared_commands_without_importing_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        declared = [
            ExtensionEntry("acme", "acme", "cli", name, f"acme.missing:{name.title()}Command") for name in ("b", "a")
        ]
        manifests = mock.Mock(spec=Manifests)
        manifests.entries.return_value = declared
        monkeypatch.setattr("interloper.registry.base.MANIFESTS", manifests)
        monkeypatch.setattr(cli_main, "COMMANDS", Registry("cli"))

        parser = cli_main.build_parser()

        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        assert list(subparsers.choices) == ["a", "b"]
        manifests.record.assert_not_called()

    def test_naming_a_command_builds_that_command_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class Named(Command):
            """Run."""

        class Unrelated(Command):
            """Serve."""

            def add_arguments(self, parser: argparse.ArgumentParser) -> None:
                raise AssertionError("an unrelated command was built")

        commands: Registry[type[Command]] = Registry()
        commands.register("run", Named)
        commands.register("api", Unrelated)
        monkeypatch.setattr(cli_main, "COMMANDS", commands)

        parser = cli_main.build_parser("run")

        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        assert list(subparsers.choices) == ["run"]

    def test_an_unknown_command_builds_every_command_for_the_usage_error(self) -> None:
        parser = cli_main.build_parser("not-a-command")

        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        assert {"db", "run"} <= set(subparsers.choices)

    def test_main_does_not_know_any_command(self) -> None:
        import inspect

        source = inspect.getsource(cli_main)
        assert "requires" not in source
        assert '"app"' not in source

    def test_every_command_and_flag_carries_user_facing_copy(self) -> None:
        for name in cli_main.COMMANDS:
            parser = cli_main.build_parser(name)
            command = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction)).choices[name]

            assert command.description, f"{name} has no description"
            for action in command._actions:
                if not isinstance(action, argparse._HelpAction | argparse._SubParsersAction):
                    assert action.help, f"{name} {action.dest} has no help"


class TestHelp:
    """What printing the help costs: the list imports no command, a command's help imports only its own."""

    @pytest.mark.parametrize("argv", [["--help"], ["run", "--help"], ["launch", "--help"]])
    def test_the_help_loads_neither_the_api_nor_the_database(
        self, argv: list[str], help_modules: Callable[..., set[str]]
    ) -> None:
        assert {"fastapi", "sqlalchemy", "interloper_api"}.isdisjoint(help_modules(*argv))
