"""Tests for ``interloper.cli.main``."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from interloper.cli import main as cli_main
from interloper.registry import Registry
from interloper.settings import AppSettings


@pytest.fixture(autouse=True)
def _no_active_settings() -> Iterator[None]:
    """Leave no activated settings behind for the rest of the suite.

    Yields:
        ``None``; the teardown clears the active settings.
    """
    yield
    AppSettings.clear_active()


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
        monkeypatch.setattr(
            "interloper.cli.commands.run._cmd_run",
            seen.append,
        )

        cli_main.main()

        assert len(seen) == 1
        assert seen[0].target == ["pkg.Asset"]

    def test_activates_settings_for_the_duration_of_the_command(self, monkeypatch: pytest.MonkeyPatch) -> None:
        active: list[AppSettings | None] = []
        monkeypatch.setattr(sys, "argv", ["interloper", "run", "pkg.Asset"])
        monkeypatch.setattr(
            "interloper.cli.commands.run._cmd_run",
            lambda args: active.append(AppSettings._active),
        )

        cli_main.main()

        assert active[0] is not None
        # The activation is scoped to the command: nothing leaks to the process.
        assert AppSettings._active is None

    def test_telemetry_is_shut_down_even_when_the_handler_raises(
        self, monkeypatch: pytest.MonkeyPatch, isolated_process_state: dict[str, list[bool]]
    ) -> None:
        monkeypatch.setattr(sys, "argv", ["interloper", "run", "pkg.Asset"])
        monkeypatch.setattr(
            "interloper.cli.commands.run._cmd_run",
            lambda args: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        with pytest.raises(RuntimeError, match="boom"):
            cli_main.main()

        assert isolated_process_state["init"] == [True]
        assert isolated_process_state["shutdown"] == [True]
        assert AppSettings._active is None


class TestBuildParser:
    """The parser the CLI runs on, and the documentation renders from."""

    def test_the_parser_holds_every_installed_command_sorted(self) -> None:
        parser = cli_main.build_parser()
        subparsers = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))

        assert list(subparsers.choices) == sorted(subparsers.choices)
        assert {"app", "db", "launch", "run"} <= set(subparsers.choices)

    def test_naming_a_command_builds_that_command_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def unrelated(_subparsers: Any) -> None:
            raise AssertionError("an unrelated command was built")

        commands: Registry[Any] = Registry()
        commands.register("run", lambda subparsers: subparsers.add_parser("run", description="Run."))
        commands.register("api", unrelated)
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
        parser = cli_main.build_parser()
        commands = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction)).choices

        for name, command in commands.items():
            assert command.description, f"{name} has no description"
            for action in command._actions:
                if not isinstance(action, argparse._HelpAction | argparse._SubParsersAction):
                    assert action.help, f"{name} {action.dest} has no help"
