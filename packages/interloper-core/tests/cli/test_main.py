"""Tests for ``interloper.cli.main``."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from interloper.cli import main as cli_main
from interloper.settings import AppSettings


@pytest.fixture(autouse=True)
def _no_active_settings() -> Iterator[None]:
    """Leave no activated settings behind for the rest of the suite.

    Yields:
        ``None``; the teardown clears the active settings.
    """
    yield
    AppSettings.clear_active()


class TestHasPackage:
    """Importability probe backing the ``requires`` declarations."""

    def test_importable_package(self) -> None:
        assert cli_main._has_package("interloper") is True

    def test_missing_package(self) -> None:
        assert cli_main._has_package("interloper_not_a_package") is False


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


class TestEnforceRequires:
    """Command requirement validation."""

    def test_satisfied_requirements_pass(self) -> None:
        args = argparse.Namespace(command="run", requires=["interloper"])
        cli_main._enforce_requires(args)

    def test_missing_requirement_exits_naming_the_package(self) -> None:
        args = argparse.Namespace(command="db", requires=["interloper_missing"])
        with pytest.raises(SystemExit, match=r"command 'db' requires package\(s\): interloper_missing"):
            cli_main._enforce_requires(args)

    def test_requires_any_passes_when_one_is_present(self) -> None:
        args = argparse.Namespace(command="app", requires_any=[["interloper_missing", "interloper"]])
        cli_main._enforce_requires(args)

    def test_requires_any_exits_when_the_whole_group_is_missing(self) -> None:
        args = argparse.Namespace(command="app", requires_any=[["nope_a", "nope_b"]])
        with pytest.raises(SystemExit, match=r"requires at least one of: nope_a, nope_b"):
            cli_main._enforce_requires(args)

    def test_requires_when_only_applies_to_enabled_flags(self) -> None:
        args = argparse.Namespace(command="app", api=False, requires_when={"api": ["interloper_missing"]})
        cli_main._enforce_requires(args)

    def test_requires_when_enforced_for_an_enabled_flag(self) -> None:
        args = argparse.Namespace(command="app", api=True, requires_when={"api": ["interloper_missing"]})
        with pytest.raises(SystemExit, match="interloper_missing"):
            cli_main._enforce_requires(args)

    def test_missing_packages_are_deduplicated_and_sorted(self) -> None:
        args = argparse.Namespace(
            command="app",
            requires=["zzz_missing", "aaa_missing"],
            api=True,
            requires_when={"api": ["zzz_missing"]},
        )
        with pytest.raises(SystemExit, match=r"requires package\(s\): aaa_missing, zzz_missing$"):
            cli_main._enforce_requires(args)

    def test_no_declarations_is_a_no_op(self) -> None:
        cli_main._enforce_requires(argparse.Namespace(command="run"))


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
        monkeypatch.setattr(sys, "argv", ["interloper", "launch", "0" * 8 + "-0000-0000-0000-" + "0" * 12])
        monkeypatch.setattr(
            "interloper.cli.commands.launch._cmd_launch",
            lambda args: seen.append(args),
        )

        cli_main.main()

        assert len(seen) == 1
        assert str(seen[0].run_id) == "00000000-0000-0000-0000-000000000000"

    def test_activates_settings_for_the_duration_of_the_command(self, monkeypatch: pytest.MonkeyPatch) -> None:
        active: list[AppSettings | None] = []
        monkeypatch.setattr(sys, "argv", ["interloper", "launch", "11111111-1111-1111-1111-111111111111"])
        monkeypatch.setattr(
            "interloper.cli.commands.launch._cmd_launch",
            lambda args: active.append(AppSettings._active),
        )

        cli_main.main()

        assert active[0] is not None
        # The activation is scoped to the command: nothing leaks to the process.
        assert AppSettings._active is None

    def test_app_command_folds_cli_overrides_into_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        ports: list[int] = []
        monkeypatch.setattr(sys, "argv", ["interloper", "app", "--port", "4321", "--no-api"])
        monkeypatch.setattr(
            "interloper.cli.commands.app._cmd_app",
            lambda args: ports.append(AppSettings.get().server.port),
        )

        cli_main.main()

        assert ports == [4321]

    def test_group_command_without_a_subcommand_prints_its_help(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setattr(sys, "argv", ["interloper", "db"])

        cli_main.main()

        out = capsys.readouterr().out
        assert "usage: interloper db" in out
        assert "upgrade" in out

    def test_unsatisfied_requirement_aborts_before_running(self, monkeypatch: pytest.MonkeyPatch) -> None:
        called: list[bool] = []
        monkeypatch.setattr(sys, "argv", ["interloper", "launch", "22222222-2222-2222-2222-222222222222"])
        monkeypatch.setattr(
            "interloper.cli.commands.launch._cmd_launch",
            lambda args: called.append(True),
        )
        monkeypatch.setattr(cli_main, "_has_package", lambda name: False)

        with pytest.raises(SystemExit, match="requires"):
            cli_main.main()

        assert called == []

    def test_telemetry_is_shut_down_even_when_the_handler_raises(
        self, monkeypatch: pytest.MonkeyPatch, isolated_process_state: dict[str, list[bool]]
    ) -> None:
        monkeypatch.setattr(sys, "argv", ["interloper", "launch", "33333333-3333-3333-3333-333333333333"])
        monkeypatch.setattr(
            "interloper.cli.commands.launch._cmd_launch",
            lambda args: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        with pytest.raises(RuntimeError, match="boom"):
            cli_main.main()

        assert isolated_process_state["init"] == [True]
        assert isolated_process_state["shutdown"] == [True]
        assert AppSettings._active is None
