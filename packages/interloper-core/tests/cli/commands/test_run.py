"""Tests for ``interloper.cli.commands.run`` (manifest input mode)."""

import argparse
import json
from pathlib import Path
from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.cli.commands.run import _cmd_run, register
from interloper.operation import OperationResult


class FakeRunSource(il.Source):
    """Single-asset source materialized by the CLI tests."""

    class One(il.Asset):
        """Returns a static row."""

        def data(self) -> Any:
            return [{"x": 1}]


class FakePartitionedSource(il.Source):
    """Single daily-partitioned asset that does not allow windowed runs."""

    class Daily(il.Asset):
        """Returns a static row stamped with the partition date."""

        partitioning: ClassVar[il.TimePartitionConfig | None] = il.TimePartitionConfig(column="date")

        def data(self, context: il.ExecutionContext) -> Any:
            return [{"date": context.partition_date, "x": 1}]


class FailingSource(il.Source):
    """Single-asset source whose asset always raises."""

    class Boom(il.Asset):
        """Always fails.

        Raises:
            RuntimeError: Always.
        """

        def data(self) -> Any:
            raise RuntimeError("asset blew up")


SOURCE_PATH = f"{FakeRunSource.__module__}.FakeRunSource"
PARTITIONED_SOURCE_PATH = f"{FakePartitionedSource.__module__}.FakePartitionedSource"


def _args(**overrides: Any) -> argparse.Namespace:
    base: dict[str, Any] = {
        "format": "paths",
        "file": None,
        "dry_run": False,
        "operation_id": None,
        "date": None,
        "start_date": None,
        "end_date": None,
        "target": [],
        "events": "pretty",
        "quiet": False,
        "verbose": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _write_spec(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "run.yaml"
    path.write_text(body)
    return path


class TestRunSpecFileMode:
    """``interloper run -f <spec.yaml>`` reconstructs a runnable component and runs its DAG."""

    def test_dry_run_prints_plan(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        spec = _write_spec(
            tmp_path,
            f"""
            path: interloper.job.base.Job
            init:
              targets:
                - path: {SOURCE_PATH}
            """,
        )
        _cmd_run(_args(file=str(spec), dry_run=True))

        out = capsys.readouterr().out
        assert "Window:    (none)" in out
        assert "1 enabled / 1 total" in out
        assert "1. fake_run_source.one" in out

    def test_source_spec_is_runnable_directly(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        spec = _write_spec(tmp_path, f"path: {SOURCE_PATH}\n")
        _cmd_run(_args(file=str(spec), dry_run=True))

        assert "1. fake_run_source.one" in capsys.readouterr().out

    def test_non_runnable_spec_rejected(self, tmp_path: Path) -> None:
        spec = _write_spec(tmp_path, "path: interloper.destination.memory.MemoryDestination\n")
        with pytest.raises(SystemExit, match="not runnable"):
            _cmd_run(_args(file=str(spec)))

    def test_file_with_targets_rejected(self, tmp_path: Path) -> None:
        spec = _write_spec(tmp_path, f"path: {SOURCE_PATH}\n")
        with pytest.raises(SystemExit, match="cannot be combined"):
            _cmd_run(_args(file=str(spec), target=["some.path"]))

    def test_no_input_rejected(self) -> None:
        with pytest.raises(SystemExit, match="provide one or more import paths"):
            _cmd_run(_args())

    def test_invalid_spec_rejected(self, tmp_path: Path) -> None:
        spec = _write_spec(tmp_path, "nonsense: true")
        with pytest.raises(SystemExit, match="Invalid spec file"):
            _cmd_run(_args(file=str(spec)))

    def test_spec_run_materializes(self, tmp_path: Path) -> None:
        spec = self._materializing_spec(tmp_path)
        _cmd_run(_args(file=str(spec)))

        assert (tmp_path / "data" / "fake_run_source" / "one" / "data.pkl").exists()

    def test_run_prints_lifecycle_events(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        spec = self._materializing_spec(tmp_path)
        _cmd_run(_args(file=str(spec)))

        err = capsys.readouterr().err
        assert "RUN_STARTED" in err
        assert "OPERATION_COMPLETED" in err
        assert "RUN_COMPLETED" in err
        # Default filter drops destination I/O chatter.
        assert "DEST_WRITE_STARTED" not in err

    def test_verbose_includes_io_events(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        spec = self._materializing_spec(tmp_path)
        _cmd_run(_args(file=str(spec), verbose=True))

        assert "DEST_WRITE_STARTED" in capsys.readouterr().err

    def test_quiet_suppresses_events(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        spec = self._materializing_spec(tmp_path)
        _cmd_run(_args(file=str(spec), quiet=True))

        assert "OPERATION_COMPLETED" not in capsys.readouterr().err

    def test_events_json_streams_to_stdout(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        spec = self._materializing_spec(tmp_path)
        _cmd_run(_args(file=str(spec), events="json"))

        lines = [line for line in capsys.readouterr().out.splitlines() if line]
        types = {json.loads(line)["type"] for line in lines}
        assert "run_started" in types
        assert "operation_completed" in types

    @staticmethod
    def _materializing_spec(tmp_path: Path) -> Path:
        return _write_spec(
            tmp_path,
            f"""
            path: interloper.job.base.Job
            init:
              destinations:
                - path: interloper.destination.file.FileDestination
                  init:
                    base_path: {tmp_path}/data
              targets:
                - path: {SOURCE_PATH}
            """,
        )


class TestRunPreflightErrors:
    """Partition errors raised by the runner's preflight surface like the other input errors."""

    def test_window_without_allow_window_is_a_clean_error(self) -> None:
        with pytest.raises(SystemExit, match="Error: The run's window is not valid") as exc_info:
            _cmd_run(_args(target=[PARTITIONED_SOURCE_PATH], start_date="2026-01-01", end_date="2026-01-07"))
        assert "does not allow windows" in str(exc_info.value)

    def test_missing_partition_is_a_clean_error(self) -> None:
        with pytest.raises(SystemExit, match="given no window"):
            _cmd_run(_args(target=[PARTITIONED_SOURCE_PATH]))


class TestRunJobSpecMode:
    """``interloper run -f <job-spec>`` reconstructs the Job and runs it."""

    def test_job_spec_dry_run_prints_plan(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        import yaml

        job = il.Job(targets=[FakeRunSource()])
        spec_file = tmp_path / "job.yaml"
        spec_file.write_text(yaml.safe_dump(job.to_spec().model_dump(mode="json", exclude_none=True)))

        _cmd_run(_args(file=str(spec_file), dry_run=True))

        out = capsys.readouterr().out
        assert "1 enabled / 1 total" in out
        assert "1. fake_run_source.one" in out

    def test_job_spec_run_materializes(self, tmp_path: Path) -> None:
        import yaml

        dest = il.MemoryDestination()
        job = il.Job(targets=[FakeRunSource(destinations=[dest])])
        spec_file = tmp_path / "job.yaml"
        spec_file.write_text(yaml.safe_dump(job.to_spec().model_dump(mode="json", exclude_none=True)))

        _cmd_run(_args(file=str(spec_file)))


class TestResolveWindow:
    """The date flags resolve through the DAG's partitioning, so a key means what the asset means."""

    def test_a_day_key_is_a_window_of_one(self) -> None:
        import datetime as dt

        from interloper.cli.commands.run import _resolve_window
        from interloper.partitioning import TimeGranularity, TimePartitionWindow

        window = _resolve_window(_args(date="2026-06-01"), il.DAG(FakePartitionedSource()))
        assert isinstance(window, TimePartitionWindow)
        assert len(window) == 1
        assert window.granularity is TimeGranularity.DAY
        assert window.start == dt.date(2026, 6, 1)

    def test_a_key_of_another_granularity_is_refused(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        with pytest.raises(SystemExit, match="is not a day key"):
            _resolve_window(_args(date="2026-06"), il.DAG(FakePartitionedSource()))

    def test_start_and_end_keys_are_a_range(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        window = _resolve_window(_args(start_date="2026-06-01", end_date="2026-06-03"), il.DAG(FakePartitionedSource()))
        assert window is not None
        assert str(window) == "2026-06-01..2026-06-03"

    def test_a_key_needs_a_partitioned_operation(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        with pytest.raises(SystemExit, match="no enabled operation of the DAG is partitioned"):
            _resolve_window(_args(date="2026-06-01"), il.DAG(FakeRunSource()))

    def test_no_flags_is_no_window(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        assert _resolve_window(_args(), il.DAG(FakeRunSource())) is None

    def test_date_and_range_cannot_be_combined(self) -> None:
        from interloper.cli.commands.run import _resolve_window

        arguments = _args(date="2026-06-01", start_date="2026-06-01", end_date="2026-06-02")
        with pytest.raises(SystemExit, match="cannot be combined"):
            _resolve_window(arguments, il.DAG(FakePartitionedSource()))


class TestRegister:
    """Parser wiring for the ``run`` command."""

    def test_defaults(self) -> None:
        parser = argparse.ArgumentParser(prog="interloper")
        register(parser.add_subparsers(dest="command"))

        args = parser.parse_args(["run", "pkg.mod.Source"])

        assert args.format == "paths"
        assert args.target == ["pkg.mod.Source"]
        assert args.file is None
        assert args.events == "pretty"
        assert (args.dry_run, args.quiet, args.verbose) == (False, False, False)
        assert (args.date, args.start_date, args.end_date, args.operation_id) == (None, None, None, None)
        assert args.handler is _cmd_run

    def test_flags_are_parsed(self) -> None:
        parser = argparse.ArgumentParser(prog="interloper")
        register(parser.add_subparsers(dest="command"))

        args = parser.parse_args(
            [
                "run",
                "--format",
                "inline",
                "--events",
                "json",
                "-v",
                "--operation-id",
                "abc",
                "--date",
                "2026-01-01",
                "{}",
            ]
        )

        assert (args.format, args.events, args.verbose, args.operation_id, args.date) == (
            "inline",
            "json",
            True,
            "abc",
            "2026-01-01",
        )


class TestRunInlineMode:
    """``interloper run --format inline <json>`` — how DockerRunner hands a mini-DAG to a container."""

    def test_runs_the_serialized_dag(self) -> None:
        dag = il.DAG(FakeRunSource(destinations=[il.MemoryDestination()]))

        _cmd_run(_args(format="inline", target=[dag.to_spec().model_dump_json()]))

    def test_exactly_one_positional_is_required(self) -> None:
        with pytest.raises(SystemExit, match="exactly one positional argument"):
            _cmd_run(_args(format="inline", target=[]))

    def test_malformed_json_is_a_clean_error(self) -> None:
        with pytest.raises(SystemExit, match="failed to parse inline DAGSpec"):
            _cmd_run(_args(format="inline", target=["{not json"]))


class TestRunPathsMode:
    """``interloper run <import path>...`` resolves classes and instantiates them."""

    def test_an_unimportable_path_is_a_clean_error(self) -> None:
        with pytest.raises(SystemExit, match=r"failed to import 'nope\.NotAModule'"):
            _cmd_run(_args(target=["nope.NotAModule"]))

    def test_a_missing_attribute_is_a_clean_error(self) -> None:
        with pytest.raises(SystemExit, match="failed to import"):
            _cmd_run(_args(target=[f"{FakeRunSource.__module__}.NotDefinedHere"]))

    def test_a_non_class_target_is_rejected(self) -> None:
        with pytest.raises(SystemExit, match="is not a Source or Asset class"):
            _cmd_run(_args(target=[f"{FakeRunSource.__module__}.SOURCE_PATH"]))

    def test_an_unrelated_class_is_rejected(self) -> None:
        with pytest.raises(SystemExit, match="is not a Source or Asset class"):
            _cmd_run(_args(target=f"{argparse.__name__}.Namespace".split(",")))


class TestRunOutcome:
    """Exit status."""

    def test_a_failed_run_exits_nonzero(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            _cmd_run(_args(target=[f"{FailingSource.__module__}.FailingSource"]))

        assert excinfo.value.code == 1

    def test_the_inline_spec_can_be_read_from_a_file(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        path = tmp_path / "spec.json"
        path.write_text(il.DAG(FakeRunSource(destinations=[il.MemoryDestination()])).to_spec().model_dump_json())

        _cmd_run(_args(format="inline", target=[f"@{path}"], dry_run=True))

        assert "1. fake_run_source.one" in capsys.readouterr().out


class TestOperation:
    """``--operation-id``: one operation of the given DAG executed once for its host, its verdict left for it."""

    @pytest.fixture
    def message(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
        """Where the operation leaves what it came to.

        Args:
            monkeypatch: Fixture used to move the termination message path.
            tmp_path: Directory the message is written to.

        Returns:
            The message's path.
        """
        path = tmp_path / "termination-log"
        monkeypatch.setattr(OperationResult, "TERMINATION_MESSAGE_PATH", str(path))
        return path

    def test_it_executes_once_and_leaves_its_verdict(self, message: Path) -> None:
        dag = il.DAG(FailingSource(destinations=[il.MemoryDestination()]))
        (operation,) = dag.operations
        operation.retry = il.RetryPolicy(max_attempts=3, delay=0)

        with pytest.raises(SystemExit):
            _cmd_run(_args(format="inline", target=[dag.to_spec().model_dump_json()], operation_id=operation.id))

        assert OperationResult.from_termination_message(message.read_text(), "exited") == OperationResult(
            error="RuntimeError: asset blew up"
        )

    def test_one_that_cannot_start_leaves_a_final_verdict(self, message: Path) -> None:
        with pytest.raises(SystemExit):
            _cmd_run(_args(format="inline", target=["{not json"], operation_id="op-1"))

        left = OperationResult.from_termination_message(message.read_text(), "exited")
        assert (left.error or "").startswith("Error: failed to parse inline DAGSpec")
        assert left.retryable is False

    def test_an_operation_not_in_the_dag_is_a_clean_error(self, message: Path) -> None:
        with pytest.raises(SystemExit, match="not found"):
            _cmd_run(_args(target=[SOURCE_PATH], operation_id="op-unknown"))


class TestPrintPlan:
    """The ``--dry-run`` plan rendering."""

    def test_renders_runner_partition_and_generations(self, capsys: pytest.CaptureFixture[str]) -> None:
        from interloper.cli.commands.run import _print_plan

        _print_plan(il.DAG(FakeRunSource()), None, "AsyncRunner")

        out = capsys.readouterr().out
        assert "Runner:    AsyncRunner" in out
        assert "Window:    (none)" in out
        assert "1. fake_run_source.one" in out
        assert "Run:" not in out


def test_an_unexpected_spec_failure_is_reported_cleanly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A reconstruct crash the spec layer does not classify still exits, not tracebacks."""
    from interloper.dag import DAG

    spec = _write_spec(tmp_path, f"path: {SOURCE_PATH}\n")
    monkeypatch.setattr(
        DAG,
        "from_spec_file",
        classmethod(lambda cls, path: (_ for _ in ()).throw(RuntimeError("unclassified"))),
    )

    with pytest.raises(SystemExit, match="failed to reconstruct spec"):
        _cmd_run(_args(file=str(spec)))


def test_file_and_format_are_exclusive(capsys: pytest.CaptureFixture[str]) -> None:
    """``-f`` with ``--format`` is refused by the parser instead of ignoring ``--format``."""
    parser = argparse.ArgumentParser()
    register(parser.add_subparsers(dest="command"))

    with pytest.raises(SystemExit):
        parser.parse_args(["run", "-f", "spec.yaml", "--format", "inline"])

    assert "not allowed with argument" in capsys.readouterr().err
