"""Tests for ``interloper.cli.commands.run`` (manifest input mode)."""

import argparse
import json
from pathlib import Path
from typing import Any

import pytest

import interloper as il
from interloper.cli.commands.run import _cmd_run


class FakeRunSource(il.Source):
    """Single-asset source materialized by the CLI tests."""

    class One(il.Asset):
        """Returns a static row."""

        def data(self) -> Any:
            return [{"x": 1}]


SOURCE_PATH = f"{FakeRunSource.__module__}.FakeRunSource"


def _args(**overrides: Any) -> argparse.Namespace:
    base: dict[str, Any] = {
        "format": "paths",
        "file": None,
        "dry_run": False,
        "run_id": None,
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
            key: job
            init:
              targets:
                - path: {SOURCE_PATH}
            """,
        )
        _cmd_run(_args(file=str(spec), dry_run=True, date="2026-06-01"))

        out = capsys.readouterr().out
        assert "2026-06-01" in out
        assert "1 materializable / 1 total" in out
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
        assert "ASSET_COMPLETED" in err
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

        assert "ASSET_COMPLETED" not in capsys.readouterr().err

    def test_events_json_streams_to_stdout(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        spec = self._materializing_spec(tmp_path)
        _cmd_run(_args(file=str(spec), events="json"))

        lines = [line for line in capsys.readouterr().out.splitlines() if line]
        types = {json.loads(line)["type"] for line in lines}
        assert "run_started" in types
        assert "asset_completed" in types

    @staticmethod
    def _materializing_spec(tmp_path: Path) -> Path:
        return _write_spec(
            tmp_path,
            f"""
            key: job
            init:
              destinations:
                - path: interloper.destination.file.FileDestination
                  init:
                    base_path: {tmp_path}/data
              targets:
                - path: {SOURCE_PATH}
            """,
        )


class TestRunJobSpecMode:
    """``interloper run -f <job-spec>`` reconstructs the Job and runs it."""

    def test_job_spec_dry_run_prints_plan(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        import yaml

        job = il.Job(targets=[FakeRunSource()])
        spec_file = tmp_path / "job.yaml"
        spec_file.write_text(yaml.safe_dump(job.to_spec().model_dump(mode="json", exclude_none=True)))

        _cmd_run(_args(file=str(spec_file), dry_run=True))

        out = capsys.readouterr().out
        assert "1 materializable / 1 total" in out
        assert "1. fake_run_source.one" in out

    def test_job_spec_run_materializes(self, tmp_path: Path) -> None:
        import yaml

        dest = il.MemoryDestination()
        job = il.Job(targets=[FakeRunSource(destinations=[dest])])
        spec_file = tmp_path / "job.yaml"
        spec_file.write_text(yaml.safe_dump(job.to_spec().model_dump(mode="json", exclude_none=True)))

        _cmd_run(_args(file=str(spec_file)))
