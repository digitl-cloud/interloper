"""Tests for ``interloper.cli.commands.run`` (manifest input mode)."""

import argparse
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
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _write_manifest(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "run.yaml"
    path.write_text(body)
    return path


class TestRunManifestMode:
    """``interloper run -f <manifest>`` behavior."""

    def test_dry_run_prints_plan(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        manifest = _write_manifest(
            tmp_path,
            f"""
            name: cli-test
            runner:
              type: serial
            assets:
              - source: {SOURCE_PATH}
            partition:
              date: 2026-06-01
            """,
        )
        _cmd_run(_args(file=str(manifest), dry_run=True))

        out = capsys.readouterr().out
        assert "cli-test" in out
        assert "SerialRunner" in out
        assert "2026-06-01" in out
        assert "1 materializable / 1 total" in out
        assert "1. fake_run_source.one" in out

    def test_cli_date_overrides_manifest_partition(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        manifest = _write_manifest(
            tmp_path,
            f"""
            runner:
              type: serial
            assets:
              - source: {SOURCE_PATH}
            partition:
              date: 2026-06-01
            """,
        )
        _cmd_run(_args(file=str(manifest), dry_run=True, date="2026-02-02"))

        assert "2026-02-02" in capsys.readouterr().out

    def test_file_with_targets_rejected(self, tmp_path: Path) -> None:
        manifest = _write_manifest(tmp_path, f"assets: [{{source: {SOURCE_PATH}}}]")
        with pytest.raises(SystemExit, match="cannot be combined"):
            _cmd_run(_args(file=str(manifest), target=["some.path"]))

    def test_no_input_rejected(self) -> None:
        with pytest.raises(SystemExit, match="provide one or more import paths"):
            _cmd_run(_args())

    def test_invalid_manifest_rejected(self, tmp_path: Path) -> None:
        manifest = _write_manifest(tmp_path, "assets: []")
        with pytest.raises(SystemExit, match="Invalid manifest"):
            _cmd_run(_args(file=str(manifest)))

    def test_manifest_run_materializes(self, tmp_path: Path) -> None:
        manifest = _write_manifest(
            tmp_path,
            f"""
            runner:
              type: serial
            destination:
              type: interloper.destination.file.FileDestination
              config:
                base_path: {tmp_path}/data
            assets:
              - source: {SOURCE_PATH}
            """,
        )
        _cmd_run(_args(file=str(manifest)))

        assert (tmp_path / "data" / "fake_run_source" / "one" / "data.pkl").exists()
