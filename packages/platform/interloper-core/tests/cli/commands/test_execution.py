"""Tests for ``interloper.cli.commands.execution``."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest

from interloper.cli.commands.execution import configure_logging


@pytest.fixture
def configured(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Record what the console is configured with.

    Args:
        monkeypatch: Fixture used to swap the console setup.

    Returns:
        The level and keyword arguments of the last call.
    """
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(
        "interloper.cli.console.configure_console_logging",
        lambda level, **kwargs: recorded.update(level=level, **kwargs),
    )
    return recorded


def test_the_project_run_from_logs_its_own_modules_at_info(
    configured: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``run`` and ``launch`` give the modules of the directory they run from the command's level."""
    monkeypatch.chdir(tmp_path)

    configure_logging()

    assert (configured["level"], configured["project"]) == (logging.INFO, tmp_path)


@pytest.mark.parametrize(("quiet", "verbose", "level"), [(True, True, logging.WARNING), (False, True, logging.DEBUG)])
def test_quiet_wins_over_verbose(configured: dict[str, Any], quiet: bool, verbose: bool, level: int) -> None:
    """``--quiet`` holds everything to warnings even beside ``-v``."""
    configure_logging(quiet=quiet, verbose=verbose)

    assert configured["level"] == level
