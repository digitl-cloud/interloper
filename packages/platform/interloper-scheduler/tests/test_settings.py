"""Tests for ``interloper_scheduler.settings``."""

import pytest
from interloper.settings import RunnerSettings, ServerSettings

from interloper_scheduler.settings import (
    CronSettings,
    LauncherSettings,
    ReaperSettings,
    RenewalSettings,
    SchedulerSettings,
    WorkerSettings,
)

SECTIONS = [CronSettings, LauncherSettings, ReaperSettings, RenewalSettings, WorkerSettings]


@pytest.fixture(autouse=True)
def _no_ambient_yaml(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Run each test where no ``interloper.yaml`` can be discovered."""
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize(
    ("section", "name"),
    [
        (CronSettings, "cron"),
        (LauncherSettings, "launcher"),
        (ReaperSettings, "reaper"),
        (RenewalSettings, "renewal"),
        (WorkerSettings, "worker"),
    ],
)
def test_the_scheduler_declares_its_own_sections(section, name):
    """Each section is read from the block and the variables of the name the scheduler declares."""
    assert section.section() == name
    assert section.declared()[name].extension == "scheduler"


@pytest.mark.parametrize("section", SECTIONS)
def test_every_public_name_reaches_a_field(section):
    """A public name is a field, or a key below a mapping field."""
    assert {name.partition(".")[0] for name in section.public} <= set(section.model_fields)


def test_the_scheduler_reads_each_section_as_the_process_runs_with_it(monkeypatch: pytest.MonkeyPatch):
    """The bundle holds the active sections, its own and the core ones its services read."""
    monkeypatch.setenv("INTERLOPER_REAPER_HEARTBEAT_TIMEOUT", "45")
    runner = RunnerSettings(type="serial")
    RunnerSettings.activate(runner)

    settings = SchedulerSettings.get()

    assert settings.runner is runner
    assert settings.reaper.heartbeat_timeout == 45
    assert settings.server is ServerSettings.get()
