"""Tests for ``interloper_app.dev.base``."""

from __future__ import annotations

import argparse
import os
import signal
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy.exc import OperationalError

from interloper_app.dev import base as base_module
from interloper_app.dev.base import DevCommand
from interloper_app.dev.platform import PlatformSpec
from interloper_app.dev.port import Port
from interloper_app.dev.project import DevProject, HeldExtension


def _args(
    *targets: str, port: int | None = 3100, no_browser: bool = False, files: list[str] | None = None
) -> argparse.Namespace:
    return argparse.Namespace(targets=list(targets), port=port, no_browser=no_browser, files=files or [])


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DevProject:
    project = DevProject(tmp_path, "Weather", tmp_path / "interloper.yaml", held=())
    monkeypatch.setattr(DevProject, "discover", classmethod(lambda cls, directory, files=(): project))
    monkeypatch.setattr(base_module.Port, "in_use", property(lambda self: False))
    monkeypatch.setattr(DevCommand, "_check_database", staticmethod(lambda: None))
    for signal_number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        monkeypatch.setattr(base_module.signal, "signal", mock.Mock())
    return project


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> tuple[list[PlatformSpec], mock.Mock, list[str]]:
    """Stand in for the platform, the file watch and the browser.

    Returns:
        The specs platforms were built with, the platform, and the links opened.
    """
    platform = mock.Mock()
    platform.wait_ready.return_value = True
    built: list[PlatformSpec] = []

    def make_platform(spec: PlatformSpec) -> mock.Mock:
        built.append(spec)
        platform.spec = spec
        return platform

    monkeypatch.setattr(base_module, "Platform", make_platform)
    monkeypatch.setattr(base_module, "watch", lambda *args, **kwargs: iter([]))
    opened: list[str] = []
    monkeypatch.setattr(base_module.webbrowser, "open", opened.append)
    return built, platform, opened


def test_windows_is_refused(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(base_module.sys, "platform", "win32")

    with pytest.raises(SystemExit, match="inside WSL"):
        DevCommand().run(_args())


def test_a_busy_port_is_refused_by_name(project: DevProject, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(base_module.Port, "in_use", property(lambda self: True))

    with pytest.raises(SystemExit, match="port 3100 is already in use"):
        DevCommand().run(_args())


def test_an_unreachable_postgres_is_named(monkeypatch: pytest.MonkeyPatch):
    def refuse(*args: object, **kwargs: object) -> None:
        raise OperationalError("connect", {}, Exception("connection refused"))

    monkeypatch.setattr(base_module, "bootstrap", refuse)

    with pytest.raises(SystemExit, match=r"Postgres at localhost:5432 \(database 'interloper'\) does not answer"):
        DevCommand._check_database()


def test_it_serves_the_project_then_stops_everything(project: DevProject, served, monkeypatch: pytest.MonkeyPatch):
    built, platform, opened = served
    monkeypatch.setattr(DevProject, "extensions", lambda self: [])

    DevCommand().run(_args("weather.OpenMeteo"))

    (spec,) = built
    assert (spec.port, spec.parent, spec.targets) == (3100, os.getpid(), ["weather.OpenMeteo"])
    assert (spec.root, spec.files) == (str(project.root), [])
    assert (spec.sign_in.organisation, spec.sign_in.extension) == ("Weather", None)
    assert opened == [spec.sign_in.link(3100)]
    platform.start.assert_called_once_with()
    platform.stop.assert_called_once_with()


def test_the_one_extension_with_a_ui_is_where_the_link_lands(
    project: DevProject, served, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    built, _, _ = served
    (tmp_path / "ui").mkdir()
    (tmp_path / "ui" / "package.json").write_text("{}")
    held = [HeldExtension("acme", "acme_interloper", tmp_path, declares_ui=True)]
    monkeypatch.setattr(DevProject, "extensions", lambda self: held)
    vite = mock.Mock(key="acme", entry="http://127.0.0.1:5173/src/index.ts")
    monkeypatch.setattr(base_module, "ViteServer", lambda key, directory, pnpm: vite)

    DevCommand().run(_args())

    (spec,) = built
    assert spec.sign_in.extension == "acme"
    assert spec.ui_entries == {"acme": "http://127.0.0.1:5173/src/index.ts"}
    vite.start.assert_called_once_with()
    vite.stop.assert_called_once_with()


def test_a_platform_that_fails_first_opens_the_browser_once_it_starts(
    project: DevProject, served, monkeypatch: pytest.MonkeyPatch
):
    _, platform, opened = served
    monkeypatch.setattr(DevProject, "extensions", lambda self: [])
    platform.wait_ready.side_effect = [False, True, True]
    edit = {(1, str(project.root / "weather.py"))}
    monkeypatch.setattr(base_module, "watch", lambda *args, **kwargs: iter([edit, edit]))

    DevCommand().run(_args())

    assert len(opened) == 1
    assert platform.restart.call_count == 2


def test_a_termination_signal_takes_the_ctrl_c_path():
    with pytest.raises(KeyboardInterrupt):
        DevCommand._interrupt(signal.SIGTERM, None)


def test_the_command_takes_targets_a_port_and_no_browser():
    parser = argparse.ArgumentParser()
    DevCommand().add_arguments(parser)

    args = parser.parse_args(["weather.OpenMeteo", "--port", "3100", "--no-browser"])

    assert (args.targets, args.port, args.no_browser) == (["weather.OpenMeteo"], 3100, True)


def test_ctrl_c_stops_everything(project: DevProject, served, monkeypatch: pytest.MonkeyPatch, capsys):
    _, platform, _ = served
    monkeypatch.setattr(DevProject, "extensions", lambda self: [])
    platform.start.side_effect = KeyboardInterrupt

    DevCommand().run(_args())

    assert "Stopping." in capsys.readouterr().out
    platform.stop.assert_called_once_with()


def test_a_missing_spec_file_is_refused_by_name(tmp_path: Path):
    with pytest.raises(SystemExit, match="no such spec file"):
        DevCommand().run(_args(files=[str(tmp_path / "missing.yaml")]))


def test_spec_files_reach_the_platform_absolute(project: DevProject, served, monkeypatch: pytest.MonkeyPatch):
    built, _, _ = served
    monkeypatch.setattr(DevProject, "extensions", lambda self: [])
    spec_file = project.root / "collection.yaml"
    spec_file.write_text("key: demo_source\n")
    monkeypatch.chdir(project.root)

    DevCommand().run(_args(files=["collection.yaml"]))

    assert built[0].files == [str(spec_file.resolve())]


def test_the_repository_serves_the_app_from_nuxt_in_front_of_the_api(
    project: DevProject, served, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    built, _, opened = served
    monkeypatch.setattr(DevProject, "extensions", lambda self: [])
    monkeypatch.setattr(DevProject, "app_source", property(lambda self: tmp_path / "app"))
    monkeypatch.setattr(base_module.Port, "free", classmethod(lambda cls: cls(45678)))
    nuxt = mock.Mock(running=True)
    made: list[dict[str, object]] = []

    def make_nuxt(directory: Path, **kwargs: object) -> mock.Mock:
        made.append({"directory": directory, **kwargs})
        return nuxt

    monkeypatch.setattr(base_module, "NuxtServer", make_nuxt)

    DevCommand().run(_args())

    (spec,) = built
    assert (spec.port, spec.spa, spec.public_url) == (45678, False, "http://127.0.0.1:3100")
    assert made == [{"directory": tmp_path / "app", "port": Port(3100), "api_port": Port(45678), "ui_entries": {}}]
    assert opened == [spec.sign_in.link(3100)]
    nuxt.start.assert_called_once_with()
    nuxt.stop.assert_called_once_with()


def test_a_nuxt_server_that_stops_stops_dev(
    project: DevProject, served, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    _, platform, _ = served
    monkeypatch.setattr(DevProject, "extensions", lambda self: [])
    monkeypatch.setattr(DevProject, "app_source", property(lambda self: tmp_path / "app"))
    monkeypatch.setattr(base_module.Port, "free", classmethod(lambda cls: cls(45678)))
    nuxt = mock.Mock(running=False)
    monkeypatch.setattr(base_module, "NuxtServer", lambda directory, **kwargs: nuxt)
    monkeypatch.setattr(base_module, "watch", lambda *args, **kwargs: iter([set()]))

    with pytest.raises(SystemExit, match="Nuxt dev server stopped"):
        DevCommand().run(_args())
    platform.stop.assert_called_once_with()
    nuxt.stop.assert_called_once_with()
