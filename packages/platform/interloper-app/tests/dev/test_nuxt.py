"""Tests for ``interloper_app.dev.nuxt``."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from interloper_app.dev.nuxt import NuxtServer
from interloper_app.dev.port import Port


class _Group:
    """A process group that records what it runs: each command keeps running, or exits with ``code``."""

    def __init__(self, code: int | None = None, missing: bool = False) -> None:
        self.started: list[tuple[list[str], dict[str, str] | None]] = []
        self.outputs: list[str | None] = []
        self.stopped = 0
        self._code = code
        self._missing = missing

    @property
    def running(self) -> bool:
        return self._code is None

    def start(
        self,
        command: list[str],
        *,
        cwd: Path | None = None,
        stdin: bytes | None = None,
        environment: dict[str, str] | None = None,
        output: str | None = None,
    ) -> None:
        if self._missing:
            raise FileNotFoundError(command[0])
        self.started.append((command, environment))
        self.outputs.append(output)

    def wait(self) -> int:
        return self._code or 0

    def stop(self, timeout: float = 10.0) -> None:
        self.stopped += 1


def _server(tmp_path: Path, group: _Group, monkeypatch: pytest.MonkeyPatch, *, answers: bool = True) -> NuxtServer:
    server = NuxtServer(
        tmp_path, port=Port(3100), api_port=Port(45678), ui_entries={"example": "http://127.0.0.1:5173/src/index.ts"}
    )
    monkeypatch.setattr(server, "_group", group)
    monkeypatch.setattr(Port, "answers", property(lambda self: answers))
    monkeypatch.setattr(Port, "in_use", property(lambda self: False))
    return server


def test_it_serves_on_the_loopback_in_front_of_the_api(tmp_path: Path):
    server = NuxtServer(
        tmp_path, port=Port(3100), api_port=Port(45678), ui_entries={"example": "http://127.0.0.1:5173/src/index.ts"}
    )

    assert server.command() == ["pnpm", "--loglevel", "warn", "dev", "--host", "127.0.0.1", "--port", "3100"]
    environment = server.environment()
    assert (environment["INTERLOPER_SERVER_PORT"], environment["INTERLOPER_API_PORT"]) == ("3100", "45678")
    assert environment["INTERLOPER_UI_DEV"] == "example=http://127.0.0.1:5173/src/index.ts"


def test_the_app_s_dependencies_install_when_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    group = _Group()

    _server(tmp_path, group, monkeypatch).start()

    assert [command for command, _ in group.started] == [
        ["pnpm", "--loglevel", "warn", "install"],
        ["pnpm", "--loglevel", "warn", "dev", "--host", "127.0.0.1", "--port", "3100"],
    ]
    assert group.started[1][1] is not None


def test_its_output_and_its_install_s_are_logged_under_nuxt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    group = _Group()

    _server(tmp_path, group, monkeypatch).start()

    assert group.outputs == ["nuxt", "nuxt"]


def test_an_install_says_so_since_it_runs_quiet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.INFO, logger="interloper_app")

    _server(tmp_path, _Group(), monkeypatch).start()
    (tmp_path / "node_modules").mkdir()
    _server(tmp_path, _Group(), monkeypatch).start()

    assert caplog.messages == ["Installing the app's dependencies (pnpm install)"]


def test_a_failed_install_stops_dev(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(SystemExit, match="`pnpm install` failed"):
        _server(tmp_path, _Group(code=1), monkeypatch).start()


def test_a_server_that_exits_before_answering_stops_dev(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    (tmp_path / "node_modules").mkdir()
    group = _Group(code=1)

    with pytest.raises(SystemExit, match="Nuxt dev server did not start"):
        _server(tmp_path, group, monkeypatch, answers=False).start()
    assert group.stopped == 1


def test_without_pnpm_it_says_so(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(SystemExit, match="needs pnpm"):
        _server(tmp_path, _Group(missing=True), monkeypatch).start()


def test_dev_waits_until_the_first_compile_answers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    (tmp_path / "node_modules").mkdir()
    server = _server(tmp_path, _Group(), monkeypatch)
    answers = iter([False, True])
    monkeypatch.setattr(Port, "answers", property(lambda self: next(answers)))

    server.start()


def test_a_port_taken_meanwhile_is_refused_rather_than_moved_from(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    (tmp_path / "node_modules").mkdir()
    server = _server(tmp_path, _Group(), monkeypatch)
    monkeypatch.setattr(Port, "in_use", property(lambda self: True))

    with pytest.raises(SystemExit, match="port 3100 was taken meanwhile"):
        server.start()


def test_it_reports_whether_it_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    assert _server(tmp_path, _Group(), monkeypatch).running is True
    assert _server(tmp_path, _Group(code=0), monkeypatch).running is False
