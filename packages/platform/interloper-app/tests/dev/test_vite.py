"""Tests for ``interloper_app.dev.vite``."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from interloper_app.dev.port import Port
from interloper_app.dev.process import ProcessGroup
from interloper_app.dev.vite import ViteServer


class _Group:
    """A process group that records what it runs: each command keeps running, or exits with ``code``."""

    def __init__(self, code: int | None = None, missing: bool = False) -> None:
        self.commands: list[list[str]] = []
        self.outputs: list[str | None] = []
        self.stopped = 0
        self._code = code
        self._missing = missing

    @property
    def running(self) -> bool:
        return self._code is None

    def start(
        self, command: list[str], *, cwd: Path | None = None, stdin: bytes | None = None, output: str | None = None
    ) -> None:
        if self._missing:
            raise FileNotFoundError(command[0])
        self.commands.append(command)
        self.outputs.append(output)

    def wait(self) -> int:
        return self._code or 0

    def stop(self, timeout: float = 10.0) -> None:
        self.stopped += 1


def _server(tmp_path: Path, group: _Group, monkeypatch: pytest.MonkeyPatch, *, answers: bool = True) -> ViteServer:
    server = ViteServer("acme", tmp_path, pnpm=False)
    monkeypatch.setattr(server, "_group", group)
    monkeypatch.setattr(Port, "answers", property(lambda self: answers))
    return server


def test_npm_runs_vite_on_the_loopback_without_clearing_the_terminal(tmp_path: Path):
    server = ViteServer("acme", tmp_path, pnpm=False)
    server.port = Port(5173)

    assert server.install_command() == ["npm", "install", "--no-audit", "--no-fund", "--loglevel", "warn"]
    assert server.command() == [
        *("npm", "exec", "--no", "--", "vite"),
        *("--host", "127.0.0.1", "--port", "5173", "--strictPort", "--clearScreen", "false", "--logLevel", "warn"),
    ]
    assert server.entry == "http://127.0.0.1:5173/src/index.ts"
    assert server.origin == "http://127.0.0.1:5173"


def test_a_pnpm_workspace_runs_it_with_pnpm(tmp_path: Path):
    server = ViteServer("acme", tmp_path, pnpm=True)

    assert server.install_command() == ["pnpm", "--loglevel", "warn", "install"]
    assert server.command()[:3] == ["pnpm", "exec", "vite"]


def test_dependencies_install_only_when_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    group = _Group()
    _server(tmp_path, group, monkeypatch).start()
    (tmp_path / "node_modules").mkdir()
    _server(tmp_path, group, monkeypatch).start()

    assert [command[:2] for command in group.commands] == [["npm", "install"], ["npm", "exec"], ["npm", "exec"]]


def test_its_output_and_its_install_s_are_logged_under_vite(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    group = _Group()

    _server(tmp_path, group, monkeypatch).start()

    assert group.outputs == ["vite", "vite"]


def test_an_install_says_so_since_it_runs_quiet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.INFO, logger="interloper_app")

    _server(tmp_path, _Group(), monkeypatch).start()
    (tmp_path / "node_modules").mkdir()
    _server(tmp_path, _Group(), monkeypatch).start()

    assert caplog.messages == ["Installing the acme UI's dependencies (npm install)"]


def test_the_install_runs_in_the_group_dev_stops(tmp_path: Path):
    assert isinstance(ViteServer("acme", tmp_path, pnpm=False)._group, ProcessGroup)


def test_a_server_that_exits_before_answering_stops_dev(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    (tmp_path / "node_modules").mkdir()
    group = _Group(code=1)

    with pytest.raises(SystemExit, match="The acme UI's Vite dev server did not start"):
        _server(tmp_path, group, monkeypatch, answers=False).start()
    assert group.stopped == 1


def test_a_failed_install_stops_dev(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(SystemExit, match="`npm install --no-audit --no-fund --loglevel warn` failed"):
        _server(tmp_path, _Group(code=1), monkeypatch).start()


def test_without_node_it_says_so(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    with pytest.raises(SystemExit, match=r"The acme UI needs Node\.js: npm was not found"):
        _server(tmp_path, _Group(missing=True), monkeypatch).start()


def test_dev_waits_until_the_server_answers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    (tmp_path / "node_modules").mkdir()
    server = ViteServer("acme", tmp_path, pnpm=False)
    monkeypatch.setattr(server, "_group", _Group())
    answers = iter([False, True])
    monkeypatch.setattr(Port, "answers", property(lambda self: next(answers)))

    server.start()
