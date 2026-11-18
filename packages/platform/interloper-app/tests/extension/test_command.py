"""Tests for ``interloper_app.extension.command``."""

from __future__ import annotations

import sys

import pytest


def test_windows_is_refused(acme, uv, extension, tree, capsys, monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    before = tree(acme)

    with pytest.raises(SystemExit) as exit_info:
        extension("init")

    assert exit_info.value.code == 1
    assert capsys.readouterr().err == ("interloper extension init runs on macOS and Linux; on Windows, run it in WSL\n")
    assert tree(acme) == before


def test_a_subdirectory_of_a_project_names_its_root(acme, uv, extension, tree, capsys, monkeypatch):
    monkeypatch.chdir(acme / "src" / "acme_interloper")
    before = tree(acme)

    with pytest.raises(SystemExit) as exit_info:
        extension("init")

    assert exit_info.value.code == 1
    assert capsys.readouterr().err == (
        f"interloper extension init: no pyproject.toml here: run it from the project's root, {acme.resolve()}\n"
    )
    assert tree(acme) == before


def test_a_directory_that_is_no_project_is_one_line(tmp_path, extension, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exit_info:
        extension("init")

    assert exit_info.value.code == 1
    assert capsys.readouterr().err == (
        "interloper extension init: no pyproject.toml here: create the project with uv init --lib\n"
    )
    assert list(tmp_path.iterdir()) == []
