"""Tests for ``interloper_app.extension.part``."""

from __future__ import annotations

import json
from importlib.metadata import version
from pathlib import Path

import pytest

from interloper_app.extension import part as part_module
from interloper_app.extension.part import PARTS
from interloper_app.extension.template import render

VALUES = {
    "package": "acme_interloper",
    "key": "acme",
    "name": "acme_shop",
    "Class": "AcmeShop",
    "NAME": "ACME_SHOP",
    "title": "Acme Shop",
    "sdk": "0.137.0",
}
SDK = Path(__file__).parents[2] / "sdk" / "package.json"


def test_add_knows_six_parts():
    assert list(PARTS) == ["source", "connection", "settings", "command", "routes", "ui"]


@pytest.mark.parametrize("part", list(PARTS))
def test_every_part_renders_python_that_compiles(part):
    for path, template in PARTS[part].files.items():
        if path.endswith(".py"):
            compile(render(template, **VALUES), path, "exec")


def test_a_folder_renders_python_that_compiles():
    compile(render("package.py.tmpl", **VALUES), "__init__.py", "exec")


def test_the_ui_names_the_sdks_versions():
    sdk = json.loads(SDK.read_text())
    values = PARTS["ui"].values("acme_interloper", "acme", "acme")

    package = json.loads(render("ui/package.json.tmpl", **values))

    assert package["devDependencies"] == {
        "@interloper/ui": values["sdk"],
        "typescript": sdk["peerDependencies"]["typescript"],
        "vite": sdk["peerDependencies"]["vite"],
        "vue-tsc": sdk["peerDependencies"]["vue-tsc"],
        "vue": sdk["dependencies"]["vue"],
    }


@pytest.mark.parametrize(
    ("installed", "sdk"),
    [(None, version("interloper-app")), ("0.138.0rc2", "0.138.0-rc.2"), ("0.137.0+local", "0.137.0")],
)
def test_the_sdk_is_the_installed_app_version(monkeypatch, installed, sdk):
    if installed is not None:
        monkeypatch.setattr(part_module, "version", lambda distribution: installed)

    assert PARTS["ui"].values("acme_interloper", "acme", "acme")["sdk"] == sdk
