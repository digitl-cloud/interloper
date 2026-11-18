"""Tests for ``interloper_app.extension.part``."""

from __future__ import annotations

import pytest

from interloper_app.extension.part import PARTS
from interloper_app.extension.template import render

VALUES = {
    "package": "acme_interloper",
    "key": "acme",
    "name": "acme_shop",
    "Class": "AcmeShop",
    "NAME": "ACME_SHOP",
    "title": "Acme Shop",
}


def test_add_knows_five_parts():
    assert list(PARTS) == ["source", "connection", "settings", "command", "routes"]


@pytest.mark.parametrize("part", list(PARTS))
def test_every_part_renders_python_that_compiles(part):
    for path, template in PARTS[part].files.items():
        compile(render(template, **VALUES), path, "exec")


def test_a_folder_renders_python_that_compiles():
    compile(render("package.py.tmpl", **VALUES), "__init__.py", "exec")
