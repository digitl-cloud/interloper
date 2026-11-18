"""The files ``init`` and ``add`` write, rendered from the templates shipped beside this module."""

from __future__ import annotations

from importlib.resources import files
from string import Template


def render(template: str, /, **values: str) -> str:
    """Render one template.

    Args:
        template: The template's file name under ``templates/``.
        **values: The values its ``$placeholders`` take, ``name`` among them.

    Returns:
        The rendered text.
    """
    source = files("interloper_app.extension").joinpath("templates", template).read_text(encoding="utf-8")
    return Template(source).substitute(values)
