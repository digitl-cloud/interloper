"""Macros the documentation site renders its reference tables with.

zensical's macros plugin loads this module (``zensical.toml``) and calls
``define_env``; the pages under ``docs/reference`` call the macros it
registers, so the settings and CLI references are read from the source
at build time rather than transcribed.
"""

from __future__ import annotations

import argparse
from typing import Any

from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import BaseSettings


def define_env(env: Any) -> None:
    """Register the macros with the plugin's environment.

    Args:
        env: The macros environment the plugin hands over.
    """
    env.macro(settings_reference)
    env.macro(cli_reference)


# -- Settings ------------------------------------------------------------------


def settings_reference() -> str:
    """Render every ``AppSettings`` section as a table of its fields.

    Returns:
        Markdown: one heading per section with its prefix, then a table of
        field, type, default and description.
    """
    from interloper.settings import AppSettings

    prefix = AppSettings.model_config.get("env_prefix", "")
    blocks: list[str] = []
    for name, field in AppSettings.model_fields.items():
        section = field.annotation
        if isinstance(section, type) and issubclass(section, BaseSettings):
            blocks.append(_section(name, section))
        else:
            variable = f"{prefix}{name.upper()}"
            blocks.append(f"### `{name}`\n\n`{variable}`, {_type(field)}. {field.description}\n")
    return "\n".join(blocks)


def _section(name: str, section: type[BaseSettings]) -> str:
    """Render one settings section.

    Args:
        name: The section's key in ``AppSettings`` and in ``interloper.yaml``.
        section: The section's settings class.

    Returns:
        Markdown: the heading, the summary line, the prefix and the table.
    """
    prefix = section.model_config.get("env_prefix", "")
    summary = (section.__doc__ or "").strip().splitlines()[0]
    rows = [
        f"| `{field_name}` | {_type(field)} | {_default(field)} | {field.description or ''} |"
        for field_name, field in section.model_fields.items()
    ]
    return "\n".join(
        [
            f"### `{name}`",
            "",
            f"{summary} Prefix `{prefix}`.",
            "",
            "| Field | Type | Default | Description |",
            "|-------|------|---------|-------------|",
            *rows,
            "",
        ]
    )


def _type(field: FieldInfo) -> str:
    """Render a field's annotation as a code span.

    Args:
        field: The field.

    Returns:
        The annotation as written in the source, in backticks.
    """
    annotation = field.annotation
    text = annotation.__name__ if isinstance(annotation, type) else str(annotation)
    return f"`{text.replace('typing.', '')}`"


def _default(field: FieldInfo) -> str:
    """Render a field's default as a code span.

    Args:
        field: The field.

    Returns:
        The default's ``repr`` in backticks, or an empty string when the
        field has none.
    """
    default = field.get_default(call_default_factory=True)
    return "" if default is PydanticUndefined else f"`{default!r}`"


# -- CLI -----------------------------------------------------------------------


def cli_reference() -> str:
    """Render every ``interloper`` command from the parser the CLI runs on.

    Returns:
        Markdown: one section per command (and per nested subcommand) with
        its description, requirements and argument table.
    """
    from interloper.cli.main import build_parser

    return "\n".join(
        _command(parser, f"interloper {name}", 2, help) for name, parser, help in _subcommands(build_parser())
    )


def _command(
    parser: argparse.ArgumentParser, name: str, level: int, help: str | None, inherited: list[str] | None = None
) -> str:
    """Render one command.

    Args:
        parser: The command's parser.
        name: The full command line to head the section with.
        level: The heading level.
        help: The one-line help the parent lists the command with, used when
            the command declares no description of its own.
        inherited: The packages the parent command already requires, which
            the subcommand does not repeat.

    Returns:
        Markdown for the command and, nested one level deeper, its subcommands.
    """
    lines = [f"{'#' * level} `{name}`", "", parser.description or help or "", ""]
    requires = [package for package in parser._defaults.get("requires", []) if package not in (inherited or [])]
    required_when = parser._defaults.get("requires_when", {})
    if requires or required_when:
        parts = [f"`{package}`" for package in requires]
        parts += [
            f"`{package}` with `--{flag.replace('_', '-')}`"
            for flag, packages in required_when.items()
            for package in packages
        ]
        lines += [f"Requires {', '.join(parts)}.", ""]
    rows = [_argument_row(action) for action in parser._actions if _is_documented(action)]
    if rows:
        lines += ["| Argument | Meaning |", "|----------|---------|", *rows, ""]
    for sub_name, sub_parser, sub_help in _subcommands(parser):
        lines.append(
            _command(sub_parser, f"{name} {sub_name}", level + 1, sub_help, parser._defaults.get("requires", []))
        )
    return "\n".join(lines)


def _subcommands(parser: argparse.ArgumentParser) -> list[tuple[str, argparse.ArgumentParser, str | None]]:
    """The parser's subcommands, in registration order.

    Args:
        parser: A parser that may declare subparsers.

    Returns:
        ``(name, parser, help)`` triples, empty when the parser has no subcommands.
    """
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            helps = {choice.dest: choice.help for choice in action._choices_actions}
            return [(name, sub, helps.get(name)) for name, sub in action.choices.items()]
    return []


def _is_documented(action: argparse.Action) -> bool:
    """Whether an action belongs in the argument table.

    Args:
        action: One of the parser's actions.

    Returns:
        False for the built-in help and for subcommand dispatch.
    """
    return not isinstance(action, argparse._HelpAction | argparse._SubParsersAction)


def _argument_row(action: argparse.Action) -> str:
    """Render one argument as a table row.

    Args:
        action: The argument's action.

    Returns:
        The row: the flags or positional name with its value placeholder,
        then the help text with the default or choices appended.
    """
    if action.option_strings:
        argument = ", ".join(action.option_strings)
        if action.nargs != 0 and not isinstance(action, argparse.BooleanOptionalAction):
            argument += f" {action.metavar or action.dest.upper()}"
    else:
        argument = action.dest if action.nargs is None else f"{action.dest} {_nargs(action.nargs)}".rstrip()
    meaning = action.help or ""
    if action.choices:
        meaning += f" One of {', '.join(f'`{choice}`' for choice in action.choices)}."
    if action.default not in (None, False, argparse.SUPPRESS) and "default" not in meaning.lower():
        meaning += f" Default `{action.default}`."
    return f"| `{argument}` | {meaning.strip()} |"


def _nargs(nargs: Any) -> str:
    """Render a positional's cardinality.

    Args:
        nargs: The action's ``nargs``.

    Returns:
        ``...`` for a list, ``[optional]`` for an optional, nothing otherwise.
    """
    return {"*": "...", "+": "...", "?": "[optional]"}.get(nargs, "")
