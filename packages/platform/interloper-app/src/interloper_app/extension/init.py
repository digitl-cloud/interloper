"""``interloper extension init``: turn a library project into an extension that declares nothing yet."""

from __future__ import annotations

import argparse
import json
import secrets

from interloper.extension import MANIFESTS

from interloper_app.extension.command import ProjectCommand
from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.part import PARTS
from interloper_app.extension.project import Project
from interloper_app.extension.template import render

CONFIG_FILE = "interloper.yaml"


class InitCommand(ProjectCommand):
    """Turn the uv project here into an extension that declares nothing yet.

    Run it in a project made with uv init --lib. It writes the package's extension.yaml with the extension's key,
    name and description and no section, names the package under the interloper.extensions entry point in
    pyproject.toml, adds interloper-core, and writes interloper.yaml, the local instance's configuration, with an
    encryption key of its own, gitignored. It refuses when the project is an extension already or when a file it
    would write exists. Add what the extension declares with interloper extension add.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the key and the name, both optional.

        Args:
            parser: The command's parser.
        """
        parser.add_argument(
            "--key",
            help="the extension's key, snake_case (default: the package's name without an interloper prefix or suffix)",
        )
        parser.add_argument("--name", help="the extension's display name (default: the key, title-cased)")

    def execute(self, project: Project, args: argparse.Namespace) -> None:
        """Write the bare extension, then add interloper-core.

        Args:
            project: The project in the current directory.
            args: The parsed command line: ``key`` and ``name``, each
                ``None`` for its default.

        Raises:
            ExtensionError: If the project declares an extension already, the
                key is not snake_case or another installed package holds it
                (the loader would skip one of the two manifests), a file to
                write exists, or ``uv add`` fails after the files were written.
        """
        if project.declares_extension:
            raise ExtensionError(
                f"{project.distribution} already declares an extension: "
                "add parts to it with uv run interloper extension add"
            )
        key = self.check_name(args.key or project.default_key, "the key")
        for extension in MANIFESTS.extensions():
            if extension.key == key and extension.package != project.package:
                raise ExtensionError(f"the key '{key}' is taken by {extension.package}: pick another with --key")
        name = args.name or key.replace("_", " ").title()
        files = {
            project.manifest_path: render(
                "extension.yaml.tmpl",
                key=key,
                name=json.dumps(name, ensure_ascii=False),
                description=json.dumps(project.description, ensure_ascii=False),
            ),
            project.root / CONFIG_FILE: render("interloper.yaml.tmpl", key=secrets.token_urlsafe(32)),
        }
        existing = [path for path in files if path.exists()]
        if existing:
            raise ExtensionError(f"{project.relative(existing)} exists already; move it away to start the extension")
        project.add_entry_point(key)
        for path, text in files.items():
            path.write_text(text, encoding="utf-8")
        project.ignore(CONFIG_FILE)
        written = f"pyproject.toml, {project.relative(files)}, .gitignore"
        try:
            project.require("interloper-core")
        except ExtensionError as error:
            raise ExtensionError(f"wrote {written}, but {error}") from error
        print(f"Wrote {written}: {project.distribution} is the extension '{key}', declaring nothing yet.")
        print("Add what it declares, one part at a time:")
        print(f"  uv run interloper extension add <{'|'.join(PARTS)}> [name]")
