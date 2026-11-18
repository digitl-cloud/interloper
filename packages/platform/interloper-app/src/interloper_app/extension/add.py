"""``interloper extension add``: add one part to the extension, working as soon as it is added."""

from __future__ import annotations

import argparse

from interloper_app.extension.command import ProjectCommand
from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.manifest import ManifestFile
from interloper_app.extension.part import PARTS
from interloper_app.extension.project import Project


class AddCommand(ProjectCommand):
    """Add a source, a connection, settings, a command or routes to the extension here.

    Writes the part's files in the extension's package from a working template, declares the part in
    extension.yaml, keeping everything else the file holds, and adds the dependency the part needs. The name is
    snake_case and defaults to the extension's key; the part's class is its PascalCase form, so a source named
    shop is the class Shop with the catalog key shop. A settings section other than the key starts with the key
    and an underscore. It refuses, changing nothing, when a file it would write exists, when the manifest declares
    the name already, or when another installed package declares the settings section or command.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the part and its optional name.

        Args:
            parser: The command's parser.
        """
        parser.add_argument("part", choices=list(PARTS), help="what to add")
        parser.add_argument("name", nargs="?", help="the part's name, snake_case (default: the extension's key)")

    def execute(self, project: Project, args: argparse.Namespace) -> None:
        """Write the part's files and its manifest entry, then add its dependency.

        Every refusal comes before the first write.

        Args:
            project: The project in the current directory.
            args: The parsed command line: ``part``, and ``name`` or ``None``
                for the extension's key.

        Raises:
            ExtensionError: If the project is not an extension yet, the name
                is refused, a file to write exists, the manifest cannot take
                the entry, or ``uv add`` fails after the files were written.
        """
        if not project.declares_extension:
            raise ExtensionError(
                f"{project.distribution} is not an extension yet: run uv run interloper extension init first"
            )
        manifest_name = project.relative([project.manifest_path])
        if not project.manifest_path.is_file():
            raise ExtensionError(f"{manifest_name} is missing: the extension has no manifest")
        manifest = ManifestFile.read(project.manifest_path)
        key = manifest.document.get("key")
        if not isinstance(key, str):
            raise ExtensionError(f"{manifest_name} names no key")
        part = PARTS[args.part]
        values = part.values(project.package, key, self.check_name(args.name or key, f"the {args.part} name"))
        part.check_name(values)
        entry = part.entry(values)
        edited = manifest.with_entry(entry, part.shape)
        files = part.render_files(project.package_directory, values)
        project.check_free(files)
        for path, text in files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        edited.write()
        written = project.relative([*files, project.manifest_path])
        try:
            project.require(part.requires)
        except ExtensionError as error:
            raise ExtensionError(f"wrote {written}, but {error}") from error
        print(f"Wrote {written}: {entry.section} declares {entry.path}.")
