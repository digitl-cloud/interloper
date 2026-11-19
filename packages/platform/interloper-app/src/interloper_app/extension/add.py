"""``interloper extension add``: add one part to the extension, working as soon as it is added."""

from __future__ import annotations

import argparse

from interloper_app.extension.command import ProjectCommand
from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.part import PARTS
from interloper_app.extension.project import Project


class AddCommand(ProjectCommand):
    """Add a source, a connection, settings, a command, routes or a UI to the extension here.

    Writes the part's files from a working template, declares the part in extension.yaml, keeping everything else
    the file holds, and adds the dependency the part needs. The name is snake_case and defaults to the extension's
    key; the part's class is its PascalCase form, so a source named shop is the class Shop with the catalog key
    shop. A settings section other than the key starts with the key and an underscore. A UI is one page and one
    sidebar entry in ui/, built with @interloper/ui and npm; it adds routes first when the extension has none, and
    loads once interloper extension build has built it. It refuses, changing nothing, when a file it would write
    exists, when the manifest declares the name already, or when another installed package declares the settings
    section or command.
    """

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Declare the part and its optional name.

        Args:
            parser: The command's parser.
        """
        parser.add_argument("part", choices=list(PARTS), help="what to add")
        parser.add_argument("name", nargs="?", help="the part's name, snake_case (default: the extension's key)")

    def execute(self, project: Project, args: argparse.Namespace) -> None:
        """Write the part's files and its manifest entry, and those of the parts it needs, then add dependencies.

        Every refusal comes before the first write.

        Args:
            project: The project in the current directory.
            args: The parsed command line: ``part``, and ``name`` or ``None``
                for the extension's key.

        Raises:
            ExtensionError: If the project is not an extension yet, a name is
                refused, a file to write exists, the manifest cannot take an
                entry, or ``uv add`` fails after the files were written.
        """
        if not project.declares_extension:
            raise ExtensionError(
                f"{project.distribution} is not an extension yet: run uv run interloper extension init first"
            )
        manifest = project.read_manifest()
        key = manifest.document.get("key")
        if not isinstance(key, str):
            raise ExtensionError(f"{project.relative([project.manifest_path])} names no key")
        part = PARTS[args.part]
        needed = [(PARTS[name], key) for name in part.needs if PARTS[name].section not in manifest.document]
        additions = [*needed, (part, self.check_name(args.name or key, f"the {args.part} name"))]
        edited, files, entries, ignored = manifest, {}, [], []
        for addition, name in additions:
            values = addition.values(project.package, key, name)
            addition.check_name(values)
            addition.check_claims(project.root, values)
            entry = addition.entry(values)
            edited = edited.with_entry(entry, addition.shape)
            files.update(addition.render_files(project.root, values))
            entries.append(entry)
            ignored.extend(addition.ignored(values))
        project.check_free(files)
        for path, text in files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        edited.write()
        project.ignore(*ignored)
        written = project.relative([*files, project.manifest_path, *([project.root / ".gitignore"] if ignored else [])])
        try:
            for distribution in dict.fromkeys(addition.requires for addition, _ in additions):
                project.require(distribution)
        except ExtensionError as error:
            raise ExtensionError(f"wrote {written}, but {error}") from error
        declared = ", ".join(f"{entry.section} declares {entry.path}" for entry in entries)
        print(f"Wrote {written}: {declared}.")
        if part.section == "ui":
            print(
                "Build it with uv run interloper extension build; until then interloper extension check names "
                "the missing build."
            )
