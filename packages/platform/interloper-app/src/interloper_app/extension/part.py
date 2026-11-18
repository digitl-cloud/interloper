"""The parts ``add`` knows: what each one writes in the package and declares in the manifest."""

from __future__ import annotations

import keyword
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from string import Template

from interloper.extension import MANIFESTS, ExtensionEntry
from interloper.utils.text import to_snake_case

from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.manifest import Shape
from interloper_app.extension.template import render


@dataclass(frozen=True)
class Part:
    """One part an extension can declare.

    The file paths, the folders and the import string are templates over the
    part's values: ``$package``, ``$key``, ``$name`` (snake_case), ``$Class``
    (its PascalCase form), ``$NAME`` (upper case) and ``$title`` (its words,
    capitalized).

    Attributes:
        section: The manifest section the part is declared under.
        shape: How the section holds entries.
        requires: The distribution the part's code imports from.
        files: Each file written, its path relative to the import package,
            mapped to the template it is rendered from.
        path: The import string the manifest declares, ``module:attribute``.
        folders: The folders under the import package the files sit in, each
            given an ``__init__.py`` when it has none.
    """

    section: str
    shape: Shape
    requires: str
    files: Mapping[str, str]
    path: str
    folders: tuple[str, ...] = field(default=())

    def values(self, package: str, key: str, name: str) -> dict[str, str]:
        """The values the part's templates take for one name.

        Args:
            package: The extension's import package.
            key: The extension's key.
            name: The part's name, snake_case.

        Returns:
            The values, by placeholder.
        """
        return {
            "package": package,
            "key": key,
            "name": name,
            "Class": "".join(word.capitalize() for word in name.split("_")),
            "NAME": name.upper(),
            "title": name.replace("_", " ").title(),
        }

    def check_name(self, values: Mapping[str, str]) -> None:
        """Refuse a name this part cannot take.

        Args:
            values: The part's values for the name.

        Raises:
            ExtensionError: If routes are named other than by the key, a
                settings section leaves the extension's namespace, another
                installed package declares the settings section or command,
                the class name is a Python keyword, or a component's catalog
                key, which follows its class name, would not follow the name.
        """
        name, key = values["name"], values["key"]
        if self.shape == "single" and name != key:
            raise ExtensionError(f"{self.section} take the extension's key, {key}, as their only name")
        if self.section == "settings" and name != key and not name.startswith(f"{key}_"):
            raise ExtensionError(
                f"a settings section other than '{key}' must start with {key}_, so that the extension's sections "
                "never collide with another package's"
            )
        if self.shape == "map":
            for entry in MANIFESTS.entries(self.section):
                if entry.name == name and entry.package != values["package"]:
                    raise ExtensionError(f"{entry.package} already declares '{name}' under {self.section}")
        class_name = self.entry(values).path.rpartition(":")[2]
        if keyword.iskeyword(class_name):
            raise ExtensionError(f"the name '{name}' gives the class {class_name}, a Python keyword")
        if self.section == "components":
            suffix = to_snake_case(class_name.removeprefix(values["Class"]))
            expected = f"{name}_{suffix}" if suffix else name
            if to_snake_case(class_name) != expected:
                raise ExtensionError(
                    f"the name '{name}' gives the class {class_name}, keyed {to_snake_case(class_name)} in the "
                    f"catalog rather than {expected}; pick a name its key can follow"
                )

    def entry(self, values: Mapping[str, str]) -> ExtensionEntry:
        """The entry the manifest declares for one name.

        Args:
            values: The part's values for the name.

        Returns:
            The entry, named as core names it: by its import string in a list,
            by its name in a map, by the key as a single import string.
        """
        path = Template(self.path).substitute(values)
        name = path if self.shape == "list" else values["name"]
        return ExtensionEntry(values["package"], values["key"], self.section, name, path)

    def render_files(self, directory: Path, values: Mapping[str, str]) -> dict[Path, str]:
        """The files the part writes for one name, rendered.

        Args:
            directory: The import package's directory.
            values: The part's values for the name.

        Returns:
            Each file's path mapped to its text: first the ``__init__.py`` of
            each folder that has none, then the part's own files.
        """
        files: dict[Path, str] = {}
        for template in self.folders:
            folder = Template(template).substitute(values)
            package = directory / folder / "__init__.py"
            if not package.exists():
                files[package] = render("package.py.tmpl", title=folder.replace("_", " ").title())
        for path, template in self.files.items():
            files[directory / Template(path).substitute(values)] = render(template, **values)
        return files


PARTS: Mapping[str, Part] = {
    "source": Part(
        "components",
        "list",
        "interloper-core",
        {"$name/source.py": "source.py.tmpl"},
        "$package.$name.source:$Class",
        ("$name",),
    ),
    "connection": Part(
        "components",
        "list",
        "interloper-core",
        {"$name/connection.py": "connection.py.tmpl"},
        "$package.$name.connection:${Class}Connection",
        ("$name",),
    ),
    "settings": Part(
        "settings",
        "map",
        "interloper-core",
        {"settings/$name.py": "settings.py.tmpl"},
        "$package.settings.$name:${Class}Settings",
        ("settings",),
    ),
    "command": Part(
        "cli",
        "map",
        "interloper-core",
        {"commands/$name.py": "command.py.tmpl"},
        "$package.commands.$name:${Class}Command",
        ("commands",),
    ),
    "routes": Part("routes", "single", "interloper-api", {"routes.py": "routes.py.tmpl"}, "$package.routes:router"),
}
