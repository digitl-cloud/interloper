"""The parts ``add`` knows: what each one writes in the project and declares in the manifest."""

from __future__ import annotations

import keyword
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from string import Template

from interloper.extension import MANIFESTS, ExtensionEntry
from interloper.utils.text import to_snake_case

from interloper_app.extension.errors import ExtensionError
from interloper_app.extension.manifest import Shape
from interloper_app.extension.template import render

UI_NAMES = frozenset({"static", "ui"})
_RELEASE_CANDIDATE = re.compile(r"(?P<release>\d+\.\d+\.\d+)rc(?P<number>\d+)")


@dataclass(frozen=True)
class Part:
    """One part an extension can declare.

    The file paths, the folders, the import string and the ``.gitignore``
    patterns are templates over the part's values: ``$package``, ``$key``,
    ``$name`` (snake_case), ``$Class`` (its PascalCase form), ``$NAME`` (upper
    case), ``$title`` (its words, capitalized) and ``$sdk`` (the
    ``@interloper/ui`` version matching the installed ``interloper-app``).

    Attributes:
        section: The manifest section the part is declared under.
        shape: How the section holds entries.
        requires: The distribution the part's code imports from.
        files: Each file written, its path relative to the project's root,
            mapped to the template it is rendered from.
        path: The import string the manifest declares, ``module:attribute``.
        folders: The folders, relative to the project's root, that the
            part's Python files sit in, each given an ``__init__.py`` when it
            has none.
        needs: The parts added first, named by the key, when the manifest
            does not declare their section yet.
        ignores: The patterns the part adds to ``.gitignore``.
        claims: The folders, relative to the project's root, that the
            part's build empties, so none of them may exist when it is added.
    """

    section: str
    shape: Shape
    requires: str
    files: Mapping[str, str]
    path: str
    folders: tuple[str, ...] = field(default=())
    needs: tuple[str, ...] = field(default=())
    ignores: tuple[str, ...] = field(default=())
    claims: tuple[str, ...] = field(default=())

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
            "sdk": self._sdk_version(),
        }

    def check_name(self, values: Mapping[str, str]) -> None:
        """Refuse a name this part cannot take.

        Args:
            values: The part's values for the name.

        Raises:
            ExtensionError: If a part declared by the key is named otherwise,
                a settings section leaves the extension's namespace, another
                installed package declares the settings section or command, a
                component's folder would be where the UI lives, the class name
                is a Python keyword, or a component's catalog key, which
                follows its class name, would not follow the name.
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
        if self.section == "components" and name in UI_NAMES:
            raise ExtensionError(
                f"the name '{name}' is where the extension's UI lives (static/ is emptied by every UI build); "
                "pick another"
            )
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

    def render_files(self, root: Path, values: Mapping[str, str]) -> dict[Path, str]:
        """The files the part writes for one name, rendered.

        Args:
            root: The project's root.
            values: The part's values for the name.

        Returns:
            Each file's path mapped to its text: first the ``__init__.py`` of
            each folder that has none, then the part's own files.
        """
        files: dict[Path, str] = {}
        for template in self.folders:
            folder = root / Template(template).substitute(values)
            package = folder / "__init__.py"
            if not package.exists():
                files[package] = render("package.py.tmpl", title=folder.name.replace("_", " ").title())
        for path, template in self.files.items():
            files[root / Template(path).substitute(values)] = render(template, **values)
        return files

    def check_claims(self, root: Path, values: Mapping[str, str]) -> None:
        """Refuse a folder the part's build would empty when it exists already.

        Args:
            root: The project's root.
            values: The part's values for the name.

        Raises:
            ExtensionError: If a claimed folder exists: what it holds would be
                gitignored, then deleted by the first build.
        """
        for template in self.claims:
            folder = root / Template(template).substitute(values)
            if folder.exists():
                raise ExtensionError(
                    f"{folder.relative_to(root)} exists, and every build of the UI empties it; "
                    "move what it holds elsewhere first"
                )

    def ignored(self, values: Mapping[str, str]) -> list[str]:
        """The ``.gitignore`` patterns the part adds for one name.

        Args:
            values: The part's values for the name.

        Returns:
            The patterns.
        """
        return [Template(pattern).substitute(values) for pattern in self.ignores]

    @staticmethod
    def _sdk_version() -> str:
        """The ``@interloper/ui`` version released with the installed ``interloper-app``.

        Both are released at the platform's version; npm spells a release
        candidate ``0.138.0-rc.2`` where Python spells it ``0.138.0rc2``.

        Returns:
            The npm version, without the installed version's local part.
        """
        installed = version("interloper-app").split("+")[0]
        candidate = _RELEASE_CANDIDATE.fullmatch(installed)
        return f"{candidate['release']}-rc.{candidate['number']}" if candidate else installed


PARTS: Mapping[str, Part] = {
    "source": Part(
        "components",
        "list",
        "interloper-core",
        {"src/$package/$name/source.py": "source.py.tmpl"},
        "$package.$name.source:$Class",
        folders=("src/$package/$name",),
    ),
    "connection": Part(
        "components",
        "list",
        "interloper-core",
        {"src/$package/$name/connection.py": "connection.py.tmpl"},
        "$package.$name.connection:${Class}Connection",
        folders=("src/$package/$name",),
    ),
    "settings": Part(
        "settings",
        "map",
        "interloper-core",
        {"src/$package/settings/$name.py": "settings.py.tmpl"},
        "$package.settings.$name:${Class}Settings",
        folders=("src/$package/settings",),
    ),
    "command": Part(
        "cli",
        "map",
        "interloper-core",
        {"src/$package/commands/$name.py": "command.py.tmpl"},
        "$package.commands.$name:${Class}Command",
        folders=("src/$package/commands",),
    ),
    "routes": Part(
        "routes", "single", "interloper-api", {"src/$package/routes.py": "routes.py.tmpl"}, "$package.routes:router"
    ),
    "ui": Part(
        "ui",
        "single",
        "interloper-api",
        {
            "src/$package/ui.py": "ui.py.tmpl",
            "ui/package.json": "ui/package.json.tmpl",
            "ui/vite.config.ts": "ui/vite.config.ts.tmpl",
            "ui/tsconfig.json": "ui/tsconfig.json.tmpl",
            "ui/src/index.ts": "ui/src/index.ts.tmpl",
            "ui/src/main.css": "ui/src/main.css.tmpl",
            "ui/src/pages/Greeting.vue": "ui/src/pages/Greeting.vue.tmpl",
        },
        "$package.ui:UI",
        needs=("routes",),
        ignores=("src/$package/static/", "ui/node_modules/"),
        claims=("src/$package/static",),
    ),
}
