"""What ``interloper dev`` was asked to serve: classes for the catalog, and the files that serve the collection.

The targets are those of ``interloper run``: a class path adds the class to
the catalog; an instance path (``weather.paris``) declares that instance, and
every component it binds, in its module's file; ``-f FILE`` declares each
document of a spec file. Declaring anything puts ``dev`` in file mode: the
collection is served from those files, read-only in the app. A file is named
relative to the project root, so the name stays the same across checkouts of
the project.
"""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID

import interloper as il
from interloper.catalog import Catalog
from interloper.errors import SpecError
from interloper.utils.imports import import_from_path
from interloper_db.store.components.declaration import Declarations


class Targets:
    """The class and instance paths and the spec files one ``dev`` run serves.

    Attributes:
        classes: The component classes named as targets, in their order.
    """

    def __init__(self, root: Path, paths: Sequence[str], files: Sequence[str]) -> None:
        """Import every target, so a mistake stops the platform before it serves anything.

        Args:
            root: The project root the files are named relative to.
            paths: The class and instance import paths.
            files: The spec files, absolute.

        Raises:
            SystemExit: When a path fails to import or names neither a
                component class nor a component instance.
        """
        self.classes: list[type[il.Component]] = []
        self._root = root
        self._files = [Path(file) for file in files]
        self._instances: dict[str, list[str]] = {}
        for path in paths:
            try:
                loaded = import_from_path(path)
            except (ImportError, AttributeError) as error:
                raise SystemExit(f"Error: cannot import '{path}': {error}") from error
            if isinstance(loaded, type) and issubclass(loaded, il.Component):
                self.classes.append(loaded)
            elif isinstance(loaded, il.Component):
                module, _, name = path.replace(":", ".").rpartition(".")
                self._instances.setdefault(module, []).append(name)
            else:
                raise SystemExit(f"Error: '{path}' is neither a component class nor a component instance")

    @property
    def declares(self) -> bool:
        """Whether files serve the collection: an instance target or a spec file was given.

        Returns:
            True in file mode.
        """
        return bool(self._instances or self._files)

    def declarations(self, org_id: UUID, catalog: Catalog) -> list[Declarations]:
        """Read what each instance module and spec file declares.

        Args:
            org_id: The dev organisation, which the row ids belong to.
            catalog: The catalog a spec's ``key`` resolves through.

        Returns:
            One `Declarations` per file, the modules first.

        Raises:
            SystemExit: When a spec file is unreadable or does not build, or a
                module's instances cannot be read.
        """
        declared: list[Declarations] = []
        seen: dict[int, UUID] = {}
        for module_name, names in self._instances.items():
            module = importlib.import_module(module_name)
            file = self._name(Path(module.__file__ or ""))
            named = {name: value for name, value in vars(module).items() if isinstance(value, il.Component)}
            roots = {name: named[name] for name in names}
            try:
                declared.append(Declarations.from_instances(org_id, file, roots, named, seen))
            except Exception as error:
                raise SystemExit(f"Error: {file} does not declare its components: {error}") from error
        for file in self._files:
            try:
                specs = il.Spec.all_from_file(file)
                declared.append(Declarations.from_documents(org_id, self._name(file), specs, catalog))
            except SpecError as error:
                raise SystemExit(f"Error: {error}") from error
            except Exception as error:
                raise SystemExit(f"Error: {self._name(file)} does not build: {error}") from error
        return declared

    def _name(self, file: Path) -> str:
        """Name a file the way its rows record it.

        Args:
            file: The file.

        Returns:
            Its path relative to the project root, or absolute when outside it.
        """
        resolved = file.resolve()
        return resolved.relative_to(self._root).as_posix() if resolved.is_relative_to(self._root) else str(resolved)
