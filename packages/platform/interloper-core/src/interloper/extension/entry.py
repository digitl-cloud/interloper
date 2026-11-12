"""The records a manifest yields: one entry per declared import string, one failure per thing skipped."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import EntryPoint
from typing import Any


@dataclass(frozen=True)
class ExtensionEntry:
    """One import string a package's manifest declares under a section.

    Attributes:
        package: The import package whose ``extension.yaml`` declares the entry.
        extension: The manifest's ``key``.
        section: The section the entry sits under.
        name: The entry's name: its key in a map, the extension's key for a
            single import string, the import string itself in a list.
        path: The import string, ``module`` or ``module:attribute``.
    """

    package: str
    extension: str
    section: str
    name: str
    path: str

    @classmethod
    def from_section(cls, package: str, extension: str, section: str, value: Any) -> tuple[ExtensionEntry, ...]:
        """Read the entries of one section, in declaration order.

        Args:
            package: The import package whose manifest declares the section.
            extension: The manifest's ``key``, the name of a single import string.
            section: The section's name.
            value: The section's value: an import string, a list of them, or a
                map of names to them.

        Returns:
            The section's entries.

        Raises:
            ValueError: If the value has none of the three shapes.
        """
        if isinstance(value, str):
            named = [(extension, value)]
        elif isinstance(value, list) and all(isinstance(path, str) for path in value):
            named = [(path, path) for path in value]
        elif isinstance(value, dict) and all(
            isinstance(name, str) and isinstance(path, str) for name, path in value.items()
        ):
            named = list(value.items())
        else:
            raise ValueError("a section holds an import string, a list of them, or a map of names to them")
        return tuple(cls(package, extension, section, name, path) for name, path in named)

    def load(self) -> Any:
        """Import the object the entry names.

        Returns:
            The module, or the attribute after the colon.
        """
        return EntryPoint(self.name, self.path, self.section).load()


@dataclass(frozen=True)
class ExtensionFailure:
    """Something a package declared that could not be loaded, and was skipped.

    Attributes:
        package: The import package whose manifest declared it.
        section: The section, or ``None`` when the whole manifest was skipped.
        entry: The entry's name, or ``None`` when a whole section or manifest
            was skipped.
        error: What went wrong.
    """

    package: str
    section: str | None
    entry: str | None
    error: str
