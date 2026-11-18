"""An extension's ``extension.yaml``, edited as text so that what its author wrote survives.

A manifest's shape is fixed: top-level keys, each section a single import
string or a block holding one entry per line. A new entry is therefore one
line inserted after the last entry of its section, written with the prefix
of the section's first entry, or a new section appended at the end; comments,
blank lines, order and line endings are left as they were. Every edit is
parsed again and compared with the document it must give, so an edit that
would change anything else is refused rather than written.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal

import yaml
from interloper.extension import ExtensionEntry

from interloper_app.extension.errors import ExtensionError

Shape = Literal["list", "map", "single"]

_TYPES: dict[str, type] = {"list": list, "map": dict, "single": str}
_DESCRIPTIONS: dict[type, str] = {str: "a single import string", list: "a list", dict: "a map"}
_LIST_ITEM = re.compile(r"\s*-\s+")


@dataclass(frozen=True)
class ManifestFile:
    """An ``extension.yaml``, held as its text.

    Attributes:
        path: Where the manifest is read from and written to.
        text: The manifest's text, its line endings as written.
    """

    path: Path
    text: str

    @classmethod
    def read(cls, path: Path) -> ManifestFile:
        """Read a manifest, keeping its line endings.

        Args:
            path: The manifest's path.

        Returns:
            The manifest.
        """
        return cls(path, path.read_bytes().decode("utf-8"))

    @property
    def document(self) -> dict[str, Any]:
        """The manifest, parsed.

        Returns:
            The top-level mapping; empty when the text holds nothing.

        Raises:
            ExtensionError: If the text is not YAML, or not a mapping.
        """
        try:
            document = yaml.safe_load(self.text)
        except yaml.YAMLError as error:
            raise ExtensionError(f"{self.path.name} is not valid YAML: {error}") from error
        if document is None:
            return {}
        if not isinstance(document, dict):
            raise ExtensionError(f"{self.path.name} is not a mapping")
        return document

    def with_entry(self, entry: ExtensionEntry, shape: Shape) -> ManifestFile:
        """Declare one more entry, in a copy of the manifest.

        Args:
            entry: The entry: its section, its name (a map's key) and its
                import string.
            shape: How the section holds entries: a list of import strings, a
                map of names to them, or a single one.

        Returns:
            A manifest whose text declares the entry and is otherwise
            unchanged; this one is left as it is.

        Raises:
            ExtensionError: If the manifest already declares the entry, holds
                the section in another shape or on one line, or would parse to
                anything but its document plus the entry.
        """
        document = self.document
        if entry.section in document:
            self._check_extendable(entry, shape, document[entry.section])
            text = self._inserted(entry, shape)
        else:
            text = self._appended(entry, shape)
        edited = replace(self, text=text)
        if edited.document != self._document_with(document, entry, shape):
            raise ExtensionError(
                f"adding {entry.path} would change more of {self.path.name} than its {entry.section} section; "
                "declare it there by hand"
            )
        return edited

    def write(self) -> None:
        """Write the manifest's text to its path, byte for byte."""
        self.path.write_bytes(self.text.encode("utf-8"))

    # -- Internals -------------------------------------------------------------

    @property
    def _newline(self) -> str:
        """The line ending the manifest is written with.

        Returns:
            The Windows ending when the text uses it, a bare line feed otherwise.
        """
        return "\r\n" if "\r\n" in self.text else "\n"

    def _lines(self) -> list[str]:
        """Split the text into lines, each keeping its line ending.

        Returns:
            The lines; the last one has no ending when the text does not end
            with one.
        """
        parts = self.text.split("\n")
        return [f"{part}\n" for part in parts[:-1]] + ([parts[-1]] if parts[-1] else [])

    def _check_extendable(self, entry: ExtensionEntry, shape: Shape, value: Any) -> None:
        """Refuse a section that already declares the entry or that a new line cannot extend.

        Args:
            entry: The entry to declare.
            shape: The shape the entry needs its section to have.
            value: The section's parsed value; ``None`` for a section holding
                no entry yet.

        Raises:
            ExtensionError: If the section is a single import string already,
                has another shape, or declares the name or the import string.
        """
        if shape == "single":
            raise ExtensionError(f"{self.path.name} already declares {entry.section}")
        if value is not None and not isinstance(value, _TYPES[shape]):
            actual = _DESCRIPTIONS.get(type(value), "neither a list nor a map")
            wanted = "a list" if shape == "list" else "a map of names to import strings"
            raise ExtensionError(
                f"{entry.section} is {actual} in {self.path.name}; make it {wanted}, one entry per line, to add to it"
            )
        if shape == "map" and entry.name in (value or {}):
            raise ExtensionError(f"{self.path.name} already declares '{entry.name}' under {entry.section}")
        if shape == "list" and entry.path in (value or []):
            raise ExtensionError(f"{self.path.name} already declares {entry.path} under {entry.section}")

    def _line(self, entry: ExtensionEntry, shape: Shape, prefix: str) -> str:
        """The line declaring an entry inside its section's block.

        Args:
            entry: The entry.
            shape: How its section holds entries, a list or a map.
            prefix: The indentation, and ``- `` for a list.

        Returns:
            The line, ended with the manifest's line ending.
        """
        declaration = entry.path if shape == "list" else f"{entry.name}: {entry.path}"
        return f"{prefix}{declaration}{self._newline}"

    def _appended(self, entry: ExtensionEntry, shape: Shape) -> str:
        """Append the entry's section, holding the entry, after everything else.

        Args:
            entry: The entry.
            shape: How its section holds it.

        Returns:
            The new text.
        """
        newline = self._newline
        text = self.text if not self.text or self.text.endswith("\n") else f"{self.text}{newline}"
        if shape == "single":
            return f"{text}{entry.section}: {entry.path}{newline}"
        return f"{text}{entry.section}:{newline}{self._line(entry, shape, '  - ' if shape == 'list' else '  ')}"

    def _inserted(self, entry: ExtensionEntry, shape: Shape) -> str:
        """Insert the entry after its section's last entry, with the prefix of the first.

        Args:
            entry: The entry.
            shape: How its section holds entries, a list or a map.

        Returns:
            The new text.

        Raises:
            ExtensionError: If the section's line cannot be found, holds its
                value on the same line, or its first entry is not a list item
                where a list is expected.
        """
        lines = self._lines()
        header = self._header(lines, entry.section)
        first = last = None
        for index in range(header + 1, len(lines)):
            content = lines[index].rstrip("\r\n")
            if not content.strip() or content.lstrip().startswith("#"):
                continue
            if not (content[0] in " \t" or (shape == "list" and (content == "-" or content.startswith("- ")))):
                break
            first = index if first is None else first
            last = index
        if first is None or last is None:
            prefix, last = ("  - " if shape == "list" else "  "), header
        else:
            match = _LIST_ITEM.match(lines[first]) if shape == "list" else re.match(r"\s*", lines[first])
            if match is None:
                raise ExtensionError(
                    f"{entry.section} in {self.path.name} does not start with a list item; add it by hand"
                )
            prefix = match.group(0)
        if not lines[last].endswith("\n"):
            lines[last] = f"{lines[last]}{self._newline}"
        lines.insert(last + 1, self._line(entry, shape, prefix))
        return "".join(lines)

    def _header(self, lines: list[str], section: str) -> int:
        """Find the line that opens a section written as a block.

        Args:
            lines: The manifest's lines.
            section: The section's name.

        Returns:
            The index of the line holding ``<section>:`` and nothing else but
            an optional comment.

        Raises:
            ExtensionError: If the section holds its value on the same line
                (flow style), or no line opens it.
        """
        pattern = re.compile(rf"{re.escape(section)}:(?P<rest>(\s.*)?)")
        for index, line in enumerate(lines):
            match = pattern.fullmatch(line.rstrip("\r\n"))
            if match is None:
                continue
            rest = match.group("rest").strip()
            if rest and not rest.startswith("#"):
                raise ExtensionError(
                    f"{section} is written on one line in {self.path.name}; "
                    "write it as a block, one entry per line, to add to it"
                )
            return index
        raise ExtensionError(f"no line of {self.path.name} starts with '{section}:'; add the entry by hand")

    @staticmethod
    def _document_with(document: dict[str, Any], entry: ExtensionEntry, shape: Shape) -> dict[str, Any]:
        """The document the manifest must parse to once the entry is added.

        Args:
            document: The manifest's document before the edit.
            entry: The entry.
            shape: How its section holds entries.

        Returns:
            A copy of the document declaring the entry.
        """
        expected = dict(document)
        value = document.get(entry.section)
        if shape == "list":
            expected[entry.section] = [*(value or []), entry.path]
        elif shape == "map":
            expected[entry.section] = {**(value or {}), entry.name: entry.path}
        else:
            expected[entry.section] = entry.path
        return expected
