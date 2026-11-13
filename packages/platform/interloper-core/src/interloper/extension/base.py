"""Extension manifests: how an installed package declares what it brings.

Every package, core included, ships an ``extension.yaml`` at the root of its
import package and names that package under the ``interloper.extensions``
entry point:

```toml
[project.entry-points."interloper.extensions"]
acme = "acme_interloper"
```

```yaml
manifest_version: 1
key: acme
name: Acme
description: Acme sources and a Polars representation.
components: acme_interloper
representations:
  - acme_interloper.polars:POLARS_REPRESENTATION
runners:
  ray: acme_interloper.runner:RayRunner
```

``manifest_version``, ``key``, ``name``, ``description`` and ``icon`` say
what the package is; its version is its distribution's. Every other top-level
key is a section, read by the `Registry` constructed for it wherever that
registry lives, so core holds no list of sections and a section no registry
reads is ignored. A section's value is an import string (named by the
extension's key), a list of them (each named by itself) or a map of names to
them; an import string is ``module`` or ``module:attribute``, resolved only
when its registry first needs it.

One package never takes the others down: a manifest, section or entry that
cannot be read is skipped with a warning and recorded in `Manifests.failures`.
`Manifests.extensions` lists every package found, its manifest read or not.
"""

from __future__ import annotations

import logging
import re
import threading
from importlib.metadata import entry_points
from importlib.util import find_spec
from pathlib import Path
from typing import Any

import yaml

from interloper.extension.entry import Extension, ExtensionEntry, ExtensionFailure

logger = logging.getLogger(__name__)

MANIFEST_FILE = "extension.yaml"
MANIFEST_VERSION = 1
IDENTITY_KEYS = ("manifest_version", "key", "name", "description", "icon")
_KEY_PATTERN = re.compile(r"[a-z][a-z0-9_]*")


class Manifests:
    """The installed packages' manifests, read once per process on first use.

    Registries claim the section they read when they are constructed, which
    is what lets `unclaimed` name a section no registry in the process reads,
    a misspelt one most often.
    """

    def __init__(self, group: str = "interloper.extensions") -> None:
        """Initialize the manifests, read lazily.

        Args:
            group: The entry-point group naming each package that ships a
                manifest.
        """
        self._group = group
        self._packages: dict[str, str] = {}
        self._extensions: list[Extension] = []
        self._entries: dict[str, list[ExtensionEntry]] = {}
        self._declared: list[tuple[str, str]] = []
        self._claimed: set[str] = set()
        self._failures: list[ExtensionFailure] = []
        self._loaded = False
        self._lock = threading.Lock()

    def claim(self, section: str) -> None:
        """Record that a registry in this process reads *section*.

        Args:
            section: The section's name.
        """
        self._claimed.add(section)

    def sections(self) -> tuple[str, ...]:
        """The sections the manifests declare.

        Returns:
            The section names, sorted.
        """
        self._read()
        return tuple(sorted(self._entries))

    def entries(self, section: str) -> tuple[ExtensionEntry, ...]:
        """The entries every manifest declares under *section*.

        Args:
            section: The section's name.

        Returns:
            The entries, manifest by manifest in the order the packages are
            found, each manifest's in declaration order; empty when no
            manifest declares the section.
        """
        self._read()
        return tuple(self._entries.get(section, ()))

    def extensions(self) -> tuple[Extension, ...]:
        """Every package the entry point names, whether its manifest could be read or not.

        Returns:
            The extensions, sorted by package; one whose manifest was skipped
            carries no key.
        """
        self._read()
        return tuple(sorted(self._extensions, key=lambda extension: extension.package))

    def unclaimed(self) -> tuple[tuple[str, str], ...]:
        """The sections declared in a manifest that no registry in this process claimed.

        Returns:
            ``(package, section)`` pairs, in manifest order.
        """
        self._read()
        return tuple((package, section) for package, section in self._declared if section not in self._claimed)

    @property
    def failures(self) -> tuple[ExtensionFailure, ...]:
        """What was skipped so far: manifests and sections when read, entries as registries load them.

        Returns:
            The failures, in the order they happened.
        """
        self._read()
        return tuple(self._failures)

    def record(self, failure: ExtensionFailure) -> None:
        """Record something skipped, with a warning naming it.

        Args:
            failure: What was skipped and why.
        """
        self._failures.append(failure)
        where = ", ".join(
            f"{label} '{value}'"
            for label, value in (("package", failure.package), ("section", failure.section), ("entry", failure.entry))
            if value is not None
        )
        logger.warning("Skipping %s: %s", where, failure.error)

    def _read(self) -> None:
        """Find and add every manifest, once, thread-safely.

        Reading the entry-point metadata failing leaves the manifests unread,
        so the next lookup retries instead of latching them empty.
        """
        if self._loaded:
            return
        with self._lock:
            if self._loaded:
                return
            for locator in entry_points(group=self._group):
                self._add(locator.module, locator.dist.version if locator.dist else None)
            self._loaded = True

    def _add(self, package: str, version: str | None) -> None:
        """Add one package's manifest, skipping what cannot be read.

        Args:
            package: The import package that ships the manifest.
            version: The version of the distribution naming it, or ``None``.
        """
        try:
            key, identity, sections = self._parse(package)
        except (ImportError, OSError, TypeError, ValueError, yaml.YAMLError) as error:
            self._extensions.append(Extension(package, version))
            self.record(ExtensionFailure(package, None, None, str(error)))
            return
        self._extensions.append(Extension(package, version, key, **identity))
        self._packages[key] = package
        for section, value in sections.items():
            try:
                section_entries = ExtensionEntry.from_section(package, key, section, value)
            except ValueError as error:
                self.record(ExtensionFailure(package, section, None, str(error)))
                continue
            self._declared.append((package, section))
            self._entries.setdefault(section, []).extend(section_entries)

    def _parse(self, package: str) -> tuple[str, dict[str, str], dict[str, Any]]:
        """Read one package's manifest and check its identity.

        Args:
            package: The import package that ships the manifest.

        Returns:
            The manifest's key, the identity values it gives besides
            (``name``, ``description``, ``icon``) and its sections.

        Raises:
            TypeError: If the manifest is not a mapping, or an identity value
                or a section name is not a string.
            ValueError: If the manifest states another version, or its key is
                missing, malformed or taken.
        """
        document = yaml.safe_load(self._locate(package).read_text())
        if not isinstance(document, dict):
            raise TypeError(f"{MANIFEST_FILE} is not a mapping")
        if document.get("manifest_version") != MANIFEST_VERSION:
            raise ValueError(f"manifest_version is {document.get('manifest_version')!r}, expected {MANIFEST_VERSION}")
        key = document.get("key")
        if not (isinstance(key, str) and _KEY_PATTERN.fullmatch(key)):
            raise ValueError(f"key {key!r} is not a lowercase identifier")
        if key in self._packages:
            raise ValueError(f"key '{key}' is already declared by package '{self._packages[key]}'")
        identity = {name: document[name] for name in ("name", "description", "icon") if name in document}
        for name, value in identity.items():
            if not isinstance(value, str):
                raise TypeError(f"{name} is not a string")
        sections = {section: value for section, value in document.items() if section not in IDENTITY_KEYS}
        for section in sections:
            if not isinstance(section, str):
                raise TypeError(f"section name {section!r} is not a string")
        return key, identity, sections

    def _locate(self, package: str) -> Path:
        """Find a package's manifest without importing the package.

        Args:
            package: The import package's name.

        Returns:
            The path of its ``extension.yaml``.

        Raises:
            ValueError: If the name is not an importable package, or the
                package ships no manifest.
        """
        spec = find_spec(package)
        if spec is None or not spec.submodule_search_locations:
            raise ValueError(f"'{package}' is not an importable package")
        for location in spec.submodule_search_locations:
            path = Path(location) / MANIFEST_FILE
            if path.is_file():
                return path
        raise ValueError(f"package '{package}' ships no {MANIFEST_FILE}")


MANIFESTS = Manifests()
