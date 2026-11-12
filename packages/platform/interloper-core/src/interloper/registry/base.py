"""Registry: the one primitive behind the framework's name → object registries.

Every registry in the framework (kinds, runners, OAuth providers,
representations, the catalog's component declarations, the CLI's commands,
the scheduler's launchers) is an instance of `Registry`: a lazily-populated
``name → object`` mapping, optionally fed by one section of the installed
packages' extension manifests (`interloper.extension`). One notion, many
instances; each instance's docstring says what it holds and who consumes it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator
from typing import Any, Generic, TypeVar

from interloper.extension import MANIFESTS, ExtensionEntry, ExtensionFailure

T = TypeVar("T")

logger = logging.getLogger(__name__)


class Registry(Generic[T]):
    """Name → object registry, optionally fed by a manifest section.

    Entries register explicitly via `register`, or are loaded lazily from
    *section* on first lookup. Registration is first-wins and idempotent; an
    entry a manifest declares under a name already taken is skipped with a
    warning naming both. *adopt* transforms a loaded entry into its
    ``(name, object)`` pair; the default keeps the entry's name and object
    as declared.

    One installed package never takes the others down: an entry that fails
    to load or adopt is skipped with a warning and recorded in
    `Manifests.failures`. Without *adopt*, an entry's name is its key, so a
    lookup by name loads only the entries declared under that name; a listing
    loads them all.
    """

    def __init__(
        self,
        section: str | None = None,
        adopt: Callable[[str, Any], tuple[str, T]] | None = None,
    ) -> None:
        """Initialize the registry and claim its section.

        Args:
            section: The manifest section to load lazily; ``None`` for a
                purely code-registered registry.
            adopt: Optional transform from a loaded entry to its
                ``(name, object)`` pair.
        """
        self._section = section
        self._adopt = adopt
        self._entries: dict[str, T] = {}
        self._loaded = section is None
        self._loaded_names: set[str] = set()
        self._failures: dict[str, str] = {}
        self._load_lock = threading.RLock()
        if section is not None:
            MANIFESTS.claim(section)

    def register(self, name: str, entry: T) -> None:
        """Register *entry* under *name* (first-wins, idempotent).

        Args:
            name: The registry key.
            entry: The object to register; ignored when the name is taken.
        """
        self._entries.setdefault(name, entry)

    def get(self, name: str) -> T | None:
        """Look up an entry by name.

        Args:
            name: The registry key.

        Returns:
            The registered object, or ``None`` if unknown.
        """
        if name not in self._entries:
            self._load(name if self._adopt is None else None)
        return self._entries.get(name)

    def __getitem__(self, name: str) -> T:
        """Look up an entry by name, failing loudly.

        Args:
            name: The registry key.

        Returns:
            The registered object.

        Raises:
            KeyError: If the name has no entry.
        """
        entry = self.get(name)
        if entry is None and name in self._failures:
            raise KeyError(f"'{name}' is declared in manifest section '{self._section}' but {self._failures[name]}")
        if entry is None:
            available = ", ".join(self.keys())
            where = f" in manifest section '{self._section}'" if self._section else ""
            hint = (
                f" (available: {available})"
                if available
                else " (no entries discovered: is the package declaring it installed?)"
            )
            raise KeyError(f"'{name}' is not registered{where}{hint}")
        return entry

    def keys(self) -> tuple[str, ...]:
        """All registered names.

        Returns:
            The names, sorted.
        """
        self._load()
        return tuple(sorted(self._entries))

    def values(self) -> tuple[T, ...]:
        """All registered objects.

        Returns:
            The objects, ordered by name.
        """
        self._load()
        return tuple(entry for _, entry in sorted(self._entries.items()))

    def items(self) -> tuple[tuple[str, T], ...]:
        """All registered entries.

        Returns:
            ``(name, object)`` pairs, sorted by name.
        """
        self._load()
        return tuple(sorted(self._entries.items()))

    def __iter__(self) -> Iterator[str]:
        """Iterate the registered names, in `keys` order.

        Defined so iteration reads as a mapping's does. Without it, the
        presence of `__getitem__` makes Python fall back to the legacy
        sequence protocol and call ``__getitem__(0)``, which raises
        ``KeyError``, and linters then suggest ``for name in registry``
        over the correct ``registry.keys()``.

        Returns:
            An iterator over the registered names, sorted.
        """
        return iter(self.keys())

    def __len__(self) -> int:
        """Count the registered entries.

        Returns:
            The number of entries, after any lazy load.
        """
        self._load()
        return len(self._entries)

    def __contains__(self, name: str) -> bool:
        """Check whether a name is registered.

        Args:
            name: The registry key.

        Returns:
            True when the name has an entry.
        """
        return self.get(name) is not None

    def _load(self, name: str | None = None) -> None:
        """Populate from the manifest section, each entry once, thread-safely.

        First lookups can race from worker threads (e.g. several assets
        conforming concurrently), so loading is serialized and ``_loaded``
        flips only after every entry registered: a concurrent reader either
        loads itself or waits, never observing a partially-populated registry.
        Reading the manifests failing leaves the registry unloaded, so the
        next lookup retries instead of latching it empty.

        Args:
            name: Load only the entries declared under this name, or every one
                when ``None``.
        """
        if self._loaded or self._section is None or name in self._loaded_names:
            return
        with self._load_lock:
            if self._loaded or name in self._loaded_names:
                return
            for entry in MANIFESTS.entries(self._section):
                if entry.name in self._loaded_names or name not in (None, entry.name):
                    continue
                self._load_entry(entry)
            if name is None:
                self._loaded = True
            else:
                self._loaded_names.add(name)

    def _load_entry(self, entry: ExtensionEntry) -> None:
        """Load and register one entry, skipping it when it fails or its name is taken.

        Args:
            entry: The manifest entry to load.
        """
        try:
            loaded = entry.load()
            name, adopted = self._adopt(entry.name, loaded) if self._adopt else (entry.name, loaded)
        except Exception as error:
            message = f"failed to load: {type(error).__name__}: {error}"
            self._failures[entry.name] = message
            MANIFESTS.record(ExtensionFailure(entry.package, entry.section, entry.name, message))
            logger.debug("Loading entry '%s' (%s) failed", entry.name, entry.path, exc_info=True)
            return
        if name in self._entries and self._entries[name] is not adopted:
            logger.warning(
                "Entry '%s' (%s) of package '%s' is shadowed in section '%s' by an earlier entry under the same "
                "name; the first one wins",
                name,
                entry.path,
                entry.package,
                entry.section,
            )
            return
        self.register(name, adopted)
