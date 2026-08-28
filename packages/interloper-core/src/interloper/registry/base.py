"""Registry: the one primitive behind the framework's name → object registries.

Every registry in the framework — kinds, runners, OAuth providers,
representations — is an instance of :class:`Registry`: a lazily-populated
``name → object`` mapping, optionally fed by an entry-point group. One
notion, many instances; each instance's docstring says what it holds and
who consumes it.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from importlib.metadata import entry_points
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Registry(Generic[T]):
    """Name → object registry, optionally fed by an entry-point group.

    Entries register explicitly via :meth:`register`, or are loaded lazily
    from *group* on first lookup. Registration is first-wins and idempotent.
    *adopt* transforms a loaded entry into its ``(name, object)`` pair —
    the default keeps the entry name and object as declared.
    """

    def __init__(
        self,
        group: str | None = None,
        adopt: Callable[[str, Any], tuple[str, T]] | None = None,
    ) -> None:
        """Initialize the registry.

        Args:
            group: Entry-point group to load lazily; ``None`` for a purely
                code-registered registry.
            adopt: Optional transform from a loaded entry to its
                ``(name, object)`` pair.
        """
        self._group = group
        self._adopt = adopt
        self._entries: dict[str, T] = {}
        self._loaded = group is None
        self._load_lock = threading.RLock()

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
        self._load()
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
        if entry is None:
            available = ", ".join(self.keys())
            where = f" in entry-point group '{self._group}'" if self._group else ""
            hint = (
                f" (available: {available})"
                if available
                else " (no entries discovered — is the package declaring it installed?)"
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
        """Iterate the registered names, in :meth:`keys` order.

        Defined so iteration reads as a mapping's does. Without it, the
        presence of :meth:`__getitem__` makes Python fall back to the legacy
        sequence protocol and call ``__getitem__(0)``, which raises
        ``KeyError`` — and linters then suggest ``for name in registry``
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

    def _load(self) -> None:
        """Populate from the entry-point group (once, thread-safely).

        First lookups can race from worker threads (e.g. several assets
        conforming concurrently), so loading is serialized and ``_loaded``
        flips only after every entry registered — a concurrent reader either
        loads itself or waits, never observing a partially-populated registry.
        A failed load leaves ``_loaded`` unset so the next lookup retries
        instead of latching the registry empty.
        """
        if self._loaded or self._group is None:
            return
        with self._load_lock:
            if self._loaded:
                return
            for entry_point in entry_points(group=self._group):
                loaded = entry_point.load()
                name, entry = self._adopt(entry_point.name, loaded) if self._adopt else (entry_point.name, loaded)
                self.register(name, entry)
            self._loaded = True
