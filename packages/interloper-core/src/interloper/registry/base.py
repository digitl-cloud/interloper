"""Registry: the one primitive behind the framework's name → object registries.

Every registry in the framework — kinds, runners, OAuth providers,
representations — is an instance of :class:`Registry`: a lazily-populated
``name → object`` mapping, optionally fed by an entry-point group. One
notion, many instances; each instance's docstring says what it holds and
who consumes it.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
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

    def register(self, name: str, obj: T) -> None:
        """Register *obj* under *name* (first-wins, idempotent)."""
        self._entries.setdefault(name, obj)

    def get(self, name: str) -> T | None:
        """Look up an entry by name.

        Returns:
            The registered object, or ``None`` if unknown.
        """
        self._load()
        return self._entries.get(name)

    def __getitem__(self, name: str) -> T:
        """Look up an entry by name, failing loudly.

        Returns:
            The registered object.

        Raises:
            KeyError: If the name has no entry.
        """
        obj = self.get(name)
        if obj is None:
            available = ", ".join(self.keys())
            where = f" in entry-point group '{self._group}'" if self._group else ""
            hint = (
                f" (available: {available})"
                if available
                else " (no entries discovered — is the package declaring it installed?)"
            )
            raise KeyError(f"'{name}' is not registered{where}{hint}")
        return obj

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
        return tuple(obj for _, obj in sorted(self._entries.items()))

    def items(self) -> tuple[tuple[str, T], ...]:
        """All registered entries.

        Returns:
            ``(name, object)`` pairs, sorted by name.
        """
        self._load()
        return tuple(sorted(self._entries.items()))

    def __contains__(self, name: str) -> bool:
        """Check whether a name is registered.

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
                name, obj = self._adopt(entry_point.name, loaded) if self._adopt else (entry_point.name, loaded)
                self.register(name, obj)
            self._loaded = True
