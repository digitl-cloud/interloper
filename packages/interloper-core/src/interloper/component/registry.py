"""Kind registry: the single authority on component kinds.

A *kind* is a category of component (``source``, ``asset``, ``connection``,
``job``, …), anchored by the class that declares it. The registry maps each
kind to its anchor so every kind-level question — which class anchors it,
whether its payload is sensitive, which relation types it may declare — has
one answer, shared by the catalog, the store, and the API.

Population — one mechanism, three feeders:

- every kind, built-ins included, is declared through the
  ``interloper.kinds`` entry-point group (each entry names a ``Component``
  subclass; core declares its own seven in its own ``pyproject.toml``),
  loaded lazily on first lookup;
- catalog discovery auto-registers the anchor of every component class it
  imports, so a package shipping concrete components under
  ``interloper.components`` never *needs* the kinds group — declare it
  anyway when the kind's metadata must resolve without catalog discovery
  (the store and API ask ``sensitive``/``relation_types`` in paths that
  never build a catalog, and the kinds group loads one class where
  discovery imports every connector module);
- ``KINDS.register`` accepts any component class and resolves it to its
  anchor, so registering ``FacebookAdsConnection`` registers ``connection``.
"""

from __future__ import annotations

from importlib.metadata import entry_points

from interloper.component.base import Component, RelationDefinition

_ENTRY_POINT_GROUP = "interloper.kinds"


class KindRegistry:
    """Registry of component kinds, keyed by ``Component.kind``."""

    def __init__(self) -> None:
        """Initialize an empty registry."""
        self._anchors: dict[str, type[Component]] = {}
        self._discovered = False

    def register(self, cls: type[Component]) -> type[Component]:
        """Register a component class's kind (idempotent).

        The kind is anchored to the base-most class in the MRO that
        declares it, so registering any subclass registers the kind itself.

        Returns:
            The class, unchanged (usable as a decorator).
        """
        anchor = self._anchor_of(cls)
        if anchor.kind:
            self._anchors.setdefault(anchor.kind, anchor)
        return cls

    def get(self, kind: str) -> type[Component] | None:
        """Look up the anchoring class for a kind.

        Returns:
            The anchor class, or ``None`` if the kind is unregistered.
        """
        self._load_entry_points()
        return self._anchors.get(kind)

    def __contains__(self, kind: str) -> bool:
        """Check whether a kind is registered.

        Returns:
            True when the kind has a registered anchor.
        """
        return self.get(kind) is not None

    def kinds(self) -> tuple[str, ...]:
        """All registered kinds.

        Returns:
            The kind names, sorted.
        """
        self._load_entry_points()
        return tuple(sorted(self._anchors))

    def sensitive(self, kind: str) -> bool:
        """Whether the kind's instance payload is sensitive (encrypted at rest).

        Returns:
            The anchor class's ``sensitive`` declaration (False if unregistered).
        """
        cls = self.get(kind)
        return bool(cls and cls.sensitive)

    def runnable(self, kind: str) -> bool:
        """Whether a run can target the kind directly.

        Returns:
            The anchor class's ``runnable`` declaration (False if unregistered).
        """
        cls = self.get(kind)
        return bool(cls and cls.runnable)

    def relation_types(self, kind: str) -> dict[str, RelationDefinition]:
        """The relation vocabulary the kind declares.

        Returns:
            The anchor class's ``relation_types`` (empty for unknown kinds).
        """
        cls = self.get(kind)
        return dict(cls.relation_types) if cls else {}

    @staticmethod
    def _anchor_of(component_cls: type[Component]) -> type[Component]:
        """The base-most class in the MRO declaring *component_cls*'s kind.

        Returns:
            The anchoring class.
        """
        anchor = component_cls
        for base in component_cls.__mro__:
            if (
                base is not Component
                and isinstance(base, type)
                and issubclass(base, Component)
                and getattr(base, "kind", "") == component_cls.kind
            ):
                anchor = base
        return anchor

    def _load_entry_points(self) -> None:
        """Register kinds declared via the ``interloper.kinds`` group (once)."""
        if self._discovered:
            return
        self._discovered = True
        for entry_point in entry_points(group=_ENTRY_POINT_GROUP):
            loaded = entry_point.load()
            if isinstance(loaded, type) and issubclass(loaded, Component):
                self.register(loaded)


KINDS = KindRegistry()
