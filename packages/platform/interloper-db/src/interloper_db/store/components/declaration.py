"""Declarations: the component rows a file declares, which ``interloper dev`` applies to the dev organisation.

A file declares components as spec documents (``interloper dev -f``) or as
Python instances (``interloper dev weather.paris``). Either way the
components are built first, so a declaration is validated exactly as a run
would validate it, and then walked: every component reachable from the
file's roots through its relations becomes one row, a component reached
twice (a ``{"ref": id}``, a shared Python object) one row, at its place under
the first root that reaches it.

A row's id is derived from the organisation, the file, the component's class
and a handle, so it stays the same across restarts and edits and keeps the
row's run history: the spec's own ``id`` when the document gives one, the
module attribute that names the object, or else its place in the walk
(``0/connection``). A component that changes class is a new row. A source's
assets are rows of their own, whose ids derive from the source's
(`Declaration.child_id`).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import interloper as il
from interloper.catalog import Catalog
from interloper.errors import SpecError
from interloper.serializable import SerializationContext, Spec

DECLARED = uuid.UUID("5c7b6a3e-8f2d-4c1a-9e0b-7d3f2a1c6b45")


@dataclass(frozen=True)
class Declaration:
    """One row a file declares.

    Attributes:
        id: The row's id, derived from the organisation, the file, the
            component's class and its handle.
        kind: The component's kind.
        key: Its catalog key.
        config: Its configuration, every field but its relations and the
            components it owns.
        relations: The relation names the file binds, each to the ids of
            the rows it binds.
        children: For a source, the assets it keeps enabled, each declared
            with its own configuration and relations; ``None`` for any other
            kind.
    """

    id: UUID
    kind: str
    key: str
    config: dict[str, Any] = field(default_factory=dict)
    relations: dict[str, list[UUID]] = field(default_factory=dict)
    children: list[Declaration] | None = None

    @staticmethod
    def child_id(parent_id: UUID, key: str) -> UUID:
        """Derive the id of a declared source's asset, the same on every apply.

        Args:
            parent_id: The source row's id.
            key: The asset's key.

        Returns:
            The asset row's id.
        """
        return uuid.uuid5(parent_id, key)

    @property
    def digest(self) -> str:
        """Hash what the file declares, so an unchanged declaration is not written again.

        Salted with the row's id, so two rows declaring the same secret do not
        share a digest.

        Returns:
            The SHA-256 of the declaration's canonical JSON, hex.
        """
        return hashlib.sha256(json.dumps(self._canonical(), sort_keys=True, default=str).encode()).hexdigest()

    def _canonical(self) -> dict[str, Any]:
        """The declaration as plain data, each list in a stable order.

        Returns:
            Its id, kind, key, configuration, relations and children.
        """
        return {
            "id": str(self.id),
            "kind": self.kind,
            "key": self.key,
            "config": self.config,
            "relations": {name: sorted(str(target) for target in targets) for name, targets in self.relations.items()},
            "children": None if self.children is None else [child._canonical() for child in self.children],
        }


class Declarations:
    """The rows one file declares, in the order they can be written: every row after the rows it binds.

    Attributes:
        file: The file, relative to the project root.
        rows: The declarations, targets before the rows that bind them.
        classes: The component classes they use, for the catalog to enable.
    """

    def __init__(
        self,
        org_id: UUID,
        file: str,
        roots: Mapping[str, il.Component],
        handles: Mapping[int, str],
        seen: MutableMapping[int, UUID] | None = None,
    ) -> None:
        """Walk the components a file declares.

        Args:
            org_id: The organisation the rows belong to.
            file: The file, relative to the project root.
            roots: The file's root components, by their place (a document's
                index, a module attribute's name).
            handles: The handle of each component the file names itself (a
                spec ``id``, a module attribute), by ``id()`` of the object.
            seen: The row id of every component an earlier file of the same
                run declared, by ``id()`` of the object, which this walk adds
                to: a component two modules reach is one row, the first file's.
        """
        self.file = file
        self.rows: list[Declaration] = []
        self.classes: list[type[il.Component]] = []
        self._org_id = org_id
        self._handles = handles
        self._ids: MutableMapping[int, UUID] = {} if seen is None else seen
        for place, root in roots.items():
            self._walk(root, place)

    @classmethod
    def from_documents(cls, org_id: UUID, file: str, specs: Sequence[Spec], catalog: Catalog) -> Declarations:
        """Read the rows a spec file declares, its documents built as ``interloper run -f`` builds them.

        Args:
            org_id: The organisation the rows belong to.
            file: The file, relative to the project root.
            specs: The file's documents, one root each.
            catalog: The catalog a ``key`` resolves through.

        Returns:
            The declarations.

        Raises:
            SpecError: If a document builds something other than a component.
        """
        given = cls._given_ids(spec.model_dump() for spec in specs)
        context = SerializationContext()
        roots: dict[str, il.Component] = {}
        for index, spec in enumerate(specs):
            built = spec.reconstruct(catalog, context=context)
            if not isinstance(built, il.Component):
                raise SpecError(f"document {index} of {file} builds a {type(built).__name__}, not a component")
            roots[str(index)] = built
        context.bind()
        components = cls._reachable(roots.values())
        handles = {id(component): f"id:{component.id}" for component in components if component.id in given}
        return cls(org_id, file, roots, handles)

    @classmethod
    def from_instances(
        cls,
        org_id: UUID,
        file: str,
        roots: Mapping[str, il.Component],
        named: Mapping[str, il.Component],
        seen: MutableMapping[int, UUID] | None = None,
    ) -> Declarations:
        """Read the rows a Python module declares: the instances named as targets, and what they bind.

        Args:
            org_id: The organisation the rows belong to.
            file: The module's file, relative to the project root.
            roots: The instances named as targets, by attribute name.
            named: Every component the module holds at top level, by
                attribute name: a bound component the module names takes that
                name as its handle, so it keeps its row when the code moves.
            seen: The components earlier modules of the same run declared,
                shared so an object two modules reach is one row.

        Returns:
            The declarations.
        """
        handles = {id(component): f"attr:{name}" for name, component in named.items()}
        return cls(org_id, file, roots, handles, seen)

    # -- Internals -------------------------------------------------------------

    def _walk(self, component: il.Component, place: str) -> UUID:
        """Declare a component and everything it binds, each once, targets first.

        A source's asset is declared through its source, and its id derived
        from the source's.

        Args:
            component: The component.
            place: Its place in the walk, the handle of a component the file
                does not name.

        Returns:
            The row's id.
        """
        if component.parent is not None:
            return Declaration.child_id(self._walk(component.parent, f"{place}/parent"), component.key)
        if id(component) in self._ids:
            return self._ids[id(component)]
        handle = self._handles.get(id(component), place)
        name = f"{self._org_id}/{self.file}/{type(component).kind}:{component.qualified_key}/{handle}"
        row_id = uuid.uuid5(DECLARED, name)
        self._ids[id(component)] = row_id
        relations = self._relations(component, place)
        children = None
        if isinstance(component, il.Source):
            children = [
                Declaration(
                    id=Declaration.child_id(row_id, child.key),
                    kind=type(child).kind,
                    key=child.key,
                    config=self._config(child),
                    relations=self._relations(child, f"{place}/{child.key}"),
                )
                for child in component.owned
                if isinstance(child, il.Asset) and child.enabled
            ]
        self.rows.append(
            Declaration(
                id=row_id,
                kind=type(component).kind,
                key=component.qualified_key,
                config=self._config(component),
                relations=relations,
                children=children,
            )
        )
        if type(component) not in self.classes:
            self.classes.append(type(component))
        return row_id

    def _relations(self, component: il.Component, place: str) -> dict[str, list[UUID]]:
        """Declare what a component binds: its targets, an asset of a source by its source.

        Args:
            component: The component.
            place: Its place in the walk.

        Returns:
            The ids each bound relation name holds; a name with nothing bound
            is left out, for the app to bind.
        """
        relations: dict[str, list[UUID]] = {}
        for name in type(component).relations:
            targets = self._bound(component, name)
            if targets:
                relations[name] = [
                    self._walk(target, f"{place}/{name}/{index}") for index, target in enumerate(targets)
                ]
        return relations

    @staticmethod
    def _bound(component: il.Component, name: str) -> list[il.Component]:
        """List what a component binds under a relation name.

        Args:
            component: The component.
            name: The relation name.

        Returns:
            The bound components, none when nothing is.
        """
        bound = component.bound(name)
        if bound is None:
            return []
        return bound if isinstance(bound, list) else [bound]

    @staticmethod
    def _config(component: il.Component) -> dict[str, Any]:
        """Read a component's configuration as a row stores it.

        Args:
            component: The component.

        Returns:
            Its spec's init, without its relations and the assets it owns;
            a source's asset has no ``enabled``, its source's set of assets
            saying it instead.
        """
        init = component.to_spec().init or {}
        left_out = {*type(component).relations}
        if isinstance(component, il.Source):
            left_out.add("assets")
        if isinstance(component, il.Asset) and component.parent is not None:
            left_out.add("enabled")
        return {name: value for name, value in init.items() if name not in left_out}

    @classmethod
    def _reachable(cls, roots: Iterable[il.Component]) -> list[il.Component]:
        """List every component reachable from the roots through their relations and the assets they own.

        Args:
            roots: The root components.

        Returns:
            Each component once.
        """
        seen: dict[int, il.Component] = {}
        pending = list(roots)
        while pending:
            component = pending.pop()
            if id(component) in seen:
                continue
            seen[id(component)] = component
            pending.extend(component.owned)
            for name in type(component).relations:
                pending.extend(cls._bound(component, name))
        return list(seen.values())

    @staticmethod
    def _given_ids(documents: Iterable[dict[str, Any]]) -> set[str]:
        """Collect the ids a spec file gives its components itself.

        Args:
            documents: The file's documents, as plain data.

        Returns:
            Every ``id`` an inline component carries.
        """
        given: set[str] = set()
        pending: list[Any] = list(documents)
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                if (value.get("path") or value.get("key")) and value.get("id"):
                    given.add(str(value["id"]))
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
        return given
