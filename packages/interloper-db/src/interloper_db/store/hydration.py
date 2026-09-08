"""Hydration: translates DB rows into ``Spec`` trees.

This module is a pure transformation layer.  It reads rows from the
database and builds the ``Spec`` tree that ``Component.from_spec``
expects.  No framework classes are instantiated here — reconstruction
happens at the call site via ``spec.reconstruct()``::

    hydrator = Hydrator(catalog, decrypt=decrypt_fn)
    with Session(engine) as session:
        db_component = session.get(Component, component_id)
        spec = hydrator.build_component_spec(session, db_component)
    component = spec.reconstruct()

One builder covers every kind: a component's init is its ``config`` (or its
decrypted ``data`` for secret-bearing kinds) plus whatever its outgoing
relations and children contribute. Relations are read by name and checked
against the row's own vocabulary (the catalog class's declaration, the
kind's anchor as drift fallback), so the walk needs no kind dispatch: an
asset simply holds no ``targets`` edges, a destination no upstream ones.

A target is written out by the rule ``Component.to_spec`` follows: one
owned by a source travels inside that source's own spec, so it is always a
``{"ref": id}``, and a parentless one is written out in full the first time
the walk reaches it and referenced afterwards. Resolving a reference the
document does not carry is the caller's job (see
:meth:`~interloper_db.store.components.ComponentStore._load`).

The Store wraps this pattern in thin ``load_*`` convenience methods, but
any caller can use the hydrator directly to assemble a spec (for example,
to serialize it to JSON and send it across a process boundary).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

from interloper.catalog.base import Catalog
from interloper.component import KINDS
from interloper.errors import CatalogKeyError, ComponentDriftError, HydrationError, format_exception
from interloper.serializable import Spec
from interloper.source.base import SourceDefinition
from sqlmodel import Session, select

from interloper_db.models import Component, ComponentRelation


class Hydrator:
    """Builds ``Spec`` trees from DB rows.

    The hydrator holds a catalog (for import-path lookups) and an optional
    decrypt callable (for secret payloads).  All methods are pure
    transformations — they read rows and return specs without ever
    instantiating framework classes.  Reconstruction is the caller's job.
    """

    def __init__(
        self,
        catalog: Catalog,
        decrypt: Callable[[bytes], bytes] | None = None,
    ) -> None:
        """Initialize the hydrator.

        Args:
            catalog: Catalog used to resolve ``key → import path``.
            decrypt: Optional ``(bytes) -> bytes`` callable for decrypting
                data blobs marked ``encrypted=True``.
        """
        self._catalog = catalog
        self._decrypt = decrypt

    def build_component_spec(
        self,
        session: Session,
        db_component: Component,
        *,
        seen: set[str] | None = None,
    ) -> Spec:
        """Build a spec for a component row of any kind.

        Args:
            session: Active DB session (used to walk relations and children).
            db_component: The component row.
            seen: Ids the walk has already written out in full, extended with
                this row's own. One set is shared by every spec of a document,
                which is what turns a repeated target into a reference.
                Defaults to ``None``, which starts a document of this row alone.

        Returns:
            A ``Spec`` with the row's ``id`` and a fully resolved
            init payload (nested components as nested specs).
        """
        init = self._build_init(session, db_component, seen=seen)
        return Spec(
            path=self._resolve_path(session, db_component),
            id=str(db_component.id) if db_component.id else "",
            init=init or None,
        )

    def decode_data(self, db_component: Component) -> dict[str, Any]:
        """Decrypt (when needed) and JSON-decode a component's data blob.

        Args:
            db_component: The row whose ``data`` bytes should be decoded.

        Returns:
            The decoded configuration dict, or ``{}`` if the row carries
            no data.

        Raises:
            HydrationError: If the row is marked ``encrypted`` but no decrypt
                callable was configured, or if decryption fails.
        """
        if db_component.data is None:
            return {}
        raw = db_component.data
        if db_component.encrypted:
            if not self._decrypt:
                raise HydrationError(
                    f"Component {db_component.id} is encrypted but INTERLOPER_ENCRYPTION_KEY "
                    "is not configured; cannot decrypt"
                )
            try:
                raw = self._decrypt(raw)
            except Exception as e:
                raise HydrationError(
                    f"Failed to decrypt component {db_component.id}; the configured "
                    "INTERLOPER_ENCRYPTION_KEY may be wrong or the data was not encrypted "
                    f"with it: {format_exception(e)}"
                ) from e
        return json.loads(raw)

    # -- Internals -------------------------------------------------------------

    def _build_init(
        self,
        session: Session,
        db_component: Component,
        *,
        seen: set[str] | None = None,
    ) -> dict[str, Any]:
        """Build the init payload for a component row.

        Each relation name the row holds edges under sits in the payload
        under that name, a list when the declared relation is ``many`` and a
        single value otherwise. Children are the one non-relation
        contribution: they embed as the ``assets`` override map, since the
        parent source is the unit of reconstruction. A child the parent's
        declaration has dropped is refused as drift by :meth:`_check_declared`
        before its own relations are read: those rows are declared by the
        class the child no longer belongs to, so reading them would report
        the parent's drift as an undeclared relation name on the child.

        Args:
            session: Active DB session, used to read relations and children.
            db_component: The component row whose init payload is built.
            seen: Ids the walk has already written out in full, extended with
                this row's own before anything else runs (so a cycle back
                onto it is caught the same way whether this is the top of the
                walk or a nested call). Defaults to ``None``, which starts a
                document of this row alone.

        Returns:
            A dict suitable for use as a ``Spec.init``.

        Raises:
            HydrationError: If the row holds edges under a relation name its
                class does not declare, or holds more than one row under a
                relation its class declares single-valued.
        """
        seen = set() if seen is None else seen
        seen.add(str(db_component.id) if db_component.id else "")
        if KINDS[db_component.kind].sensitive:
            init = self.decode_data(db_component)
        else:
            init = dict(db_component.config or {})

        vocabulary = self._catalog.vocabulary(
            db_component.kind, db_component.key, parent_key=db_component.parent_key(session)
        )
        for name, rows in self._relations_by_name(session, db_component.id).items():
            relation = vocabulary.get(name)
            if relation is None:
                raise HydrationError(
                    f"Component {db_component.id} ({db_component.kind}) has '{name}' relations "
                    "its class does not declare"
                )
            values = [self._dst_value(session, row, seen) for row in rows]
            if relation.many:
                init[name] = values
            elif len(values) > 1:
                raise HydrationError(
                    f"Component {db_component.id} ({db_component.kind}) holds {len(values)} rows under "
                    f"single-valued relation '{name}'"
                )
            else:
                init[name] = values[0]

        children = session.exec(
            select(Component).where(Component.parent_id == db_component.id).order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
        ).all()
        assets: dict[str, Any] = {}
        for child in children:
            self._check_declared(db_component, child)
            assets[child.key] = {"id": str(child.id), **self._build_init(session, child, seen=seen)}
        if assets:
            init["assets"] = assets

        return init

    def _check_declared(self, db_parent: Component, db_child: Component) -> None:
        """Refuse a child row whose key its parent no longer declares.

        The same question :func:`~interloper_db.store.status.asset_status`
        asks of an asset row, asked here of the parent's whole child set: the
        catalog's own declaration of the parent is authoritative, and a child
        outside it has drifted out of the source. A parent that does not
        resolve as a source declares nothing to check against, and is left to
        :meth:`_resolve_path` to report.

        Args:
            db_parent: The owning component row, whose declaration decides.
            db_child: The child row whose key is checked.

        Raises:
            ComponentDriftError: If the parent's declaration does not name
                the child's key.
        """
        definition = self._catalog.get(db_parent.key)
        if not isinstance(definition, SourceDefinition):
            return
        if db_child.key not in {asset.key for asset in definition.assets}:
            raise ComponentDriftError(
                f"Asset '{db_child.key}' ({db_child.id}) is no longer declared by source "
                f"'{db_parent.key}' ({db_parent.id}); its catalog key has drifted."
            )

    def _relations_by_name(self, session: Session, src_id: UUID | None) -> dict[str, list[ComponentRelation]]:
        """Group a component's outgoing relations by name, ordered stably.

        Args:
            session: Active DB session used to read the relation rows.
            src_id: The source component's id, or ``None`` for an unflushed row.

        Returns:
            A ``{name: relations}`` mapping ordered by ``(name, dst_id)``, or
            ``{}`` when ``src_id`` is ``None``.
        """
        if src_id is None:
            return {}
        rows = session.exec(
            select(ComponentRelation)
            .where(ComponentRelation.src_id == src_id)
            .order_by(ComponentRelation.name, ComponentRelation.dst_id)  # ty: ignore[invalid-argument-type]
        ).all()
        grouped: dict[str, list[ComponentRelation]] = {}
        for row in rows:
            grouped.setdefault(row.name, []).append(row)
        return grouped

    def _dst_value(self, session: Session, row: ComponentRelation, seen: set[str]) -> dict[str, Any]:
        """Write out one edge's destination, in full or as a reference.

        Args:
            session: Active DB session used to load the destination row.
            row: The edge whose ``dst_id`` is written out.
            seen: Ids the walk has already written out in full, extended with
                the destination's own when it is written out here.

        Returns:
            The destination's own spec as a JSON-able mapping, or the
            ``{"ref": id}`` reference standing in for it.

        Raises:
            HydrationError: If the edge points at a component row that does
                not exist.
        """
        db_dst = session.get(Component, row.dst_id)
        if db_dst is None:  # defensive: FKs make this unreachable
            raise HydrationError(f"Relation {row.src_id} -[{row.name}]-> {row.dst_id} points at a missing component")
        if db_dst.parent_id is not None or str(db_dst.id) in seen:
            return Spec.reference(str(db_dst.id))
        return self.build_component_spec(session, db_dst, seen=seen).model_dump(mode="json")

    def _resolve_path(self, session: Session, db_component: Component) -> str:
        """Look up a component's import path via the catalog.

        A source-owned asset resolves through its parent, whose declaration
        carries the composite path (``module:Source.Asset``) the flat key
        cannot name. An asset row referenced as a relation destination (a job
        target, a hook watch) therefore builds a reconstructible spec.

        Args:
            session: Active DB session used to look up the parent source row.
            db_component: The component row whose import path is resolved.

        Returns:
            The resolved import path.

        Raises:
            CatalogKeyError: If the catalog has no entry for the row's key.
        """
        definition = self._catalog.get(db_component.key, parent_key=db_component.parent_key(session))
        if not definition:
            raise CatalogKeyError(f"Unknown {db_component.kind} key: {db_component.key}")
        return definition.path
