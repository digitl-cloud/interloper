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
relations and children contribute. Relations are mapped through the row's
own vocabulary (the catalog class's definition, anchor as drift fallback),
so the walk needs no kind dispatch — an asset simply has no ``target``
relations, a destination no ``dependency`` ones.

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
from interloper.errors import CatalogKeyError, HydrationError
from interloper.serializable import Spec
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

    def build_component_spec(self, session: Session, db_component: Component) -> Spec:
        """Build a spec for a component row of any kind.

        Args:
            session: Active DB session (used to walk relations and children).
            db_component: The component row.

        Returns:
            A ``Spec`` with the row's ``id`` and a fully resolved
            init payload (nested components as nested specs).
        """
        init = self._build_init(session, db_component)
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
                    f"with it: {e}"
                ) from e
        return json.loads(raw)

    # -- Internals -------------------------------------------------------------

    def _build_init(self, session: Session, db_component: Component) -> dict[str, Any]:
        """Build the init payload for a component row.

        Relations are mapped through the kind's own vocabulary: each type
        fills its declared ``field``, shaped by its definition — slotted
        types as ``{slot: value}``, unslotted ones as lists, carrying
        nested specs (``inline``) or bare instance ids.
        Children are the one non-relation contribution: they embed as the
        ``assets`` override map, since the parent source is the unit of
        reconstruction.

        Returns:
            A dict suitable for use as a ``Spec.init``.
        """
        if KINDS[db_component.kind].sensitive:
            init = self.decode_data(db_component)
        else:
            init = dict(db_component.config or {})

        vocabulary = self._catalog.vocabulary(db_component.kind, db_component.key)
        for type_, rels in self._relations_by_type(session, db_component.id).items():
            definition = vocabulary.get(type_)
            if definition is None:
                raise HydrationError(
                    f"Component {db_component.id} ({db_component.kind}) has '{type_}' relations, "
                    "which its kind's vocabulary does not declare"
                )
            if definition.inline:
                values = [self._dst_spec(session, rel).model_dump(mode="json") for rel in rels]
            else:
                values = [str(rel.dst_id) for rel in rels]
            init[definition.field] = (
                {rel.slot: value for rel, value in zip(rels, values)} if definition.slotted else values
            )

        children = session.exec(
            select(Component).where(Component.parent_id == db_component.id).order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
        ).all()
        if assets := {
            child.key: {"id": str(child.id), **self._build_init(session, child)} for child in children
        }:
            init["assets"] = assets

        return init

    def _relations_by_type(self, session: Session, src_id: UUID | None) -> dict[str, list[ComponentRelation]]:
        """Group a component's outgoing relations by type, ordered stably."""
        if src_id is None:
            return {}
        rows = session.exec(
            select(ComponentRelation)
            .where(ComponentRelation.src_id == src_id)
            .order_by(ComponentRelation.slot, ComponentRelation.dst_id)  # ty: ignore[invalid-argument-type]
        ).all()
        grouped: dict[str, list[ComponentRelation]] = {}
        for rel in rows:
            grouped.setdefault(rel.type, []).append(rel)
        return grouped

    def _dst_spec(self, session: Session, rel: ComponentRelation) -> Spec:
        """Build the spec of a relation's destination component."""
        db_dst = session.get(Component, rel.dst_id)
        if db_dst is None:  # defensive: FKs make this unreachable
            raise HydrationError(f"Relation {rel.src_id} -[{rel.type}]-> {rel.dst_id} points at a missing component")
        return self.build_component_spec(session, db_dst)

    def _resolve_path(self, session: Session, db_component: Component) -> str:
        """Look up a component's import path via the catalog.

        Source-owned assets are not top-level catalog entries: their composite
        path (``module:Source.Asset``) comes from the parent source's
        definition, so an asset row referenced as a relation destination
        (a job target, a hook watch) builds a reconstructible spec.

        Returns:
            The resolved import path.

        Raises:
            CatalogKeyError: If the catalog has no entry for the row's key.
        """
        if db_component.kind == "asset" and db_component.parent_id is not None:
            parent = session.get(Component, db_component.parent_id)
            parent_definition = self._catalog.get(parent.key) if parent else None
            for asset_definition in getattr(parent_definition, "assets", []):
                if asset_definition.key == db_component.key:
                    return asset_definition.path

        definition = self._catalog.get(db_component.key)
        if not definition:
            raise CatalogKeyError(f"Unknown {db_component.kind} key: {db_component.key}")
        return definition.path
