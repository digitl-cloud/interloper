"""Hydration: turns component rows into live framework components.

A row becomes a ``Spec`` tree (:meth:`Hydrator.build_component_spec`), which
``Component.from_spec`` reconstructs; :meth:`Hydrator.load` does both and
fails closed on drift. Whether a key resolves is asked of the catalog
directly: hydration needs a yes or no, while the descriptive status a
response shows (disabled, missing, unreadable) is the row store's
:meth:`~interloper_db.store.components.base.ComponentStore.read`.

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
document does not carry is resolved through :meth:`Hydrator.load` itself,
within the same call.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

import interloper as il
from interloper.catalog.base import Catalog
from interloper.errors import CatalogKeyError, ComponentDriftError, HydrationError, NotFoundError, format_exception
from interloper.serializable import Spec
from interloper.source.base import SourceDefinition
from interloper.telemetry import attributes
from interloper.telemetry.tracer import tracer
from sqlalchemy import Engine
from sqlmodel import select

from interloper_db.models import Component, ComponentRelation
from interloper_db.session import session_scope


class Hydrator:
    """Turns component rows into live framework components."""

    def __init__(
        self,
        engine: Engine,
        catalog: Catalog,
        decrypt: Callable[[bytes], bytes] | None = None,
    ) -> None:
        """Initialize the hydrator.

        Args:
            engine: Engine rows are read on.
            catalog: Catalog the rows' keys resolve against.
            decrypt: Optional ``(bytes) -> bytes`` callable for decrypting
                payloads marked ``encrypted=True``.
        """
        self._engine = engine
        self._catalog = catalog
        self._decrypt = decrypt

    def load(self, component_id: UUID) -> il.Component:
        """Hydrate a framework component of any kind from its row.

        Source-owned assets hydrate through their parent source and are
        extracted from it; jobs drift-check every target first. One cache
        backs the whole call (see :meth:`_load`), so a component reached
        several times within the same document (a job's own target and,
        through a cross-source upstream, that same target again) hydrates
        once and every consumer binds the identical instance. Fails closed
        on any catalog drift: see :meth:`_load` for what a missing row, a
        drifted key or a failed reconstruction raises.

        Args:
            component_id: The component UUID.

        Returns:
            The reconstructed framework component.
        """
        with tracer().start_as_current_span(
            "interloper.store.load", attributes={attributes.TARGET_ID: str(component_id)}
        ):
            return self._load(component_id, {})

    def build_component_spec(self, db_component: Component, *, seen: set[str] | None = None) -> Spec:
        """Build a spec for a component row of any kind.

        Args:
            db_component: The component row.
            seen: Ids the walk has already written out in full, extended with
                this row's own. One set is shared by every spec of a document,
                which is what turns a repeated target into a reference.
                Defaults to ``None``, which starts a document of this row alone.

        Returns:
            A ``Spec`` with the row's ``id`` and a fully resolved
            init payload (nested components as nested specs).
        """
        with session_scope(self._engine):
            init = self._build_init(db_component, seen=seen)
            path = self._resolve_path(db_component)
        return Spec(
            path=path,
            id=str(db_component.id) if db_component.id else "",
            init=init or None,
        )

    # -- Internals -------------------------------------------------------------

    def _load(
        self,
        component_id: UUID,
        cache: dict[UUID, il.Component],
        chain: tuple[tuple[UUID, str], ...] = (),
    ) -> il.Component:
        """Hydrate a component row (the traced body of :meth:`load`).

        A spec reference (``{"ref": id}``) is resolved from the document
        first, which reconstruction does on its own, and only then through
        :meth:`_resolve_reference`, which re-enters here with the same
        *cache* and *chain* rather than starting a fresh call: a job that
        targets both a source and a consumer of that source's asset holds
        one instance of it, not two, and an owned asset's parent (loaded
        through :meth:`_load_owned_asset`) shares the cache the same way.
        *chain* is the trail of ids currently being hydrated, paired with
        the catalog key each was hydrated under; a reference back onto one
        of them is a cycle, reported from the trail rather than left to
        exhaust the stack.

        Args:
            component_id: The component UUID.
            cache: Components already hydrated within this :meth:`load` call,
                by id, consulted before doing any work.
            chain: Ids currently being hydrated in this call, in resolution
                order. Defaults to ``()``, the top-level call's empty trail.

        Returns:
            The reconstructed framework component.

        Raises:
            NotFoundError: If the component is not found.
            ComponentDriftError: If a catalog key no longer resolves.
            HydrationError: If the stored payload does not decrypt, if
                reconstruction fails, or if a reference revisits an id
                already being hydrated earlier in the same call.
        """
        if component_id in cache:
            return cache[component_id]
        cyclic_key = next((key for id_, key in chain if id_ == component_id), None)
        if cyclic_key is not None:
            trail = " -> ".join(key for _, key in chain)
            raise HydrationError(f"Reference cycle while hydrating: {trail} -> {cyclic_key}")

        with session_scope(self._engine) as session:
            db_component = session.get(Component, component_id)
            if not db_component:
                raise NotFoundError(f"Component {component_id} not found")
            owned_asset = db_component.kind == "asset" and db_component.parent_id is not None
            if not owned_asset:
                self._check_resolves(db_component)
                if db_component.kind == "job":
                    self._check_job_targets(db_component)
                spec = self.build_component_spec(db_component)

        # Reconstruction happens outside the session: it imports classes and,
        # for owned assets, recursively loads the parent source.
        chain = (*chain, (component_id, db_component.key))
        if owned_asset:
            component = self._load_owned_asset(db_component.parent_id, db_component.key, component_id, cache, chain)
        else:

            def resolve(reference: str) -> il.Component:
                return self._load(UUID(reference), cache, chain)

            try:
                component = il.Component.from_spec(spec, resolve=resolve)
            except (ComponentDriftError, NotFoundError):
                # A dedicated handler (the API's drift endpoint, say) needs to
                # tell these apart from a generic hydration failure, so they
                # pass through untouched rather than folding into the catch-all.
                raise
            except Exception as e:
                # format_exception, never str(e): a ValidationError here carries the
                # decrypted payload of sensitive kinds in its input_value dumps, and
                # this message is persisted into run events and shown in the UI.
                raise HydrationError(
                    f"Failed to hydrate {db_component.kind} '{db_component.key}' ({db_component.id}): "
                    f"{format_exception(e)}"
                ) from e
        cache[component_id] = component
        return component

    def _load_owned_asset(
        self,
        parent_id: UUID,
        key: str,
        asset_id: UUID,
        cache: dict[UUID, il.Component],
        chain: tuple[tuple[UUID, str], ...],
    ) -> il.Asset:
        """Hydrate a source-owned asset through its parent source.

        The parent source is the unit of reconstruction: loading it binds all
        its assets, and the child is picked out by key. The parent loads
        through the same *cache* and *chain* as the asset itself, so a
        source hydrates once even when several of its owned assets are each
        reached independently within one :meth:`load` call.

        Args:
            parent_id: UUID of the owning source component.
            key: Catalog key of the asset to pick out of the source.
            asset_id: UUID of the asset row, for the drift error message.
            cache: Components already hydrated within the enclosing
                :meth:`load` call.
            chain: Ids currently being hydrated in the enclosing call.

        Returns:
            The bound asset instance.

        Raises:
            ComponentDriftError: If the source no longer declares the key.
        """
        source = cast(il.Source, self._load(parent_id, cache, chain))
        for asset in source.assets:
            if asset.key == key:
                return asset
        raise ComponentDriftError(
            f"Asset '{key}' ({asset_id}) is no longer declared by source '{source.key}'; its catalog key has drifted."
        )

    def _check_job_targets(self, db_job: Component) -> None:
        """Fail closed when any job target's catalog key no longer resolves.

        Args:
            db_job: The job row being hydrated.
        """
        statement = select(ComponentRelation).where(
            ComponentRelation.src_id == db_job.id, ComponentRelation.name == "targets"
        )
        with session_scope(self._engine) as session:
            for relation in session.exec(statement).all():
                target = session.get(Component, relation.dst_id)
                if target is not None:
                    self._check_resolves(target, subject=f"Job '{db_job.name}' ({db_job.id})")

    def _check_resolves(self, db_component: Component, *, subject: str | None = None) -> None:
        """Fail closed when a row's key does not resolve in this deployment's catalog.

        Args:
            db_component: The row whose qualified key is checked.
            subject: What cannot be hydrated because of it; ``None`` names the
                row itself.

        Raises:
            ComponentDriftError: If the key does not resolve.
        """
        if self._catalog.get(db_component.qualified_key) is not None:
            return
        row = f"{db_component.kind} '{db_component.qualified_key}' ({db_component.id})"
        if subject is None:
            raise ComponentDriftError(
                f"{row[0].upper()}{row[1:]} cannot be hydrated: its catalog key does not resolve in this deployment."
            )
        raise ComponentDriftError(f"{subject} cannot be hydrated: target {row} does not resolve in this deployment.")

    def _build_init(self, db_component: Component, *, seen: set[str] | None = None) -> dict[str, Any]:
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
        init = db_component.read_config(self._decrypt)
        vocabulary = self._catalog.vocabulary(db_component.kind, db_component.qualified_key)
        for name, rows in self._relations_by_name(db_component.id).items():
            relation = vocabulary.get(name)
            if relation is None:
                raise HydrationError(
                    f"Component {db_component.id} ({db_component.kind}) has '{name}' relations "
                    "its class does not declare"
                )
            values = [self._dst_value(row, seen) for row in rows]
            if relation.many:
                init[name] = values
            elif len(values) > 1:
                raise HydrationError(
                    f"Component {db_component.id} ({db_component.kind}) holds {len(values)} rows under "
                    f"single-valued relation '{name}'"
                )
            else:
                init[name] = values[0]

        children_statement = (
            select(Component).where(Component.parent_id == db_component.id).order_by(Component.created_at)  # ty: ignore[invalid-argument-type]
        )
        assets: dict[str, Any] = {}
        with session_scope(self._engine) as session:
            for child in session.exec(children_statement).all():
                self._check_declared(db_component, child)
                assets[child.key] = {"id": str(child.id), **self._build_init(child, seen=seen)}
        if assets:
            init["assets"] = assets

        return init

    def _check_declared(self, db_parent: Component, db_child: Component) -> None:
        """Refuse a child row whose key its parent no longer declares.

        The question a row's qualified key asks of the catalog, asked here
        of the parent's whole child set: the
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

    def _relations_by_name(self, src_id: UUID | None) -> dict[str, list[ComponentRelation]]:
        """Group a component's outgoing relations by name, ordered stably.

        Args:
            src_id: The source component's id, or ``None`` for an unflushed row.

        Returns:
            A ``{name: relations}`` mapping ordered by ``(name, dst_id)``, or
            ``{}`` when ``src_id`` is ``None``.
        """
        if src_id is None:
            return {}
        statement = (
            select(ComponentRelation)
            .where(ComponentRelation.src_id == src_id)
            .order_by(ComponentRelation.name, ComponentRelation.dst_id)  # ty: ignore[invalid-argument-type]
        )
        with session_scope(self._engine) as session:
            rows = session.exec(statement).all()
        grouped: dict[str, list[ComponentRelation]] = {}
        for row in rows:
            grouped.setdefault(row.name, []).append(row)
        return grouped

    def _dst_value(self, row: ComponentRelation, seen: set[str]) -> dict[str, Any]:
        """Write out one edge's destination, in full or as a reference.

        Args:
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
        with session_scope(self._engine) as session:
            db_dst = session.get(Component, row.dst_id)
            if db_dst is None:  # defensive: FKs make this unreachable
                raise HydrationError(
                    f"Relation {row.src_id} -[{row.name}]-> {row.dst_id} points at a missing component"
                )
            if db_dst.parent_id is not None or str(db_dst.id) in seen:
                return Spec.reference(str(db_dst.id))
            return self.build_component_spec(db_dst, seen=seen).model_dump(mode="json")

    def _resolve_path(self, db_component: Component) -> str:
        """Look up a component's import path via the catalog.

        A source-owned asset resolves through its parent, whose declaration
        carries the composite path (``module:Source.Asset``) the flat key
        cannot name. An asset row referenced as a relation destination (a job
        target, a hook watch) therefore builds a reconstructible spec.

        Args:
            db_component: The component row whose import path is resolved.

        Returns:
            The resolved import path.

        Raises:
            CatalogKeyError: If the catalog has no entry for the row's key.
        """
        definition = self._catalog.get(db_component.qualified_key)
        if not definition:
            raise CatalogKeyError(f"Unknown {db_component.kind} key: {db_component.key}")
        return definition.path
