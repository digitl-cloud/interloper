"""Hydration: turns component rows into live framework components.

A row's component is built directly: its class from the catalog, its init
from its config, the targets of its relations hydrated first and handed to
the constructor, and a source's children as the ``assets`` override map the
source builds its instances from. A target that is itself owned by a source
(an asset) cannot be handed to a constructor, since its own source is the
unit of construction: it is bound once it exists, after that source has
been hydrated within the same call.

One cache backs a whole call (see :meth:`Hydrator._load`), so a component
reached several times (a destination two sources share, a job's own target
reached again through a cross-source upstream) hydrates once and every
consumer binds the identical instance. Hydration fails closed on catalog
drift. Whether a key resolves is asked of the catalog directly: hydration
needs a yes or no, while the descriptive status a response shows
(disabled, missing, unreadable) is the row store's
:meth:`~interloper_db.store.components.base.ComponentStore.read`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

import interloper as il
from interloper.catalog import Catalog
from interloper.errors import format_exception
from interloper.source import SourceDefinition
from interloper.telemetry import attributes
from interloper.telemetry.tracer import tracer
from sqlalchemy import Engine
from sqlmodel import col, select

from interloper_db.errors import ComponentDriftError, HydrationError, NotFoundError
from interloper_db.models import Component, ComponentRelation
from interloper_db.session import session_scope

Cache = dict[UUID, il.Component]
Chain = tuple[tuple[UUID, str], ...]
Owed = list[tuple[UUID, str, list[UUID]]]


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

        Source-owned assets hydrate through their parent source; jobs
        drift-check every target first.

        Args:
            component_id: The component UUID.

        Returns:
            The reconstructed framework component.
        """
        with tracer().start_as_current_span(
            "interloper.store.load", attributes={attributes.TARGET_ID: str(component_id)}
        ):
            return self._load(component_id, {}, ())

    # -- Internals -------------------------------------------------------------

    def _load(self, component_id: UUID, cache: Cache, chain: Chain) -> il.Component:
        """Hydrate a component row (the traced body of :meth:`load`).

        *chain* is the trail of ids currently being hydrated, paired with the
        catalog key each was hydrated under; a relation back onto one of them
        is a cycle, reported from the trail rather than left to exhaust the
        stack.

        Args:
            component_id: The component UUID.
            cache: Components already hydrated within this :meth:`load` call,
                by id, consulted before doing any work.
            chain: Ids currently being hydrated in this call, in resolution
                order.

        Returns:
            The reconstructed framework component.

        Raises:
            NotFoundError: If the component is not found.
            HydrationError: If a relation revisits an id already being
                hydrated earlier in the same call.
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

        chain = (*chain, (component_id, db_component.key))
        if owned_asset:
            return self._load_owned_asset(db_component, cache, chain)
        return self._build(db_component, cache, chain)

    def _load_owned_asset(self, db_asset: Component, cache: Cache, chain: Chain) -> il.Component:
        """Hydrate a source-owned asset through its parent source.

        The parent source is the unit of construction: hydrating it builds
        and caches every asset it declares, so the child is read back from
        the cache. The parent loads through the same *cache* and *chain* as
        the asset itself, so a source hydrates once even when several of its
        owned assets are each reached independently within one call.

        Args:
            db_asset: The asset row, owned by a source.
            cache: Components already hydrated within the enclosing call.
            chain: Ids currently being hydrated in the enclosing call.

        Returns:
            The bound asset instance.

        Raises:
            ComponentDriftError: If the source no longer declares the key.
        """
        assert db_asset.parent_id is not None
        source = self._load(db_asset.parent_id, cache, chain)
        asset = cache.get(db_asset.id)
        if asset is None:
            raise ComponentDriftError(
                f"Asset '{db_asset.key}' ({db_asset.id}) is no longer declared by source '{source.key}'; "
                "its catalog key has drifted."
            )
        return asset

    def _build(self, db_component: Component, cache: Cache, chain: Chain) -> il.Component:
        """Construct a parentless row's component, bind what exists only once it does, and check it.

        Args:
            db_component: The component row, parentless.
            cache: Components already hydrated within the enclosing call; the
                component and the children it owns join it here.
            chain: Ids currently being hydrated in the enclosing call.

        Returns:
            The component, its relations bound and checked.

        Raises:
            ComponentDriftError: If a target's catalog key no longer resolves.
            NotFoundError: If a target row is gone.
            HydrationError: If the stored payload does not decrypt, or the
                class refuses it or the bound relations.
        """
        cls = il.Component.resolve_key(db_component.qualified_key, self._catalog)
        init, owed = self._init(db_component, cache, chain)
        try:
            component = cls(**init)
            cache[db_component.id] = component
            for child in component.owned:
                cache[UUID(child.id)] = child
            for owner_id, name, target_ids in owed:
                cache[owner_id].bind(name, *(self._load(target_id, cache, chain) for target_id in target_ids))
            component.validate_relations()
        except (ComponentDriftError, HydrationError, NotFoundError):
            raise
        except Exception as e:
            # format_exception, never str(e): a ValidationError here carries the
            # decrypted payload of sensitive kinds in its input_value dumps, and
            # this message is persisted into run events and shown in the UI.
            raise HydrationError(
                f"Failed to hydrate {db_component.kind} '{db_component.key}' ({db_component.id}): {format_exception(e)}"
            ) from e
        return component

    def _init(self, db_component: Component, cache: Cache, chain: Chain) -> tuple[dict[str, Any], Owed]:
        """The constructor arguments of a row, and the bindings owed once the component exists.

        The row's config, then each relation name the row holds edges under,
        a list when the declared relation is ``many`` and a single value
        otherwise: a parentless target is hydrated and handed over, an owned
        one (an asset) is owed, since its source is built as a whole. A
        source's children embed as the ``assets`` override map, their own
        config and relations read the same way; a child the parent's
        declaration has dropped is refused as drift by :meth:`_check_declared`
        before its relations are read, since those are declared by the class
        the child no longer belongs to.

        Args:
            db_component: The component row.
            cache: Components already hydrated within the enclosing call.
            chain: Ids currently being hydrated in the enclosing call.

        Returns:
            The keyword arguments to construct with, and the bindings owed as
            ``(owner row id, relation name, target row ids)``.

        Raises:
            HydrationError: If the row holds edges under a relation name its
                class does not declare, or holds more than one row under a
                relation its class declares single-valued.
        """
        init: dict[str, Any] = {"id": str(db_component.id), **db_component.read_config(self._decrypt)}
        vocabulary = self._catalog.vocabulary(db_component.kind, db_component.qualified_key)
        owed: Owed = []
        for name, targets in self._targets(db_component.id).items():
            relation = vocabulary.get(name)
            if relation is None:
                raise HydrationError(
                    f"Component {db_component.id} ({db_component.kind}) has '{name}' relations "
                    "its class does not declare"
                )
            if not relation.many and len(targets) > 1:
                raise HydrationError(
                    f"Component {db_component.id} ({db_component.kind}) holds {len(targets)} rows under "
                    f"single-valued relation '{name}'"
                )
            built = [self._load(target.id, cache, chain) for target in targets if target.parent_id is None]
            owned = [target.id for target in targets if target.parent_id is not None]
            if built:
                init[name] = built if relation.many else built[0]
            if owned:
                owed.append((db_component.id, name, owned))

        children_statement = (
            select(Component).where(Component.parent_id == db_component.id).order_by(col(Component.created_at))
        )
        assets: dict[str, Any] = {}
        with session_scope(self._engine) as session:
            for child in session.exec(children_statement).all():
                self._check_declared(db_component, child)
                child_init, child_owed = self._init(child, cache, chain)
                assets[child.key] = child_init
                owed.extend(child_owed)
        if assets:
            init["assets"] = assets
        return init, owed

    def _targets(self, src_id: UUID) -> dict[str, list[Component]]:
        """The rows a component's outgoing relations point at, by relation name, ordered stably.

        Args:
            src_id: The source component's id.

        Returns:
            A ``{name: target rows}`` mapping ordered by ``(name, dst_id)``.
        """
        statement = (
            select(ComponentRelation.name, Component)
            .join(Component, col(Component.id) == col(ComponentRelation.dst_id))
            .where(ComponentRelation.src_id == src_id)
            .order_by(col(ComponentRelation.name), col(ComponentRelation.dst_id))
        )
        grouped: dict[str, list[Component]] = {}
        with session_scope(self._engine) as session:
            for name, target in session.exec(statement).all():
                grouped.setdefault(name, []).append(target)
        return grouped

    def _check_job_targets(self, db_job: Component) -> None:
        """Fail closed when any job target's catalog key no longer resolves.

        Args:
            db_job: The job row being hydrated.
        """
        for target in self._targets(db_job.id).get("targets", []):
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

    def _check_declared(self, db_parent: Component, db_child: Component) -> None:
        """Refuse a child row whose key its parent no longer declares.

        The catalog's own declaration of the parent is authoritative, and a
        child outside it has drifted out of the source. A parent that does
        not resolve as a source declares nothing to check against.

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
