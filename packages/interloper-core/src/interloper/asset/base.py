"""Asset: the core data-producing component of the interloper framework."""

from __future__ import annotations

import asyncio
import inspect
import traceback
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, cast, get_args, get_origin, get_type_hints

from pydantic import Field, PrivateAttr
from typing_extensions import Self

from interloper.asset.context import ExecutionContext
from interloper.asset.upstream import Upstream
from interloper.component import Component, ComponentDefinition, ComponentIdentity, Relation, unwrap_optional
from interloper.conformer import Conformer
from interloper.destination import Destination, IOContext
from interloper.errors import (
    AssetError,
    DataNotFoundError,
    DestinationError,
    NormalizerError,
    PartitionError,
    format_exception,
)
from interloper.events import EventBus, EventType
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.operation import Operation, OperationContext, OperationResult
from interloper.partitioning import (
    Partition,
    PartitionConfig,
    PartitionWindow,
    TimePartition,
    TimePartitionConfig,
    TimePartitionWindow,
)
from interloper.representation import Representation
from interloper.resource.fields import SelectField
from interloper.schema import Schema
from interloper.telemetry import attributes as telemetry_attributes
from interloper.telemetry.tracer import tracer
from interloper.utils import concurrency
from interloper.utils.concurrency import invoke
from interloper.utils.data import is_empty
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_identifier, to_label

if TYPE_CHECKING:
    from interloper.dag import DAG
    from interloper.source import Source

_UNSET = object()

# Parameters of ``data()`` that declare no relation: the instance itself, the
# two values the asset injects, and the catch-all.
_RESERVED_PARAMETERS = frozenset({"self", "context", "source", "kwargs"})
_VARIADIC_KINDS = (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)


warnings.filterwarnings("ignore", message='Field name "schema" in "AssetDefinition"')
# Deliberate: Asset refines the Operation node protocol's plain defaults into real fields.
warnings.filterwarnings("ignore", message='Field name "materializable" in "Asset"')


class AssetDefinition(ComponentDefinition):
    """Definition of an asset: what may fill its relations, its schema, its partitioning.

    Cross-entity references travel as keys: ``relations`` names the kinds and
    keys that may fill each declared link. Same-entity data is inlined:
    ``asset_schema`` is the asset's own output schema and ``partitioning`` its
    own partition config.

    Asset keys come in three forms:

    - **Bare key**, ``"campaigns"``: scoped to the parent source. Used for
      intra-source relations.
    - **Qualified key**, ``"facebook_ads.campaigns"``: globally unique. Used
      for cross-source relations.
    - **Wildcard key**, ``"*.campaigns"``: that asset key from any source.
      Used by many-valued relations to fan in across providers.

    The ``qualified_key`` property returns the globally unique form.
    """

    source_key: str = Field(default="")
    config_schema: dict[str, Any] = Field(default_factory=dict)
    asset_schema: dict[str, Any] | None = Field(default=None)
    partitioning: dict[str, Any] | None = Field(default=None)

    @property
    def qualified_key(self) -> str:
        """Globally unique asset key: ``source_key.asset_key``.

        Falls back to the bare ``key`` if no source key is set
        (e.g. standalone assets not owned by a source).
        """
        return str(ComponentIdentity(self.source_key or None, self.key))


class Asset(Component, Operation):
    """A data-producing component.

    Subclass and implement ``data()`` to define an asset. Every parameter of
    that signature is filled at run time: ``context`` and ``source`` are
    injected, and every other parameter declares a relation read off its
    annotation. A component class is filled with whatever is bound to it, and
    ``il.Upstream`` (or ``list[il.Upstream]``) with the data read from an
    upstream asset of the parameter's name::

        class Revenue(Asset):
            def data(self, connection: MyConnection, orders: Upstream) -> Any:
                return connection.price(orders.data)

    Or use the ``@asset`` decorator for a functional style::

        @asset
        def revenue(connection: MyConnection, orders: Upstream) -> Any:
            return connection.price(orders.data)
    """

    # Definition
    destinations: list[Destination] = Relation("destination", many=True, optional=True)
    schema: ClassVar[type[Schema] | None] = None
    partitioning: ClassVar[PartitionConfig | None] = None
    internal_fields: ClassVar[frozenset[str]] = frozenset({"normalizer"})
    tags: ClassVar[list[str]] = []

    _source_type: ClassVar[type[Source] | None] = None

    # State
    dataset: str = Field(default="")
    default_destination_key: str = Field(default="")
    materializable: bool = Field(default=True)
    materialization_strategy: MaterializationStrategy = SelectField(
        default=MaterializationStrategy.AUTO,
        label="Materialization Strategy",
        description="How this asset's data is checked against its schema.",
        info=(
            "'Auto' coerces data to the schema (or infers a schema when "
            "none is declared), 'Strict' fails on any mismatch, "
            "'Reconcile' requires a schema and coerces values to it."
        ),
    )
    normalizer: Normalizer | None = Field(default=None)

    # Private
    _effective_schema: type[Schema] | None = PrivateAttr(default=None)

    # -- Construction ----------------------------------------------------------

    @classmethod
    def collect(cls) -> None:
        """Collect the declared relations, then infer one per ``data()`` parameter.

        The signature is the declaration: a parameter that is neither reserved
        (see :data:`_RESERVED_PARAMETERS`), variadic (``*args``, ``**extra``:
        nothing is passed through them) nor already declared as a relation
        (by ``relations=``, a :class:`Relation` attribute or an annotation, all
        of which win) gets a relation inferred from its annotation. An asset
        therefore says what it needs once, where it uses it.

        Inference only runs on a class that writes its own ``data()``; a
        subclass that inherits one inherits its relations with it. A parameter
        nothing could ever fill is a definition error, raised from
        :meth:`_infer_relation` as the class is created.
        """
        super().collect()
        if "data" not in cls.__dict__:
            return
        try:
            signature = inspect.signature(cls.data)
        except (TypeError, ValueError):
            return
        hints = cls._data_hints()
        inferred: dict[str, Relation] = {}
        for name, parameter in signature.parameters.items():
            if name in _RESERVED_PARAMETERS or name in cls.relations or parameter.kind in _VARIADIC_KINDS:
                continue
            hint = hints.get(name, parameter.annotation)
            inferred[name] = cls._infer_relation(name, hint, optional=parameter.default is None)
        if inferred:
            cls.relations = {**cls.relations, **inferred}
            for name, relation in inferred.items():
                setattr(cls, name, relation)

    @classmethod
    def _data_hints(cls) -> dict[str, Any]:
        """The resolved type hints of ``data()``.

        Resolving a string annotation needs the globals of the module the
        function was written in. The ``@asset`` decorator's ``data()`` is a
        wrapper, so :func:`typing.get_type_hints` follows its ``__wrapped__``
        to the decorated function, as it does for any wrapper.

        Returns:
            Parameter name to resolved annotation, empty when an annotation
            cannot be resolved at all (a class local to a function body, say),
            in which case the caller falls back to the raw annotations.
        """
        try:
            return get_type_hints(cls.data)
        except Exception:  # noqa: BLE001 - an unresolvable annotation is handled by the caller
            return {}

    @classmethod
    def _infer_relation(cls, name: str, hint: Any, *, optional: bool) -> Relation:
        """The relation one ``data()`` parameter declares.

        Args:
            name: The parameter name, which is the relation's name and, for an
                upstream, the bare asset key it expects.
            hint: The parameter's annotation, resolved when it could be.
            optional: Whether the parameter defaults to ``None``, which makes
                the relation optional whatever its annotation says.

        Returns:
            An ``asset``-kind relation on the parameter's own name for
            ``il.Upstream`` (many-valued for ``list[il.Upstream]``), otherwise
            one targeting the annotated component class.

        Raises:
            TypeError: If the annotation could not be resolved to a type at
                all, or if it names neither a component class nor
                ``il.Upstream``, so nothing could ever fill the parameter.
        """
        target, admits_none = unwrap_optional(hint, {})
        optional = optional or admits_none
        if target is Upstream:
            return Relation("asset", name, optional=optional, name=name)
        if get_origin(target) is list and get_args(target) == (Upstream,):
            return Relation("asset", name, many=True, optional=optional, name=name)
        if isinstance(target, type) and issubclass(target, Component) and target.kind:
            return Relation(target, optional=optional, name=name)
        if isinstance(hint, str):
            raise TypeError(f"{cls.__name__}.data() parameter '{name}': annotation '{hint}' could not be resolved")
        raise TypeError(
            f"{cls.__name__}.data() parameter '{name}' is neither a Component class nor il.Upstream; "
            "nothing can fill it"
        )

    # -- Identity & definition -------------------------------------------------

    @property
    def source(self) -> Source | None:
        """The source this asset belongs to, if any.

        An asset's owning source *is* its parent (see
        :attr:`~interloper.component.base.Component.parent`); only a source
        ever parents an asset, so the cast is safe.
        """
        return cast("Source | None", self.parent)

    @property
    def table(self) -> str:
        """The physical table (or leaf) name this asset materializes to.

        Derived, never stored: the owning source composes it (see
        :meth:`~interloper.source.base.Source.asset_table`) and the result is
        coerced to a valid identifier. Standalone assets use their class key.
        """
        source = self.source
        raw = source.asset_table(self) if source is not None else self.key
        return to_identifier(raw)

    @classmethod
    def classpath(cls) -> str:
        """Fully qualified import path for this asset class.

        Source-owned assets return the composite form
        ``"module:SourceName.AssetName"``, where the colon explicitly
        marks the module / attribute boundary.  Resolution walks the
        attribute chain at class level via the ``AssetRef`` descriptor
        installed on the parent source, with no instantiation required.

        Standalone assets return the regular dotted module path.

        Returns:
            Import path string.
        """
        if cls._source_type is not None:
            source_cls = cls._source_type
            return f"{source_cls.__module__}:{source_cls.__name__}.{cls.__name__}"
        return get_object_path(cls)

    @classmethod
    def definition(cls) -> AssetDefinition:
        """Produce a structured definition of this asset class.

        Uses :meth:`classpath` so that source-owned assets get the correct
        ``"module.Source:asset_kind"`` path.

        Returns:
            An AssetDefinition with metadata derived from the class.
        """
        schema_dict: dict[str, Any] | None = None
        if cls.schema is not None and hasattr(cls.schema, "json_schema"):
            schema_dict = cls.schema.json_schema()

        partitioning_dict: dict[str, Any] | None = None
        if cls.partitioning is not None:
            from dataclasses import asdict

            partitioning_dict = asdict(cls.partitioning)

        return AssetDefinition(
            kind=cls.kind,
            key=cls.key,
            path=cls.classpath(),
            name=cls.name or to_label(cls.__name__),
            icon=cls.icon,
            description=cls.__doc__ or "",
            tags=list(cls.tags),
            config_schema=cls.config_schema(),
            relations=dict(cls.relations),
            asset_schema=schema_dict,
            partitioning=partitioning_dict,
        )

    # -- Reconfiguration -------------------------------------------------------

    def __call__(
        self,
        *,
        id: str | None = None,
        materializable: bool | None = None,
        dataset: str | None = None,
        default_destination_key: str | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
        normalizer: Normalizer | None = _UNSET,  # ty: ignore[invalid-parameter-default]
        **relations: Any,
    ) -> Self:
        """Return a reconfigured copy of this asset.

        Every fixed parameter defaults to ``None``, meaning "leave unchanged".
        Two exceptions: ``normalizer``, whose sentinel default lets an explicit
        ``None`` clear the configured normalizer, and a name in **relations,
        where ``None`` clears the binding, so only leaving the name out
        entirely leaves it as is.

        The copy carries this asset's own bindings and parent, so a copy made
        to flip one field (the non-materializable parents of a mini-DAG, say)
        still reads from the same destinations and upstreams.

        Args:
            id: New component id for the copy.
            materializable: Whether the copy writes to destinations at all.
            dataset: Dataset (schema/namespace) the asset materializes into.
            default_destination_key: When the asset has several destinations,
                the one downstream assets read it from.
            materialization_strategy: How the data is checked against the schema.
            normalizer: Normalizer applied before conform; pass ``None`` to
                explicitly clear it.
            **relations: Replacement targets for the copy's declared relations,
                keyed by relation name: a single component, a list of them, or
                ``None`` to clear the binding.

        Returns:
            A copy of this asset carrying the overrides.

        Raises:
            TypeError: If a keyword argument names no declared relation.
        """
        unknown = [name for name in relations if name not in type(self).relations]
        if unknown:
            raise TypeError(f"{type(self).__name__} declares no relation(s): {', '.join(sorted(unknown))}")
        overrides: dict[str, Any] = {}
        if id is not None:
            overrides["id"] = id
        if materializable is not None:
            overrides["materializable"] = materializable
        if dataset is not None:
            overrides["dataset"] = dataset
        if default_destination_key is not None:
            overrides["default_destination_key"] = default_destination_key
        if materialization_strategy is not None:
            overrides["materialization_strategy"] = materialization_strategy
        if normalizer is not _UNSET:
            overrides["normalizer"] = normalizer
        copy = self.model_copy(update=overrides)
        # The copy inherits the private state, bindings included: its own dict,
        # pointing at the same targets, so rebinding one leaves this asset alone.
        copy._bound = {name: list(targets) for name, targets in self._bound.items()}
        for name, value in relations.items():
            setattr(copy, name, value)
        return copy

    # -- Execution -------------------------------------------------------------

    async def execute(self, context: OperationContext) -> OperationResult:
        """Materialize this asset: the asset's operation.

        The runner-facing adapter over :meth:`materialize_async`; the manual
        entry points (:meth:`run`, :meth:`materialize`) stay the authoring
        API.

        Args:
            context: The facts this execution is scoped to.

        Returns:
            An effectless result: a materialization's effects are its
            destination writes and the events it emits.
        """
        await self.materialize_async(context.partition_or_window, context.dag, context.metadata)
        return OperationResult()

    def run(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to destination.

        Sync entrypoint for scripts, REPLs, and notebooks; drives
        :meth:`run_async` to completion on the bridge loop
        (see :func:`interloper.run`)::

            data = asset.run()

        Async code awaits :meth:`run_async` instead.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The raw execution result.
        """
        return concurrency.run(self.run_async(partition_or_window, dag, metadata))

    async def run_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to destination.

        Resolves the context and everything the declared relations fill
        (upstreams through the DAG), then runs the data function. Sync
        ``data()`` functions are automatically offloaded to a thread via
        ``asyncio.to_thread``; async ``data()`` functions are awaited
        natively.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The raw execution result.
        """
        self._validate_partitioning(partition_or_window)

        source = self.source
        context = ExecutionContext(
            asset_key=self.key,
            partition_or_window=partition_or_window,
            partitioning=self.partitioning,
            metadata=metadata,
            asset_id=self.id,
            source_id=source.id if source is not None else None,
        )

        kwargs = await self._build_kwargs(context, partition_or_window, dag)

        exec_meta = self._event_metadata(metadata or {}, partition_or_window)
        span_attrs = telemetry_attributes.from_metadata(exec_meta)
        EventBus.emit(
            EventType.ASSET_DATA_STARTED,
            metadata={**exec_meta, "message": f"Executing '{self.key}'"},
        )
        try:
            with tracer().start_as_current_span("interloper.asset.data", attributes=span_attrs):
                result = await invoke(self.data, **kwargs)
            EventBus.emit(
                EventType.ASSET_DATA_COMPLETED,
                metadata={**exec_meta, "message": f"Executed '{self.key}'"},
            )
        except Exception as e:
            EventBus.emit(
                EventType.ASSET_DATA_FAILED,
                metadata={
                    **exec_meta,
                    "error": format_exception(e),
                    "traceback": traceback.format_exc(),
                    "message": f"Execution of '{self.key}' failed: {format_exception(e)}",
                },
            )
            raise

        # Normalization + conform is CPU-bound (pandas/pyarrow); offload it so
        # it never blocks the event loop while other assets run concurrently.
        result = await asyncio.to_thread(self._normalize_and_conform, result)

        return result

    def materialize(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and write the result to all configured destinations.

        Sync entrypoint for scripts, REPLs, and notebooks; drives
        :meth:`materialize_async` to completion on the bridge loop
        (see :func:`interloper.run`)::

            asset.materialize()

        Async code awaits :meth:`materialize_async` instead.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The execution result, or ``None`` if the asset is not materializable.
        """
        return concurrency.run(self.materialize_async(partition_or_window, dag, metadata))

    async def materialize_async(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and write the result to all configured destinations.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The execution result, or ``None`` if the asset is not materializable.
        """
        if not self.materializable:
            return None

        metadata = metadata or {}
        result = await self.run_async(partition_or_window, dag, metadata)
        await self._destination_write(partition_or_window, metadata, result)
        return result

    def data(self, **kwargs: Any) -> Any:
        """Return this asset's data.

        Subclasses must override this method.

        Args:
            **kwargs: The resolved call arguments: the execution context, the
                owning source and whatever fills the declared relations, keyed
                by the parameter names of the overriding signature.

        Raises:
            NotImplementedError: If the subclass does not implement ``data()``.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement data()")

    def partition_row_counts(self) -> dict[str, int]:
        """Return row counts grouped by this asset's partition column.

        Delegates to :meth:`Destination.partition_row_counts` using the
        configured default destination.

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            PartitionError: If this asset is not partitioned.
        """
        if self.partitioning is None:
            raise PartitionError(
                f"Asset '{self.key}' is not partitioned. "
                "Cannot compute partition row counts without a partition column."
            )

        destination = self._read_destination()
        context = IOContext(asset=self)
        return destination.partition_row_counts(context)

    # -- Internals -------------------------------------------------------------
    async def _build_kwargs(
        self,
        context: ExecutionContext,
        partition_or_window: Partition | PartitionWindow | None,
        dag: DAG | None,
    ) -> dict[str, Any]:
        """Build the keyword arguments ``data()`` is called with.

        One value per parameter: ``context`` and ``source`` are injected
        directly, an ``asset``-kind relation is read through
        :meth:`_read_upstreams` (every leg when it is many-valued, the single
        leg or ``None`` otherwise), and any other relation resolves to what is
        bound to it, or to what it can fill itself with (see
        :meth:`~interloper.component.base.Component.resolve`).

        Args:
            context: The execution context injected as the ``context`` parameter.
            partition_or_window: Scope the upstreams are read at.
            dag: DAG the upstream assets are looked up in. ``None`` is allowed
                only when no relation has an upstream to read.

        Returns:
            Keyword arguments to pass to ``data()``.

        Raises:
            AssetError: If an upstream is bound but no DAG was provided.
        """
        kwargs: dict[str, Any] = {}
        relations = type(self).relations

        for name in inspect.signature(self.data).parameters:
            if name in ("self", "kwargs"):
                continue
            if name == "context":
                kwargs["context"] = context
            elif name == "source":
                kwargs["source"] = self._parent
            elif name not in relations:
                continue
            elif "asset" in relations[name].kinds():
                # Bind-time validation is what makes this cast sound: only an asset is accepted here.
                targets = cast("list[Asset]", self._bound.get(name, []))
                if targets and dag is None:
                    raise AssetError(
                        f"Asset '{self.key}' has upstreams but no DAG provided. "
                        "Pass a DAG to run() or materialize() for upstream resolution."
                    )
                legs = await self._read_upstreams(name, targets, dag, partition_or_window, context.metadata)
                kwargs[name] = legs if relations[name].many else (legs[0] if legs else None)
            else:
                # Lazily-built clients cost under the data() span, not here.
                with tracer().start_as_current_span(
                    "interloper.asset.resolve_resource",
                    attributes={**self._span_attributes(), telemetry_attributes.RESOURCE_NAME: name},
                ):
                    kwargs[name] = self.resolve(name)

        return kwargs

    async def _read_upstreams(
        self,
        name: str,
        targets: list[Asset],
        dag: DAG | None,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Read every leg bound to one upstream relation.

        A leg whose upstream has nothing materialized where the destination
        looks (no table or object for that scope at all) is handed over with
        ``data=None`` and a warning event, never dropped: the asset decides
        what a missing leg means. An existing but empty scope is not this
        case; it comes back as whatever the destination returns for an empty
        read (an empty list or frame), not ``None``. Any other read failure
        fails the asset, optional relation or not. A bound upstream absent
        from *dag* is skipped the same way, with its own warning: relation
        validation and the DAG's graph construction only tolerate such a
        binding when the relation is optional, so the leg simply does not
        exist for this run.

        Args:
            name: The ``data()`` parameter the legs are read for.
            targets: The bound upstream assets; one absent from *dag* is
                skipped rather than read, and one present is read through the
                DAG's own node.
            dag: The DAG the upstream assets are looked up in, ``None`` when
                the run was given none, which leaves nothing to look up.
            partition_or_window: Scope of the reads.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            One :class:`Upstream` per leg present in *dag*, in binding order.

        Raises:
            AssetError: If a leg cannot be read for a reason other than
                missing data.
        """
        legs: list[Upstream] = []
        for target in targets:
            # DAG membership is by id, so the leg is read from the DAG's own node: for a mini-DAG
            # that is a read-only copy of the bound asset, and reading the binding would miss it.
            if dag is not None and target.id not in dag.operation_map:
                EventBus.emit(
                    EventType.LOG,
                    metadata={
                        **self._event_metadata(metadata, partition_or_window),
                        "level": "WARNING",
                        "message": (
                            f"Asset '{self.key}' upstream '{target.qualified_key}' for parameter '{name}' "
                            "is not in the DAG; the leg is skipped"
                        ),
                    },
                )
                continue
            upstream = cast("Asset", dag.operation_map[target.id]) if dag is not None else target
            try:
                data = await self._destination_read(upstream, partition_or_window, metadata)
            except AssetError as error:
                if not isinstance(error.__cause__, DataNotFoundError):
                    raise
                EventBus.emit(
                    EventType.LOG,
                    metadata={
                        **self._event_metadata(metadata, partition_or_window),
                        "level": "WARNING",
                        "message": (
                            f"Asset '{self.key}' found no data in upstream '{upstream.qualified_key}' for "
                            f"parameter '{name}' at {partition_or_window}; the leg is passed with data=None"
                        ),
                    },
                )
                data = None
            legs.append(Upstream(asset=upstream, data=data))
        return legs

    async def _destination_write(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
        result: Any,
    ) -> None:
        """Write the execution result to all configured destinations.

        Args:
            partition_or_window: Scope of the run, narrowed to what the asset
                actually consumes before it reaches the destination.
            metadata: Run-level metadata (e.g. run_id, backfill_id), carried
                onto the emitted write events.
            result: The normalized and conformed data to write. An empty result
                is skipped with a warning.
        """
        destinations = self._destinations()
        if not destinations:
            return

        if is_empty(result):
            EventBus.emit(
                EventType.LOG,
                metadata={
                    **self._event_metadata(metadata, partition_or_window),
                    "level": "WARNING",
                    "message": (
                        f"Asset '{self.key}' produced no data; skipping write to {len(destinations)} destination(s)"
                    ),
                },
            )
            return

        destination_context = IOContext(
            asset=self,
            partition_or_window=self.effective_partition(partition_or_window),
            metadata=metadata,
            schema=self._effective_schema or self.schema,
        )

        for destination in destinations:
            destination_key = destination.key
            destination_meta = self._event_metadata(metadata, partition_or_window)
            destination_meta["destination_key"] = destination_key
            span_attrs = telemetry_attributes.from_metadata(destination_meta)
            EventBus.emit(
                EventType.DEST_WRITE_STARTED,
                metadata={**destination_meta, "message": f"Writing '{self.key}'"},
            )
            try:
                with tracer().start_as_current_span("interloper.destination.write", attributes=span_attrs):
                    await invoke(destination.write, destination_context, result)
                EventBus.emit(
                    EventType.DEST_WRITE_COMPLETED,
                    metadata={**destination_meta, "message": f"Wrote '{self.key}'"},
                )
            except Exception as e:
                EventBus.emit(
                    EventType.DEST_WRITE_FAILED,
                    metadata={
                        **destination_meta,
                        "error": format_exception(e),
                        "traceback": traceback.format_exc(),
                        "message": f"Failed to write '{self.key}': {format_exception(e)}",
                    },
                )
                raise

    async def _destination_read(
        self,
        upstream_asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> Any:
        """Read data from an upstream asset's read destination (see ``_read_destination``).

        Args:
            upstream_asset: The asset whose materialized data is read.
            partition_or_window: Scope of the read, narrowed to what the
                *upstream* asset consumes.
            metadata: Run-level metadata (e.g. run_id, backfill_id), carried
                onto the emitted read events.

        Returns:
            The data read from the upstream asset's destination.

        Raises:
            AssetError: If no destination is found for the upstream asset.
        """
        destination = upstream_asset._read_destination()

        effective_partition = upstream_asset.effective_partition(partition_or_window)
        destination_context = IOContext(
            asset=upstream_asset,
            partition_or_window=effective_partition,
            metadata=metadata,
            schema=upstream_asset.schema,
        )

        destination_meta = self._event_metadata(metadata, effective_partition)
        destination_meta["destination_key"] = destination.key
        span_attrs = telemetry_attributes.from_metadata(destination_meta)
        span_attrs[telemetry_attributes.UPSTREAM_KEY] = upstream_asset.key
        EventBus.emit(
            EventType.DEST_READ_STARTED,
            metadata={**destination_meta, "message": f"Reading '{upstream_asset.key}'"},
        )
        try:
            with tracer().start_as_current_span("interloper.destination.read", attributes=span_attrs):
                result = await invoke(destination.read, destination_context)
            EventBus.emit(
                EventType.DEST_READ_COMPLETED,
                metadata={**destination_meta, "message": f"Read '{upstream_asset.key}'"},
            )
        except Exception as e:
            EventBus.emit(
                EventType.DEST_READ_FAILED,
                metadata={
                    **destination_meta,
                    "error": format_exception(e),
                    "traceback": traceback.format_exc(),
                    "message": f"Failed to read '{upstream_asset.key}': {format_exception(e)}",
                },
            )
            raise AssetError(
                f"Failed to load data from upstream asset '{upstream_asset.key}': {format_exception(e)}"
            ) from e

        return result

    def _normalize_and_conform(self, result: Any) -> Any:
        """Apply optional normalization, then always conform to the schema.

        Normalization (when a normalizer is configured) reshapes the data:
        flattening, column renaming, missing-key fill.  Conform then enforces
        the declared schema according to the materialization strategy: it
        runs whether or not a normalizer is configured, so a declared schema
        is always a checked contract.

        Args:
            result: The raw value returned by ``data()``.

        Returns:
            The normalized and conformed result.
        """
        # Two spans, not one: normalization is skipped without a normalizer.
        span_attrs = self._span_attributes()
        if self.normalizer is not None:
            with tracer().start_as_current_span("interloper.normalizer.normalize", attributes=span_attrs):
                result = self.normalizer.normalize(result)
        with tracer().start_as_current_span("interloper.asset.conform", attributes=span_attrs):
            return self._conform(result)

    def _conform(self, result: Any) -> Any:
        """Enforce the asset's schema according to the materialization strategy.

        AUTO: reconcile when a schema is declared, infer one otherwise.
        STRICT: schema required; reject extra, missing, or mistyped fields.
        RECONCILE: schema required; align columns and coerce values.

        The schema operations come from a single :class:`Conformer`, resolved
        once from the data's representation (rows or DataFrame). Tabular data
        is canonicalized on the way in (dict / model / generator →
        ``list[dict]``); non-tabular data without a schema passes through
        untouched. The effective schema (declared, or inferred under AUTO) is
        carried to destinations via ``IOContext.schema``.

        Args:
            result: The data to conform, already normalized when a normalizer
                is configured.

        Returns:
            The conformed result.

        Raises:
            AssetError: If the strategy requires a schema but none is declared,
                or if a schema is declared but the data is not tabular.
        """
        strategy = self.materialization_strategy
        schema = self.schema

        if schema is None and strategy != MaterializationStrategy.AUTO:
            raise AssetError(f"Asset '{self.key}': strategy='{strategy.value}' requires a schema.")

        conformer = Representation.of(result).conformer
        try:
            result = conformer.prepare(result)
        except NormalizerError as e:
            if schema is None:
                # Non-tabular data without a contract (e.g. arbitrary objects
                # bound for a FileDestination) passes through untouched.
                self._effective_schema = None
                return result
            raise AssetError(
                f"Asset '{self.key}' declares a schema but returned data that cannot be checked against it: {e}"
            ) from e

        if schema is None:
            with tracer().start_as_current_span("interloper.asset.infer_schema", attributes=self._span_attributes()):
                self._effective_schema = self._infer_schema(conformer, result)
            return result

        self._effective_schema = schema
        if strategy == MaterializationStrategy.STRICT:
            conformer.validate(result, schema, strict=True)
            return result
        with tracer().start_as_current_span("interloper.conformer.reconcile", attributes=self._span_attributes()):
            return conformer.reconcile(result, schema)

    def _infer_schema(self, conformer: Conformer, result: Any) -> type[Schema] | None:
        """Best-effort schema inference for the IO boundary (AUTO, no declared schema).

        Inference is metadata for destinations (DDL, typed loads): it must
        never fail a materialization, so any inference error yields ``None``.

        Args:
            conformer: The conformer resolved for *result*.
            result: The prepared (canonical) data.

        Returns:
            The inferred schema, or ``None`` when the data is empty or
            inference fails.
        """
        if is_empty(result):
            return None
        try:
            return conformer.infer(result)
        except Exception:  # noqa: BLE001 - inference is best-effort metadata
            return None

    def _validate_partitioning(
        self,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Validate partitioning constraints before execution.

        Args:
            partition_or_window: The scope the run was given. ``None`` means the
                run was unscoped.

        Raises:
            PartitionError: If partitioning constraints are violated.
        """
        if self.partitioning is None and partition_or_window is not None:
            warnings.warn(f"Asset '{self.key}' is not partitioned, partition/partition_window will be ignored")

        if self.partitioning is not None and partition_or_window is None:
            raise PartitionError(f"Asset '{self.key}' is partitioned, but no partition/partition_window provided")

        if (
            self.partitioning is not None
            and isinstance(partition_or_window, PartitionWindow)
            and not self.partitioning.allow_window
        ):
            raise PartitionError(f"Asset '{self.key}' does not support windowed runs (allow_window=False).")

        if isinstance(self.partitioning, TimePartitionConfig):
            self._validate_time_partitioning(self.partitioning, partition_or_window)

    def _validate_time_partitioning(
        self,
        partitioning: TimePartitionConfig,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Validate a scope against the asset's time partitioning.

        A time-partitioned asset requires a *time* partition: only those carry
        the granularity, so anything else would reach the asset as a scope that
        cannot answer ``granularity`` or ``bounds``, the contract
        ``context.partition`` rests on.

        Args:
            partitioning: The asset's declared time partition config.
            partition_or_window: The scope the run was given. ``None`` short-circuits
                the check; the missing-scope case is caught by the caller.

        Raises:
            PartitionError: If the scope is not a time partition, its
                granularity disagrees with the asset's, or it reaches before
                the asset's ``start``.
        """
        scope = partition_or_window
        if scope is None:
            return

        if not isinstance(scope, (TimePartition, TimePartitionWindow)):
            raise PartitionError(
                f"Asset '{self.key}' is time-partitioned, but the run was given a "
                f"{type(scope).__name__}. Use `TimePartition` or `TimePartitionWindow`."
            )

        if scope.granularity is not partitioning.granularity:
            raise PartitionError(
                f"Asset '{self.key}' is partitioned by {partitioning.granularity.value}, "
                f"but the run was given a {scope.granularity.value} partition."
            )

        if partitioning.start is None:
            return

        earliest = scope.start if isinstance(scope, PartitionWindow) else scope.value
        if partitioning.granularity.truncate(earliest) < partitioning.start:
            raise PartitionError(
                f"Asset '{self.key}' has no data before {partitioning.start.isoformat()}, "
                f"but the run reaches back to {partitioning.granularity.truncate(earliest).isoformat()}."
            )

    def _validate_destination(self, destination: Destination) -> None:
        """Check that a destination is one this asset's ``destinations`` relation accepts.

        Args:
            destination: The destination instance to check. Any destination is
                accepted when the relation narrows no key.

        Raises:
            DestinationError: If the relation does not accept the destination.
        """
        relation = type(self).relations["destinations"]
        if relation.accepts(destination.kind, destination.identity, owner=self.identity):
            return
        raise DestinationError(
            f"Destination '{type(destination).__name__}' is not compatible with "
            f"asset '{self.key}'. Allowed keys: [{', '.join(relation.keys())}]"
        )

    def _destinations(self) -> list[Destination]:
        """The destinations this asset writes to and is read from.

        Nothing is resolved here: a source trickles its own destinations into
        every asset that has none of its own at construction, so what is bound
        is the whole answer. The check catches a binding made by something
        other than :meth:`~interloper.component.base.Component.bind` (a
        hydration writing straight into the instance, say).

        Returns:
            The bound destinations, in binding order; empty when none is bound.
        """
        destinations: list[Destination] = self.destinations
        for destination in destinations:
            self._validate_destination(destination)
        return destinations

    def _read_destination(self) -> Destination:
        """The destination downstream readers load this asset from.

        The destination whose key equals ``default_destination_key`` when one
        is configured and bound, else the first bound destination.

        Returns:
            The destination to read from.

        Raises:
            AssetError: If the asset has no destination at all.
        """
        destinations = self._destinations()
        if not destinations:
            raise AssetError(f"No destination found for upstream asset '{self.key}'")
        preferred = next((d for d in destinations if d.key == self.default_destination_key), None)
        return preferred or destinations[0]

    def _span_attributes(self) -> dict[str, str]:
        """Identity attributes for spans opened below the asset's own span.

        Run id and partition are omitted deliberately: the ancestor spans
        already carry them, and these are emitted from code paths that
        don't hold the run metadata.

        Returns:
            The asset's identity attributes.
        """
        return telemetry_attributes.from_metadata(self._event_metadata({}))

    def _event_metadata(
        self,
        metadata: dict[str, Any],
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> dict[str, Any]:
        """Build the base event metadata dict for this asset.

        Merges run-level metadata with the asset's component identity fields.

        Args:
            metadata: Run-level metadata (e.g. run_id, backfill_id).
            partition_or_window: Current partition scope.

        Returns:
            The merged metadata dict.
        """
        base: dict[str, Any] = {
            **metadata,
            "component_id": self.id,
            "component_kind": self.kind,
            "component_key": self.key,
            "qualified_key": self.qualified_key,
            "partition_or_window": str(partition_or_window) if partition_or_window else None,
        }
        source = self.source
        if source is not None:
            base["source_id"] = source.id
        return base
