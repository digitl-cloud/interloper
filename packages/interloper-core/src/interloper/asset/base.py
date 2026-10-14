"""Asset: the core data-producing component of the interloper framework."""

from __future__ import annotations

import asyncio
import functools
import inspect
import warnings
from collections.abc import Sized
from typing import TYPE_CHECKING, Any, ClassVar, cast, get_args, get_origin, get_type_hints, overload

from pydantic import Field
from pydantic._internal._model_construction import ModelMetaclass

from interloper.asset.context import ExecutionContext
from interloper.asset.output import Output
from interloper.asset.upstream import Upstream
from interloper.component import Component, ComponentDefinition, ComponentIdentity, Relation, unwrap_optional
from interloper.destination import Destination, IOContext
from interloper.errors import (
    AssetError,
    PartitionError,
)
from interloper.events import EventBus, EventType, Step, StepKind
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.operation import Operation, OperationContext, OperationResult
from interloper.partitioning import Partition, PartitionWindow
from interloper.resource.fields import SelectField
from interloper.schema import Schema
from interloper.serializable import ClassBody
from interloper.telemetry import attributes as telemetry_attributes
from interloper.telemetry.tracer import tracer
from interloper.utils import concurrency
from interloper.utils.concurrency import invoke
from interloper.utils.data import is_empty
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_identifier

if TYPE_CHECKING:
    from types import FunctionType

    from interloper.dag import DAG
    from interloper.source import Source

# Parameters of ``data()`` that declare no relation: the instance itself, the
# two values the asset injects, and the catch-all.
_RESERVED_PARAMETERS = frozenset({"self", "context", "source", "kwargs"})
_VARIADIC_KINDS = (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL)


warnings.filterwarnings("ignore", message='Field name "schema" in "AssetDefinition"')


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


class AssetMeta(ModelMetaclass):
    """The metaclass of every asset class: an asset class read off a source instance is that source's asset.

    An asset class is its own descriptor, as a `Relation` is. Read on a class it
    is the class itself, which is what composite import paths
    (``module:Shop.orders``) walk; read on a `Source` instance it is that
    instance's live asset of the same key; read on any other instance it is the
    class, as for any class attribute.
    """

    @overload
    def __get__(cls, instance: None, owner: type | None = None) -> type[Asset]: ...
    @overload
    def __get__(cls, instance: Source, owner: type | None = None) -> Asset: ...
    @overload
    def __get__(cls, instance: object, owner: type | None = None) -> type[Asset]: ...
    def __get__(cls, instance: object, owner: type | None = None) -> Any:
        """Resolve to the class, or to a source instance's live asset of this key.

        Args:
            instance: The object the attribute is read on, ``None`` on a class.
            owner: The class the attribute is read through; unused, part of
                the descriptor protocol. Defaults to ``None``.

        Returns:
            The live asset when *instance* is a source, else this class.

        Raises:
            AttributeError: If the source holds no asset of this key (a source
                narrowed to some of its assets by an override map).
        """
        from interloper.source.base import Source

        if not isinstance(instance, Source):
            return cls
        key = cast("type[Asset]", cls).key
        for asset in instance.assets:
            if asset.key == key:
                return asset
        raise AttributeError(f"Source '{type(instance).__name__}' has no asset with key '{key}'")


class Asset(Operation, metaclass=AssetMeta):
    """A data-producing component.

    Subclass and implement ``data()`` to define an asset. Every parameter of
    that signature is filled at run time: ``context`` and ``source`` are
    injected, and every other parameter declares a relation read off its
    annotation. A component class is filled with whatever is bound to it, and
    ``il.Upstream`` (or ``list[il.Upstream]``) with the data read from an
    upstream asset of the parameter's name:

    ```py
    class Revenue(Asset):
        def data(self, connection: MyConnection, orders: Upstream) -> Any:
            return connection.price(orders.data)
    ```

    Or use the ``@asset`` decorator for a functional style:

    ```py
    @asset
    def revenue(connection: MyConnection, orders: Upstream) -> Any:
        return connection.price(orders.data)
    ```
    """

    # Definition
    kind: ClassVar[str] = "asset"
    destinations: list[Destination] = Relation("destination", many=True, optional=True)
    schema: ClassVar[type[Schema] | None] = None
    internal_fields: ClassVar[frozenset[str]] = frozenset({"normalizer"})
    tags: ClassVar[list[str]] = []

    _source_type: ClassVar[type[Source] | None] = None
    _parameters: ClassVar[tuple[str, ...]] = ()

    # State
    dataset: str | None = Field(default=None, json_schema_extra={"x-inherit": True})
    default_destination_key: str | None = Field(default=None, json_schema_extra={"x-inherit": True})
    materialization_strategy: MaterializationStrategy | None = SelectField(
        default=None,
        inherit=True,
        label="Materialization Strategy",
        description="How this asset's data is checked against its schema; inherited from its source when unset.",
        info=(
            "'Reconcile' coerces data to the schema (or infers a schema when "
            "none is declared); 'Strict' requires a schema and fails on any "
            "mismatch. Both write the schema's types."
        ),
    )
    normalizer: Normalizer | None = Field(default=None, json_schema_extra={"x-inherit": True})

    # -- Construction ----------------------------------------------------------

    @classmethod
    def _collect(cls) -> None:
        """Collect the declared relations, then infer one per ``data()`` parameter.

        The signature is the declaration: a parameter that is neither reserved
        (see `_RESERVED_PARAMETERS`), variadic (``*args``, ``**extra``:
        nothing is passed through them) nor already declared as a relation
        (by ``relations=``, a `Relation` attribute or an annotation, all
        of which win) gets a relation inferred from its annotation. An asset
        therefore says what it needs once, where it uses it.

        Inference only runs on a class that writes its own ``data()``; a
        subclass that inherits one inherits its relations with it. A parameter
        nothing could ever fill is a definition error, raised from
        `_infer_relation` as the class is created. The parameters the call
        fills are recorded, in order, in ``_parameters``, so a run never reads
        the signature again.
        """
        super()._collect()
        if "data" not in cls.__dict__:
            return
        try:
            signature = inspect.signature(cls.data)
        except (TypeError, ValueError):
            return
        hints = cls._data_hints()
        inferred: dict[str, Relation] = {}
        parameters: list[str] = []
        for name, parameter in signature.parameters.items():
            if name == "self" or parameter.kind in _VARIADIC_KINDS:
                continue
            parameters.append(name)
            if name in _RESERVED_PARAMETERS or name in cls.relations:
                continue
            hint = hints.get(name, parameter.annotation)
            relation = cls._infer_relation(name, hint, optional=parameter.default is None)
            inferred[name] = relation.model_copy(update={"name": name})
        cls._parameters = tuple(parameters)
        if inferred:
            cls.relations = {**cls.relations, **inferred}
            for name, relation in inferred.items():
                setattr(cls, name, relation)

    @classmethod
    def _data_hints(cls) -> dict[str, Any]:
        """The resolved type hints of ``data()``.

        Resolving a string annotation needs the globals of the module the
        function was written in. The ``@asset`` decorator's ``data()`` is a
        wrapper, so `typing.get_type_hints` follows its ``__wrapped__``
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

        An optional upstream is inferred ``on_delete="detach"``: the parameter
        already tolerates the upstream being absent, so nothing is owed to it and
        deleting the upstream must not be refused on its behalf. A parameter
        naming a component class keeps the default ``block`` whether it is
        optional or not: this ruling covers asset upstreams only, and a
        connection or config a read consumes stays undeletable while it is
        bound.

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
        on_delete = "detach" if optional else "block"
        if target is Upstream:
            return Relation("asset", name, optional=optional, on_delete=on_delete)
        if get_origin(target) is list and get_args(target) == (Upstream,):
            return Relation("asset", name, many=True, optional=optional, on_delete=on_delete)
        if isinstance(target, type) and issubclass(target, Component) and target.kind:
            return Relation(target, optional=optional)
        if isinstance(hint, str):
            raise TypeError(f"{cls.__name__}.data() parameter '{name}': annotation '{hint}' could not be resolved")
        raise TypeError(
            f"{cls.__name__}.data() parameter '{name}' is neither a Component class nor il.Upstream; "
            "nothing can fill it"
        )

    @classmethod
    def body_from_function(cls, fn: FunctionType) -> ClassBody:
        """Read a decorated function into an asset body: the function becomes ``data()``.

        A function whose first parameter is ``self`` is a method asset: it
        receives the asset's source as ``self``. The generated ``data()`` wraps
        the function in the `functools.wraps` sense, so relation inference reads
        the original's annotations through ``__wrapped__`` and the stamped
        ``__signature__`` is what the parameters read as; an async function
        gives an async ``data()``, which the run awaits on its loop.

        Args:
            fn: The decorated function.

        Returns:
            The body carrying the generated ``data()``.
        """
        signature = inspect.signature(fn)
        method = next(iter(signature.parameters), None) == "self"
        if inspect.iscoroutinefunction(fn):

            async def data(self: Asset, **kwargs: Any) -> Any:
                return await fn(*([self.source] if method else []), **kwargs)

        else:

            def data(self: Asset, **kwargs: Any) -> Any:
                return fn(*([self.source] if method else []), **kwargs)

        functools.update_wrapper(data, fn)
        if not method:
            self_parameter = inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)
            signature = signature.replace(parameters=[self_parameter, *signature.parameters.values()])
        data.__signature__ = signature  # ty: ignore[invalid-assignment]
        return ClassBody.from_function(fn, members={"data": data})

    # -- Identity --------------------------------------------------------------

    @property
    def source(self) -> Source | None:
        """The source this asset belongs to, if any.

        An asset's owning source *is* its parent (see
        `parent`); only a source
        ever parents an asset, so the cast is safe.
        """
        return cast("Source | None", self.parent)

    @property
    def table(self) -> str:
        """The physical table (or leaf) name this asset materializes to, or refers to.

        Named by the owning source (see `Source.table`); a standalone asset
        uses its class key.
        """
        source = self.source
        return source.table(self) if source is not None else to_identifier(self.key)

    @classmethod
    def classpath(cls) -> str:
        """Fully qualified import path for this asset class.

        Source-owned assets return the composite form
        ``"module:SourceName.AssetName"``, where the colon explicitly
        marks the module / attribute boundary.  Resolution walks the
        attribute chain at class level: an asset class read on its source
        class is the class itself (see `AssetMeta`), with no instantiation
        required.

        Standalone assets return the regular dotted module path.

        Returns:
            Import path string.
        """
        if cls._source_type is not None:
            source_class = cls._source_type
            return f"{source_class.__module__}:{source_class.__name__}.{cls.__name__}"
        return get_object_path(cls)

    @classmethod
    def definition(cls) -> AssetDefinition:
        """Produce a structured definition of this asset class.

        The path comes from `classpath`, so a source-owned asset gets
        the composite ``"module.Source:asset_kind"`` form.

        Returns:
            An AssetDefinition with the output schema and partitioning inlined.
        """
        has_schema = cls.schema is not None and hasattr(cls.schema, "json_schema")
        asset_schema = cls.schema.json_schema() if has_schema else None
        partitioning = cls.partitioning.to_definition() if cls.partitioning is not None else None
        return AssetDefinition(**dict(super().definition()), asset_schema=asset_schema, partitioning=partitioning)

    # -- Execution -------------------------------------------------------------

    async def execute(self, context: OperationContext) -> OperationResult:
        """Materialize this asset: the asset's operation.

        The runner-facing adapter over `materialize_async`; the manual
        entry points (`run`, `materialize`) stay the authoring
        API.

        Args:
            context: The facts this execution is scoped to.

        Returns:
            An effectless result: a materialization's effects are its
            destination writes and the events it emits.
        """
        await self.materialize_async(context.window, context.dag, context.metadata)
        return OperationResult()

    def run(
        self,
        window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to destination.

        Sync entrypoint for scripts, REPLs, and notebooks; drives
        `run_async` to completion on the bridge loop
        (see `interloper.run`):

        ```py
        data = asset.run()
        ```

        Async code awaits `run_async` instead.

        Args:
            window: The window the run covers; a partition is accepted as the window of one.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The raw execution result.
        """
        return concurrency.run(self.run_async(window, dag, metadata))

    async def run_async(
        self,
        window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to destination.

        Resolves the context and the inputs, runs ``data()`` and shapes its
        result (see `_produce`); a sync ``data()`` is offloaded to a thread,
        an async one awaited.

        Args:
            window: The window the run covers; a partition is accepted as the window of one.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The raw execution result.
        """
        window = self.effective_window(PartitionWindow.of(window))
        return (await self._produce(window, dag, metadata or {})).data

    def materialize(
        self,
        window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and write the result to all configured destinations.

        Sync entrypoint for scripts, REPLs, and notebooks; drives
        `materialize_async` to completion on the bridge loop
        (see `interloper.run`):

        ```py
        asset.materialize()
        ```

        Async code awaits `materialize_async` instead.

        Args:
            window: The window the run covers; a partition is accepted as the window of one.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The execution result, or ``None`` if the asset is disabled.
        """
        return concurrency.run(self.materialize_async(window, dag, metadata))

    async def materialize_async(
        self,
        window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and write the result to all configured destinations.

        Args:
            window: The window the run covers; a partition is accepted as the window of one.
            dag: DAG for dependency resolution (required if asset has upstreams).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The execution result, or ``None`` if the asset is disabled.
        """
        if not self.enabled:
            return None
        window = self.effective_window(PartitionWindow.of(window))
        metadata = metadata or {}
        output = await self._produce(window, dag, metadata)
        await self._land(output, window, metadata)
        return output.data

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

        Delegates to `Destination.partition_row_counts` using the
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

        return self.default_destination.partition_row_counts(IOContext(asset=self))

    @property
    def default_destination(self) -> Destination:
        """The destination downstream readers load this asset from.

        The destination whose key is ``default_destination_key`` when one is
        set and bound, else the first bound destination.

        Raises:
            AssetError: If the asset has no destination at all.
        """
        destinations: list[Destination] = self.destinations
        if not destinations:
            raise AssetError(f"No destination found for upstream asset '{self.key}'")
        preferred = next(
            (destination for destination in destinations if destination.key == self.default_destination_key), None
        )
        return preferred or destinations[0]

    # -- Internals -------------------------------------------------------------
    async def _build_kwargs(
        self,
        context: ExecutionContext,
        window: PartitionWindow | None,
        dag: DAG | None,
    ) -> dict[str, Any]:
        """Build the keyword arguments ``data()`` is called with.

        One value per recorded parameter (see `_collect`): ``context`` and ``source`` are injected
        directly, an ``asset``-kind relation is handed over through
        `_upstreams` (every upstream when it is many-valued, the single
        one or ``None`` otherwise), and any other relation resolves to what is
        bound to it, or to what it can fill itself with (see
        `resolve`).

        Args:
            context: The execution context injected as the ``context`` parameter.
            window: Scope the upstreams are read at.
            dag: DAG the upstream assets are looked up in. ``None`` is allowed
                only when no relation has an upstream to read.

        Returns:
            Keyword arguments to pass to ``data()``.

        Raises:
            AssetError: If an upstream is bound but no DAG was provided.
        """
        kwargs: dict[str, Any] = {}
        relations = type(self).relations

        for name in type(self)._parameters:
            if name == "context":
                kwargs["context"] = context
            elif name == "source":
                kwargs["source"] = self._parent
            elif name not in relations:
                continue
            elif "asset" in relations[name].kinds:
                # Bind-time validation is what makes this cast sound: only an asset is accepted here.
                targets = cast("list[Asset]", self._bound.get(name, []))
                if targets and dag is None:
                    raise AssetError(
                        f"Asset '{self.key}' has upstreams but no DAG provided. "
                        "Pass a DAG to run() or materialize() for upstream resolution."
                    )
                upstreams = await self._upstreams(name, targets, dag, window, context.metadata)
                kwargs[name] = upstreams if relations[name].many else (upstreams[0] if upstreams else None)
            else:
                # Lazily-built clients cost under the data() span, not here.
                with tracer().start_as_current_span(
                    "interloper.asset.resolve_resource",
                    attributes={**self._span_attributes(), telemetry_attributes.RESOURCE_NAME: name},
                ):
                    kwargs[name] = self.resolve(name)

        return kwargs

    async def _upstreams(
        self,
        name: str,
        targets: list[Asset],
        dag: DAG | None,
        window: PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> list[Upstream]:
        """Hand every upstream bound to one relation to `_upstream`, through the DAG's own nodes.

        DAG membership is by id, so each upstream is taken from the DAG's node
        when it is there: for a mini-DAG that node is a read-only copy of the
        binding. An upstream absent from the DAG is skipped with a warning when
        this asset reads data, since there is no run to read it for; a kind
        that only refers to its upstreams refers to the binding itself.

        Args:
            name: The ``data()`` parameter the upstreams are for.
            targets: The bound upstream assets.
            dag: The DAG the upstreams are looked up in, ``None`` when the run
                was given none.
            window: The run's window.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            One `Upstream` per upstream handed over, in binding order.
        """
        upstreams: list[Upstream] = []
        for target in targets:
            node = dag.operation_map.get(target.id) if dag is not None else target
            if node is None and self.reads_upstream_data:
                EventBus.emit(
                    EventType.LOG,
                    metadata=self.event_metadata(
                        metadata,
                        window,
                        level="WARNING",
                        message=(
                            f"Asset '{self.key}' upstream '{target.qualified_key}' for parameter '{name}' "
                            "is not in the DAG; it is skipped"
                        ),
                    ),
                )
                continue
            upstreams.append(await self._upstream(name, cast("Asset", node or target), window, metadata))
        return upstreams

    async def _upstream(
        self, name: str, target: Asset, window: PartitionWindow | None, metadata: dict[str, Any]
    ) -> Upstream:
        """Hand one upstream to ``data()``: its data, or a reference to it for a kind that reads none.

        Args:
            name: The ``data()`` parameter the upstream is bound to.
            target: The upstream asset, the DAG's node when it has one.
            window: The run's window.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            The upstream as ``data()`` receives it.
        """
        if self.reads_upstream_data:
            return await Upstream.read(target, self, window, metadata, parameter=name)
        return Upstream.reference(target, self.default_destination)

    async def _produce(self, window: PartitionWindow | None, dag: DAG | None, metadata: dict[str, Any]) -> Output:
        """Run ``data()`` over the resolved inputs and shape what it returned.

        A generator is consumed inside the observed step, so a failure while
        it yields fails the step; the rest of the shaping is CPU-bound
        (pandas, pyarrow) and runs off the event loop.

        Args:
            window: The window this asset consumes, already narrowed.
            dag: DAG the upstream assets are looked up in.
            metadata: Run-level metadata carried onto the emitted events.

        Returns:
            The run's output.
        """
        self._check_window(window)
        context = ExecutionContext(self, window, metadata)
        kwargs = await self._build_kwargs(context, window, dag)
        with Step(StepKind.ASSET_DATA, self.event_metadata(metadata, window), subject=f"'{self.key}'"):
            result = Output._rows(await invoke(self.data, **kwargs))
        return await asyncio.to_thread(self._output, result)

    def _output(self, result: Any) -> Output:
        """Shape what ``data()`` returned into this asset's output (see `Output.from_result`).

        A kind whose output is not data overrides it (see `View`).

        Args:
            result: The value ``data()`` returned.

        Returns:
            The output.
        """
        return Output.from_result(result, self)

    async def _land(self, output: Output, window: PartitionWindow | None, metadata: dict[str, Any]) -> None:
        """Write the output to every destination, one observed step each.

        An empty output is skipped with a warning. A kind that lands
        elsewhere overrides it (see `View`).

        Args:
            output: The run's output.
            window: The window the asset consumed, ``None`` for an unpartitioned asset.
            metadata: Run-level metadata carried onto the emitted events.
        """
        destinations: list[Destination] = self.destinations
        if not destinations:
            return
        if is_empty(output.data):
            EventBus.emit(
                EventType.LOG,
                metadata=self.event_metadata(
                    metadata,
                    window,
                    level="WARNING",
                    message=(
                        f"Asset '{self.key}' produced no data; skipping write to {len(destinations)} destination(s)"
                    ),
                ),
            )
            return
        context = IOContext(asset=self, window=window, metadata=metadata, schema=output.schema or self.schema)
        rows = len(output.data) if isinstance(output.data, Sized) else None
        for destination in destinations:
            step = Step(
                StepKind.DEST_WRITE,
                self.event_metadata(metadata, window, destination_key=destination.key),
                subject=f"'{self.key}'",
            )
            with step:
                await invoke(destination.write, context, output.data)
                step.record(rows=rows)

    def _check_window(self, window: PartitionWindow | None) -> None:
        """Check a run's window against this asset's partitioning.

        An unpartitioned asset accepts any window and ignores it; a partitioned one defers to
        its config, whose predicate is prefixed with the asset's name.

        Args:
            window: The run's window, ``None`` when the run was unscoped.

        Raises:
            PartitionError: If the window is not valid for the asset's partitioning.
        """
        if self.partitioning is None:
            return
        try:
            self.partitioning.check(window)
        except PartitionError as error:
            raise PartitionError(f"Asset '{self.key}' {error}") from None

    def _span_attributes(self) -> dict[str, str]:
        """Identity attributes for spans opened below the asset's own span.

        Run id and window are omitted deliberately: the ancestor spans already carry them, and
        these are emitted from code paths that don't hold the run metadata.

        Returns:
            The asset's identity attributes.
        """
        return telemetry_attributes.from_metadata(self.event_metadata({}))
