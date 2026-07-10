"""Asset: the core data-producing component of the interloper framework."""

from __future__ import annotations

import asyncio
import inspect
import traceback
import warnings
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import Field, PrivateAttr, field_validator
from typing_extensions import Self

from interloper.asset.context import ExecutionContext
from interloper.component import Component, ComponentDefinition, RelationDefinition, RelationSlot
from interloper.conformer import Conformer
from interloper.destination import Destination, IOContext
from interloper.errors import AssetError, NormalizerError, PartitionError
from interloper.events import EventBus, EventType
from interloper.normalizer import MaterializationStrategy, Normalizer
from interloper.partitioning import Partition, PartitionConfig, PartitionWindow
from interloper.representation import Representation
from interloper.resource import Resource
from interloper.schema import Schema
from interloper.utils import concurrency
from interloper.utils.concurrency import invoke
from interloper.utils.data import is_empty
from interloper.utils.imports import get_object_path
from interloper.utils.text import to_label

if TYPE_CHECKING:
    from interloper.dag import DAG
    from interloper.source import Source

_UNSET = object()


warnings.filterwarnings("ignore", message='Field name "schema" in "AssetDefinition"')


class AssetDefinition(ComponentDefinition):
    """Definition of an asset including its resource types and tags.

    Cross-entity references use keys (not inlined schemas):
    - ``resource_types`` maps resource name → component key
    - ``destination_types`` lists destination component keys
    - ``requires`` maps param name → asset key (bare or qualified)

    Same-entity data is inlined:
    - ``asset_schema`` is the asset's own output schema
    - ``partitioning`` is the asset's own partition config

    Asset keys come in two forms:

    - **Bare key** — ``"campaigns"`` — scoped to the parent source.
      Used for intra-source dependencies.
    - **Qualified key** — ``"facebook_ads.campaigns"`` — globally unique.
      Used for cross-source dependencies in ``requires`` / ``optional_requires``.

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
        if self.source_key:
            return f"{self.source_key}.{self.key}"
        return self.key


class Asset(Component):
    """A data-producing component.

    Subclass and implement ``data()`` to define an asset::

        class Users(Asset):
            resource_types = {"config": MyConfig}

            def data(self, **kwargs: Any) -> Any:
                return fetch_users()

    Or use the ``@asset`` decorator for a functional style::

        @asset(resources={"config": MyConfig})
        def users(**kwargs: Any) -> Any:
            return fetch_users()
    """

    # Definition
    destination_types: ClassVar[list[type[Destination]]] = []
    schema: ClassVar[type[Schema] | None] = None
    partitioning: ClassVar[PartitionConfig | None] = None
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "resource": RelationDefinition(kinds=["connection", "config", "resource"], field="resources", slotted=True),
        "destination": RelationDefinition(kinds=["destination"], field="destinations"),
        "dependency": RelationDefinition(kinds=["asset"], field="dependencies", slotted=True, inline=False),
    }
    internal_fields: ClassVar[frozenset[str]] = frozenset(
        {"destinations", "normalizer", "materialization_strategy", "dependencies"}
    )
    requires: ClassVar[dict[str, str]] = {}
    optional_requires: ClassVar[dict[str, str]] = {}
    tags: ClassVar[list[str]] = []
    runnable: ClassVar[bool] = True

    _source_type: ClassVar[type[Source] | None] = None

    # State
    destinations: list[Destination] = Field(default_factory=list)
    dataset: str = Field(default="")
    default_destination_key: str = Field(default="")
    materializable: bool = Field(default=True)
    materialization_strategy: MaterializationStrategy = Field(default=MaterializationStrategy.AUTO)
    normalizer: Normalizer | None = Field(default=None)
    dependencies: dict[str, str] = Field(default_factory=dict)

    # Private
    _source: Source | None = PrivateAttr(default=None)
    # Effective schema of the last conform: the declared schema, or the one
    # inferred from the data. Carried to destinations via IOContext.schema.
    _effective_schema: type[Schema] | None = PrivateAttr(default=None)

    @field_validator("destinations", mode="before")
    @classmethod
    def _validate_destinations(cls, value: Any) -> Any:
        """Accept a single destination or ``None`` where a list is expected.

        Returns:
            The value as a list.
        """
        if value is None:
            return []
        return value if isinstance(value, (list, tuple)) else [value]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Infer ``resource_types`` from ``data()`` type annotations."""
        super().__init_subclass__(**kwargs)
        cls._infer_resource_types()

    @classmethod
    def _infer_resource_types(cls) -> None:
        """Populate ``resource_types`` from ``data()`` annotations.

        Uses ``inspect.signature`` (which respects ``__signature__``
        overrides set by the ``@asset`` decorator) to read parameter
        annotations.  Any parameter annotated with a ``Resource``
        subclass that isn't already explicitly declared is added.
        Explicit declarations always take precedence.
        """
        if "data" not in cls.__dict__:
            return
        explicit: dict[str, type[Resource]] = cls.__dict__.get("resource_types", {})
        try:
            sig = inspect.signature(cls.data)
        except (TypeError, ValueError):
            return
        inferred: dict[str, type[Resource]] = {}
        for param_name, param in sig.parameters.items():
            if param_name in ("self", "context", "source", "kwargs"):
                continue
            if param_name in explicit:
                continue
            hint = param.annotation
            if hint is inspect.Parameter.empty:
                continue
            if isinstance(hint, type) and issubclass(hint, Resource):
                inferred[param_name] = hint
        if inferred:
            cls.resource_types = {**explicit, **inferred}

    # -- Identity & definition -------------------------------------------------

    @property
    def source(self) -> Source | None:
        """The source this asset belongs to, if any."""
        return self._source

    def effective_partition(
        self, partition_or_window: Partition | PartitionWindow | None
    ) -> Partition | PartitionWindow | None:
        """Return the partition scope this asset actually consumes.

        Unpartitioned assets ignore any requested scope.

        Returns:
            The scope unchanged for partitioned assets, ``None`` otherwise.
        """
        return partition_or_window if self.partitioning is not None else None

    @property
    def qualified_key(self) -> str:
        """The fully qualified asset key: ``source_key.asset_key``."""
        if self._source is not None:
            return f"{self._source.key}.{self.key}"
        return self.key

    @classmethod
    def classpath(cls) -> str:
        """Fully qualified import path for this asset class.

        Source-owned assets return the composite form
        ``"module:SourceName.AssetName"``, where the colon explicitly
        marks the module / attribute boundary.  Resolution walks the
        attribute chain at class level via the ``AssetRef`` descriptor
        installed on the parent source — no instantiation required.

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
            relations=cls.relation_definitions(),
            asset_schema=schema_dict,
            partitioning=partitioning_dict,
        )

    @classmethod
    def relation_definitions(cls) -> dict[str, RelationDefinition]:
        """Enrich the vocabulary with dependency slots and destination keys.

        Dependency slots come from the class's ``requires`` /
        ``optional_requires`` contracts (slot key is the — possibly
        qualified — upstream asset key).

        Returns:
            Relation type → enriched definition.
        """
        relations = super().relation_definitions()
        if "dependency" in relations:
            slots = {param: RelationSlot(key=key) for param, key in cls.requires.items()}
            slots |= {param: RelationSlot(key=key, required=False) for param, key in cls.optional_requires.items()}
            relations["dependency"] = relations["dependency"].model_copy(update={"slots": slots})
        if "destination" in relations:
            relations["destination"] = relations["destination"].model_copy(
                update={"keys": [dest_cls.key for dest_cls in cls.destination_types]}
            )
        return relations

    # -- Reconfiguration -------------------------------------------------------

    def __call__(
        self,
        *,
        id: str | None = None,
        resources: dict[str, Resource] | None = None,
        destinations: Destination | list[Destination] | None = None,
        dataset: str | None = None,
        default_destination_key: str | None = None,
        materializable: bool | None = None,
        materialization_strategy: MaterializationStrategy | None = None,
        normalizer: Normalizer | None = _UNSET,  # ty: ignore[invalid-parameter-default]
        dependencies: dict[str, str] | None = None,
    ) -> Self:
        """Return a reconfigured copy of this asset."""
        overrides: dict[str, Any] = {}
        if id is not None:
            overrides["id"] = id
        if resources is not None:
            overrides["resources"] = {**self.resources, **resources}
        if destinations is not None:
            overrides["destinations"] = destinations if isinstance(destinations, list) else [destinations]
        if dataset is not None:
            overrides["dataset"] = dataset
        if default_destination_key is not None:
            overrides["default_destination_key"] = default_destination_key
        if materializable is not None:
            overrides["materializable"] = materializable
        if materialization_strategy is not None:
            overrides["materialization_strategy"] = materialization_strategy
        if normalizer is not _UNSET:
            overrides["normalizer"] = normalizer
        if dependencies is not None:
            overrides["dependencies"] = dependencies
        return self.model_copy(update=overrides)

    # -- Execution -------------------------------------------------------------

    def run(
        self,
        partition_or_window: Partition | PartitionWindow | None = None,
        dag: DAG | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Execute the asset and return the result without writing to destination.

        Sync entrypoint for scripts, REPLs, and notebooks — drives
        :meth:`run_async` to completion on the bridge loop
        (see :func:`interloper.run`)::

            data = asset.run()

        Async code awaits :meth:`run_async` instead.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has dependencies).
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

        Resolves context, resources, and upstream dependencies (via DAG), then
        runs the data function.  Sync ``data()`` functions are automatically
        offloaded to a thread via ``asyncio.to_thread``; async ``data()``
        functions are awaited natively.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has dependencies).
            metadata: Arbitrary metadata dict (e.g. run_id, backfill_id).

        Returns:
            The raw execution result.
        """
        self._validate_partitioning(partition_or_window)

        context = ExecutionContext(
            asset_key=type(self).key,
            partition_or_window=partition_or_window,
            partitioning=self.partitioning,
            metadata=metadata,
            asset_id=self.id,
            source_id=self._source.id if self._source is not None else None,
        )

        kwargs = await self._build_kwargs(context, partition_or_window, dag)

        exec_meta = self._event_metadata(metadata or {}, partition_or_window)
        EventBus.emit(
            EventType.ASSET_EXEC_STARTED,
            metadata={**exec_meta, "message": f"Executing '{type(self).key}'"},
        )
        try:
            result = await invoke(self.data, **kwargs)
            EventBus.emit(
                EventType.ASSET_EXEC_COMPLETED,
                metadata={**exec_meta, "message": f"Executed '{type(self).key}'"},
            )
        except Exception as e:
            EventBus.emit(
                EventType.ASSET_EXEC_FAILED,
                metadata={
                    **exec_meta,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "message": f"Execution of '{type(self).key}' failed: {e}",
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

        Sync entrypoint for scripts, REPLs, and notebooks — drives
        :meth:`materialize_async` to completion on the bridge loop
        (see :func:`interloper.run`)::

            asset.materialize()

        Async code awaits :meth:`materialize_async` instead.

        Args:
            partition_or_window: Partition or PartitionWindow for this run.
            dag: DAG for dependency resolution (required if asset has dependencies).
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
            dag: DAG for dependency resolution (required if asset has dependencies).
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

        Raises:
            NotImplementedError: If the subclass does not implement ``data()``.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement data()")

    def partition_row_counts(self) -> dict[str, int]:
        """Return row counts grouped by this asset's partition column.

        Delegates to :meth:`Destination.partition_row_counts` using the first
        resolved destination.

        Returns:
            Mapping from partition value (as string) to row count.

        Raises:
            PartitionError: If this asset is not partitioned.
            AssetError: If no destinations are configured.
        """
        if self.partitioning is None:
            raise PartitionError(
                f"Asset '{self.key}' is not partitioned. "
                "Cannot compute partition row counts without a partition column."
            )

        dests = self._resolve_destinations()
        if not dests:
            raise AssetError(f"No destinations found for asset '{self.key}'")

        context = IOContext(asset=self)
        return dests[0].partition_row_counts(context)

    # -- Internals -------------------------------------------------------------
    async def _build_kwargs(
        self,
        context: ExecutionContext,
        partition_or_window: Partition | PartitionWindow | None,
        dag: DAG | None,
    ) -> dict[str, Any]:
        """Build kwargs for the data function.

        Maps function parameters to their values: ``context`` is injected
        directly, declared resources are resolved by name, and all other
        parameters are treated as upstream dependencies loaded from
        destination via the DAG.

        Returns:
            Keyword arguments to pass to ``data()``.

        Raises:
            AssetError: If a dependency cannot be resolved or read.
        """
        kwargs: dict[str, Any] = {}
        sig = inspect.signature(self.data)
        optional_names = set(type(self).optional_requires)

        for param_name in sig.parameters:
            if param_name in ("self", "source", "kwargs"):
                continue
            if param_name == "context":
                kwargs["context"] = context
            elif param_name in type(self).resource_types:
                kwargs[param_name] = self._resolve_resource(param_name)
            else:
                if param_name not in self.dependencies:
                    continue
                if dag is None:
                    if param_name in optional_names:
                        kwargs[param_name] = None
                        continue
                    raise AssetError(
                        f"Asset '{self.key}' has dependencies but no DAG provided. "
                        "Pass a DAG to run() or materialize() for dependency resolution."
                    )

                upstream_id = self.dependencies[param_name]
                upstream_asset = dag.asset_map[upstream_id]
                if param_name in optional_names:
                    try:
                        kwargs[param_name] = await self._destination_read(
                            upstream_asset, partition_or_window, context.metadata
                        )
                    except (AssetError, Exception):  # noqa: BLE001
                        kwargs[param_name] = None
                else:
                    kwargs[param_name] = await self._destination_read(
                        upstream_asset, partition_or_window, context.metadata
                    )

        return kwargs

    async def _destination_write(
        self,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
        result: Any,
    ) -> None:
        """Write the execution result to all configured destinations."""
        dests = self._resolve_destinations()
        if not dests:
            return

        if is_empty(result):
            EventBus.emit(
                EventType.LOG,
                metadata={
                    **self._event_metadata(metadata, partition_or_window),
                    "level": "WARNING",
                    "message": (
                        f"Asset '{type(self).key}' produced no data; skipping write to {len(dests)} destination(s)"
                    ),
                },
            )
            return

        dest_context = IOContext(
            asset=self,
            partition_or_window=self.effective_partition(partition_or_window),
            metadata=metadata,
            schema=self._effective_schema or self.schema,
        )

        for dest in dests:
            dest_key = type(dest).key
            dest_meta = self._event_metadata(metadata, partition_or_window)
            dest_meta["destination_key"] = dest_key
            EventBus.emit(
                EventType.DEST_WRITE_STARTED,
                metadata={**dest_meta, "message": f"Writing '{type(self).key}'"},
            )
            try:
                await invoke(dest.write, dest_context, result)
                EventBus.emit(
                    EventType.DEST_WRITE_COMPLETED,
                    metadata={**dest_meta, "message": f"Wrote '{type(self).key}'"},
                )
            except Exception as e:
                EventBus.emit(
                    EventType.DEST_WRITE_FAILED,
                    metadata={
                        **dest_meta,
                        "error": str(e),
                        "traceback": traceback.format_exc(),
                        "message": f"Failed to write '{type(self).key}': {e}",
                    },
                )
                raise

    async def _destination_read(
        self,
        upstream_asset: Asset,
        partition_or_window: Partition | PartitionWindow | None,
        metadata: dict[str, Any],
    ) -> Any:
        """Read data from an upstream asset's first destination.

        Returns:
            The data read from the upstream asset's destination.

        Raises:
            AssetError: If no destination is found for the upstream asset.
        """
        dests = upstream_asset._resolve_destinations()
        if not dests:
            raise AssetError(f"No destination found for upstream asset '{upstream_asset.key}'")
        dest = dests[0]

        effective_partition = upstream_asset.effective_partition(partition_or_window)
        dest_context = IOContext(
            asset=upstream_asset,
            partition_or_window=effective_partition,
            metadata=metadata,
            schema=upstream_asset.schema,
        )

        dest_meta = self._event_metadata(metadata, effective_partition)
        EventBus.emit(
            EventType.DEST_READ_STARTED,
            metadata={**dest_meta, "message": f"Reading '{type(upstream_asset).key}'"},
        )
        try:
            result = await invoke(dest.read, dest_context)
            EventBus.emit(
                EventType.DEST_READ_COMPLETED,
                metadata={**dest_meta, "message": f"Read '{type(upstream_asset).key}'"},
            )
        except Exception as e:
            EventBus.emit(
                EventType.DEST_READ_FAILED,
                metadata={
                    **dest_meta,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "message": f"Failed to read '{type(upstream_asset).key}': {e}",
                },
            )
            raise AssetError(f"Failed to load data from upstream asset '{upstream_asset.key}': {e}") from e

        return result

    def _normalize_and_conform(self, result: Any) -> Any:
        """Apply optional normalization, then always conform to the schema.

        Normalization (when a normalizer is configured) reshapes the data:
        flattening, column renaming, missing-key fill.  Conform then enforces
        the declared schema according to the materialization strategy — it
        runs whether or not a normalizer is configured, so a declared schema
        is always a checked contract.

        Returns:
            The normalized and conformed result.
        """
        if self.normalizer is not None:
            result = self.normalizer.normalize(result)
        return self._conform(result)

    def _conform(self, result: Any) -> Any:
        """Enforce the asset's schema according to the materialization strategy.

        AUTO: validate when a schema is declared, infer one otherwise.
        STRICT: schema required; reject extra, missing, or mistyped fields.
        RECONCILE: schema required; align columns and coerce values.

        The schema operations come from a single :class:`Conformer`, resolved
        once from the data's representation (rows or DataFrame). Tabular data
        is canonicalized on the way in (dict / model / generator →
        ``list[dict]``); non-tabular data without a schema passes through
        untouched. The effective schema (declared, or inferred under AUTO) is
        carried to destinations via ``IOContext.schema``.

        Returns:
            The conformed result.

        Raises:
            AssetError: If the strategy requires a schema but none is declared,
                or if a schema is declared but the data is not tabular.
        """
        strategy = self.materialization_strategy
        schema = self.schema

        if schema is None and strategy != MaterializationStrategy.AUTO:
            raise AssetError(f"Asset '{type(self).key}': strategy='{strategy.value}' requires a schema.")

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
                f"Asset '{type(self).key}' declares a schema but returned data that cannot be checked against it: {e}"
            ) from e

        if schema is None:
            self._effective_schema = self._infer_schema(conformer, result)
            return result

        self._effective_schema = schema
        if strategy == MaterializationStrategy.RECONCILE:
            return conformer.reconcile(result, schema)
        conformer.validate(result, schema, strict=strategy == MaterializationStrategy.STRICT)
        return result

    def _infer_schema(self, conformer: Conformer, result: Any) -> type[Schema] | None:
        """Best-effort schema inference for the IO boundary (AUTO, no declared schema).

        Inference is metadata for destinations (DDL, typed loads) — it must
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
        except Exception:  # noqa: BLE001 — inference is best-effort metadata
            return None

    def _validate_partitioning(
        self,
        partition_or_window: Partition | PartitionWindow | None,
    ) -> None:
        """Validate partitioning constraints before execution.

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

    def _validate_destination(self, dest: Destination) -> None:
        """Validate that a destination is compatible with this asset's destination_types.

        Raises:
            DestinationError: If the destination type is not in destination_types.
        """
        allowed = type(self).destination_types
        if not allowed:
            return
        if not isinstance(dest, tuple(allowed)):
            from interloper.errors import DestinationError

            allowed_names = ", ".join(t.__name__ for t in allowed)
            raise DestinationError(
                f"Destination '{type(dest).__name__}' is not compatible with "
                f"asset '{self.key}'. Allowed types: [{allowed_names}]"
            )

    def _resolve_resource(self, name: str) -> Resource | None:
        """Resolve a named resource instance for this asset.

        Resolution order:
        1. Asset's own ``resources[name]``.
        2. Source's ``resources[name]`` (if asset belongs to a source).
        3. Source's resource matching by type (if asset belongs to a source).
        4. Auto-instantiate from ``resource_types[name]``.
        5. None.

        Args:
            name: The resource name to resolve.

        Returns:
            A resource instance or ``None``.

        Raises:
            AssetError: If the resolved resource does not match the declared type.
        """
        res_type = type(self).resource_types.get(name)

        resolved: Resource | None = None

        # 1. Asset's own instance
        if name in self.resources:
            resolved = self.resources[name]

        # 2–3. Source resources (by name, then by type)
        elif self._source is not None:
            source_res = self._source.resources.get(name)
            if source_res is not None:
                resolved = source_res
            elif res_type is not None:
                for sr in self._source.resources.values():
                    if isinstance(sr, res_type):
                        resolved = sr

        # 4. Auto-instantiate
        if resolved is None and res_type is not None:
            resolved = res_type()

        # Validate against declared resource type
        if resolved is not None and res_type is not None and not isinstance(resolved, res_type):
            raise AssetError(
                f"Resource '{name}' on asset '{type(self).key}' expected type "
                f"'{res_type.__name__}', got '{type(resolved).__name__}'."
            )

        return resolved

    def _resolve_destinations(self) -> list[Destination]:
        """Resolve and validate the destination list for this asset.

        Resolution order:
        1. Asset's own destinations.
        2. Source's destinations (if asset belongs to a source).
        3. Empty list.

        Returns:
            A list of validated destination instances (may be empty).
        """
        dests = self.destinations
        if not dests and self._source is not None:
            dests = self._source.destinations
        for dest in dests:
            self._validate_destination(dest)
        return dests

    def _event_metadata(
        self,
        metadata: dict[str, Any],
        partition_or_window: Partition | PartitionWindow | None = None,
    ) -> dict[str, Any]:
        """Build the base event metadata dict for this asset.

        Merges run-level metadata with asset identity fields.

        Args:
            metadata: Run-level metadata (e.g. run_id, backfill_id).
            partition_or_window: Current partition scope.

        Returns:
            The merged metadata dict.
        """
        base: dict[str, Any] = {
            **metadata,
            "asset_id": self.id,
            "asset_key": self.key,
            "asset_qualified_key": self.qualified_key,
            "partition_or_window": str(partition_or_window) if partition_or_window else None,
        }
        if self._source is not None:
            base["source_id"] = self._source.id
        return base
