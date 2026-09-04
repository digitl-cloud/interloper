"""Tests for ``interloper.asset.decorator``."""

# Note: no ``from __future__ import annotations`` — ``Asset._infer_resource_types``
# reads parameter annotations via ``inspect.signature`` and needs them as real
# classes (not lazily-evaluated strings).

import datetime as dt
from typing import Any

import interloper as il
from interloper.normalizer import MaterializationStrategy, Normalizer


class DecoratorResource(il.Resource):
    """Resource fixture referenced from a decorated function's annotations."""

    value: str = ""


class DecoratorDestination(il.Destination):
    """Destination fixture declared through the decorator."""

    def read(self, context: Any) -> Any:  # pragma: no cover - not exercised
        """Unused.

        Args:
            context: Ignored IO context.

        Returns:
            Nothing.
        """
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover - not exercised
        """Unused.

        Args:
            context: Ignored IO context.
            data: Ignored payload.
        """


class DecoratorSchema(il.Schema):
    value: int | None = None


class TestBareForm:
    """``@asset`` applied directly to a function."""

    def test_the_function_becomes_the_assets_data(self):
        @il.asset
        def users() -> list[dict[str, Any]]:
            return [{"id": 1}]

        assert issubclass(users, il.Asset)
        assert users().data() == [{"id": 1}]

    def test_the_key_and_module_come_from_the_function(self):
        @il.asset
        def page_stats() -> list[dict[str, Any]]:
            return []

        assert page_stats.key == "page_stats"
        assert page_stats.__module__ == __name__

    def test_the_docstring_is_carried_over(self):
        # Asset docstrings ship as the materialized table's description.
        @il.asset
        def documented() -> list[dict[str, Any]]:
            """One row per widget.

            Returns:
                No rows.
            """
            return []

        assert documented.__doc__ is not None
        assert documented.__doc__.startswith("One row per widget.")

    def test_an_undocumented_function_leaves_the_class_undocumented(self):
        @il.asset
        def undocumented() -> list[dict[str, Any]]:
            return []

        assert undocumented.__doc__ is None


class TestParameterizedForm:
    """``@asset(...)`` with declarations."""

    def test_every_classvar_declaration_is_applied(self):
        @il.asset(
            destinations=[DecoratorDestination],
            schema=DecoratorSchema,
            partitioning=il.TimePartitionConfig(column="date"),
            tags=["Report"],
            key="custom",
            name="Custom Asset",
            icon="carbon:data-table",
            resources={"config": DecoratorResource},
            requires={"upstream": "other"},
            optional_requires={"maybe": "another"},
        )
        def declared(context: il.ExecutionContext, config: DecoratorResource) -> list[dict[str, Any]]:
            return []

        assert declared.destination_types == [DecoratorDestination]
        assert declared.schema is DecoratorSchema
        assert declared.partitioning is not None
        assert declared.partitioning.column == "date"
        assert declared.tags == ["Report"]
        assert declared.key == "custom"
        assert declared.name == "Custom Asset"
        assert declared.icon == "carbon:data-table"
        assert declared.resource_types == {"config": DecoratorResource}
        assert declared.requires == {"upstream": "other"}
        assert declared.optional_requires == {"maybe": "another"}

    def test_field_declarations_become_real_field_defaults(self):
        normalizer = Normalizer()

        @il.asset(materialization_strategy=MaterializationStrategy.STRICT, normalizer=normalizer)
        def declared() -> list[dict[str, Any]]:
            return []

        instance = declared()
        assert instance.materialization_strategy is MaterializationStrategy.STRICT
        # Pydantic deep-copies a mutable field default per instance.
        assert instance.normalizer == normalizer

    def test_no_declarations_leaves_the_defaults(self):
        @il.asset()
        def plain() -> list[dict[str, Any]]:
            return []

        instance = plain()
        assert instance.materialization_strategy is MaterializationStrategy.AUTO
        assert instance.normalizer is None
        assert plain.schema is None
        assert plain.partitioning is None


class TestResourceInference:
    """Resource types are read off the ``data()`` annotations."""

    def test_an_annotated_parameter_is_inferred(self):
        @il.asset
        def uses_resource(config: DecoratorResource) -> list[dict[str, Any]]:
            return []

        assert uses_resource.resource_types == {"config": DecoratorResource}

    def test_context_and_kwargs_are_not_resources(self):
        @il.asset
        def uses_context(context: il.ExecutionContext, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        assert uses_context.resource_types == {}


class TestSignatureShapes:
    """Standalone functions and source methods both back a ``data()``."""

    async def test_a_standalone_function_gains_a_self_parameter(self):
        @il.asset
        def standalone(context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [{"asset": context.asset_key}]

        import inspect

        assert list(inspect.signature(standalone.data).parameters) == ["self", "context"]
        assert await standalone(id="standalone").run_async() == [{"asset": "standalone"}]

    async def test_an_async_standalone_function_is_awaited(self):
        @il.asset
        async def standalone() -> list[dict[str, Any]]:
            return [{"a": 1}]

        assert await standalone(id="standalone").run_async() == [{"a": 1}]

    async def test_a_method_asset_receives_its_source(self):
        @il.source
        class WithMethodAsset(il.Source):
            """Source whose asset reads an input field off ``self``."""

            greeting: str = il.InputField(default="hello")

            @il.asset
            def rows(self) -> list[dict[str, Any]]:
                """One row carrying the source's greeting.

                Returns:
                    The single greeting row.
                """
                return [{"greeting": self.greeting}]

        source = WithMethodAsset(greeting="hi")  # ty: ignore[unknown-argument]

        assert await source.rows.run_async() == [{"greeting": "hi"}]

    async def test_an_async_method_asset_receives_its_source(self):
        @il.source
        class WithAsyncMethodAsset(il.Source):
            """Source whose async asset reads an input field off ``self``."""

            greeting: str = il.InputField(default="hello")

            @il.asset
            async def rows(self) -> list[dict[str, Any]]:
                """One row carrying the source's greeting.

                Returns:
                    The single greeting row.
                """
                return [{"greeting": self.greeting}]

        source = WithAsyncMethodAsset(greeting="hi")  # ty: ignore[unknown-argument]

        assert await source.rows.run_async() == [{"greeting": "hi"}]


class TestPartitionedAsset:
    """A decorated function reaches its partition through the context."""

    async def test_the_partition_date_is_available(self):
        @il.asset(partitioning=il.TimePartitionConfig(column="date"))
        def daily(context: il.ExecutionContext) -> list[dict[str, Any]]:
            return [{"date": context.partition_date}]

        rows = await daily(id="daily").run_async(il.TimePartition(dt.date(2026, 6, 1)))

        assert rows == [{"date": dt.date(2026, 6, 1)}]
