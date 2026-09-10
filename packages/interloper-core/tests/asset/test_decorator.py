"""Tests for ``interloper.asset.decorator``."""

# Note: no ``from __future__ import annotations``. ``Asset._collect`` reads the
# ``data()`` parameter annotations to infer relations and needs them as real
# classes (not lazily-evaluated strings).

import datetime as dt
import inspect
from typing import Any

import interloper as il
from interloper.normalizer import MaterializationStrategy, Normalizer


class DecoratorConfig(il.Config):
    """Config fixture referenced from a decorated function's annotations."""

    value: str = ""


class DecoratorDestination(il.MemoryDestination):
    """Destination fixture declared through the decorator."""


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

    def test_data_wraps_the_function(self):
        def rows(context: il.ExecutionContext, config: DecoratorConfig) -> list[dict[str, Any]]:
            return []

        cls = il.asset(rows)

        assert inspect.unwrap(cls.data) is rows
        assert cls.data.__name__ == "rows"
        assert [*inspect.signature(cls.data).parameters] == ["self", "context", "config"]


class TestParameterizedForm:
    """``@asset(...)`` with declarations."""

    def test_every_classvar_declaration_is_applied(self):
        @il.asset(
            schema=DecoratorSchema,
            partitioning=il.TimePartitionConfig(column="date"),
            tags=["Report"],
            key="custom",
            name="Custom Asset",
            icon="carbon:data-table",
            relations={
                "destinations": [DecoratorDestination],
                "upstream": il.Relation("asset", "other"),
                "maybe": il.Relation("asset", "another", optional=True),
            },
        )
        def declared(context: il.ExecutionContext, config: DecoratorConfig) -> list[dict[str, Any]]:
            return []

        assert declared.relations["destinations"].keys == [DecoratorDestination.key]
        assert declared.schema is DecoratorSchema
        assert declared.partitioning is not None
        assert declared.partitioning.column == "date"
        assert declared.tags == ["Report"]
        assert declared.key == "custom"
        assert declared.name == "Custom Asset"
        assert declared.icon == "carbon:data-table"
        assert declared.relations["config"].key == DecoratorConfig.key
        upstream, maybe = declared.relations["upstream"], declared.relations["maybe"]
        assert (upstream.kind, upstream.key, upstream.name, upstream.optional) == ("asset", "other", "upstream", False)
        assert (maybe.kind, maybe.key, maybe.name, maybe.optional) == ("asset", "another", "maybe", True)

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
        assert instance.materialization_strategy is MaterializationStrategy.RECONCILE
        assert instance.normalizer is None
        assert plain.schema is None
        assert plain.partitioning is None


class TestRelationInference:
    """Relations are read off the ``data()`` annotations."""

    def test_an_annotated_parameter_is_inferred(self):
        @il.asset
        def uses_config(config: DecoratorConfig) -> list[dict[str, Any]]:
            return []

        assert uses_config.relations["config"].key == DecoratorConfig.key
        assert uses_config.relations["config"].kind == "config"

    def test_context_and_kwargs_declare_nothing(self):
        @il.asset
        def uses_context(context: il.ExecutionContext, **kwargs: Any) -> list[dict[str, Any]]:
            return []

        assert set(uses_context.relations) == {"destinations"}

    def test_an_explicit_relation_wins_over_the_annotation(self):
        @il.asset(relations={"config": il.Relation(DecoratorConfig, optional=True)})
        def uses_config(config: DecoratorConfig) -> list[dict[str, Any]]:
            return []

        assert uses_config.relations["config"].optional is True

    def test_a_list_of_destination_classes_narrows_the_relation_keys(self):
        @il.asset(relations={"destinations": [DecoratorDestination]})
        def narrowed() -> list[dict[str, Any]]:
            return []

        relation = narrowed.relations["destinations"]
        assert relation.keys == [DecoratorDestination.key]
        assert (relation.many, relation.optional) == (True, True)


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
