"""Tests for ``interloper.component.decorator``."""

# Note: no ``from __future__ import annotations``. Decorated functions'
# parameter annotations must be real classes for relation inference.

from typing import Any

import pytest
from typing_extensions import assert_type

import interloper as il
from interloper.component import Decorator


class DecoratorConnection(il.Connection):
    """Connection fixture used as a relation target."""

    token: str = il.SecretField(default="")


class TestForms:
    def test_bare_builds_the_target(self):
        @il.asset
        def rows() -> list[dict[str, Any]]:
            return []

        assert issubclass(rows, il.Asset)

    def test_with_settings_returns_a_decorator_carrying_them(self):
        configured = il.asset(tags=["Report"])

        assert isinstance(configured, Decorator)
        assert configured.settings == {"tags": ["Report"]}

    def test_settings_accumulate(self):
        configured = il.asset(tags=["Report"])(name="Rows")

        assert configured.settings == {"tags": ["Report"], "name": "Rows"}


class TestSettings:
    def test_a_classvar_reaches_the_built_class(self):
        @il.asset(tags=["Report"], icon="carbon:data-table", name="Rows")
        def rows() -> list[dict[str, Any]]:
            return []

        assert (rows.tags, rows.icon, rows.name) == (["Report"], "carbon:data-table", "Rows")

    def test_a_maturity_reaches_the_definition(self):
        @il.destination(maturity="alpha")
        class FakeAlphaDestination(il.MemoryDestination):
            pass

        assert FakeAlphaDestination.definition().maturity is il.Maturity.ALPHA

    def test_an_invalid_classvar_raises_at_decoration(self):
        with pytest.raises(TypeError, match=r"rows\.maturity: Input should be"):

            @il.asset(maturity="gamma")
            def rows() -> list[dict[str, Any]]:
                return []

    def test_an_invalid_classvar_on_a_subclass_raises_too(self):
        with pytest.raises(TypeError, match=r"FakeTaggedDestination\.tags: Input should be a valid list"):

            @il.destination(tags="Cloud")
            class FakeTaggedDestination(il.MemoryDestination):
                pass

    def test_a_plain_class_setting_is_validated_too(self):
        with pytest.raises(TypeError, match=r"FakePlain\.tags: Input should be a valid list"):

            @il.connection(tags="Cloud")
            class FakePlain:
                host: str = "x"

    def test_a_field_default_reaches_instances(self):
        @il.asset(dataset="raw")
        def rows() -> list[dict[str, Any]]:
            return []

        assert rows.model_fields["dataset"].default == "raw"
        assert rows().dataset == "raw"

    def test_an_unknown_name_is_refused_at_decoration(self):
        with pytest.raises(TypeError, match=r"does not accept 'bogus'"):

            @il.source(bogus=False)
            def probe():
                return []

    def test_oauth_on_a_connection_subclass_names_the_kind_that_declares_it(self):
        with pytest.raises(TypeError, match=r"FakeSubclassOAuth does not accept 'oauth'; OAuthConnection declares it"):

            @il.connection(oauth=None)
            class FakeSubclassOAuth(il.Connection):
                pass

    def test_oauth_on_a_plain_connection_names_the_subclass(self):
        with pytest.raises(TypeError, match=r"OAuthConnection declares it"):

            @il.connection(oauth=None)
            class FakePlainOAuth:
                pass

    @pytest.mark.parametrize("name", ["kind", "internal_fields", "asset_types", "model_config"])
    def test_a_reserved_name_is_never_a_setting(self, name):
        with pytest.raises(TypeError, match=r"does not accept"):
            il.Source._sort_settings({name: None})

    def test_a_private_name_is_never_a_setting(self):
        with pytest.raises(TypeError, match=r"does not accept '_source_type'"):
            il.Asset._sort_settings({"_source_type": None})

    def test_a_name_that_is_both_a_classvar_and_a_field_is_refused(self):
        # `name` is a ClassVar on Component and a field here, so the value
        # would read one way on the class and another on every instance.
        with pytest.warns(UserWarning, match=r"shadows an attribute"):

            class Ambiguous(il.Config):
                """Config fixture whose own field shadows the ``name`` ClassVar."""

                name: str = "field-default"

        with pytest.raises(TypeError, match=r"Ambiguous declares 'name' both as a ClassVar and a field"):
            Ambiguous._sort_settings({"name": "Given"})

    def test_a_subclass_widens_what_is_accepted(self):
        @il.destination(base_path="/data")
        class FakeCSV(il.CSVDestination):
            pass

        assert FakeCSV().base_path == "/data"


class TestRelationChannel:
    """``relations=`` is the only relation channel."""

    def test_a_relation_is_kept_as_declared(self):
        relation = il.Relation("asset", "orders", optional=True)

        assert il.Asset._declared_relations({"upstream": relation}) == {"upstream": relation}

    def test_a_component_class_is_the_shorthand_for_a_relation_on_it(self):
        declared = il.Asset._declared_relations({"connection": DecoratorConnection})

        assert declared == {"connection": il.Relation(DecoratorConnection)}
        assert declared["connection"].target is DecoratorConnection

    def test_a_list_of_classes_narrows_the_relation(self):
        relation = il.Asset._declared_relations({"destinations": [il.MemoryDestination]})["destinations"]

        assert relation.keys == [il.MemoryDestination.key]
        assert (relation.kind, relation.many, relation.optional) == ("destination", True, True)

    def test_a_tuple_of_classes_narrows_the_same_way(self):
        declared = il.Source._declared_relations({"destinations": (il.MemoryDestination, il.CSVDestination)})

        assert declared["destinations"].keys == [il.MemoryDestination.key, il.CSVDestination.key]

    def test_a_narrowing_list_needs_a_relation_of_that_name(self):
        with pytest.raises(TypeError, match=r"Asset declares no relation named 'upstream'"):
            il.Asset._declared_relations({"upstream": [il.MemoryDestination]})

    def test_a_class_of_the_wrong_kind_is_refused(self):
        with pytest.raises(TypeError, match=r"'destinations' accepts kinds \['destination'\]"):
            il.Asset._declared_relations({"destinations": [DecoratorConnection]})

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("orders", id="a-bare-string"),
            pytest.param(3, id="a-number"),
            pytest.param([], id="an-empty-list"),
            pytest.param([il.MemoryDestination, "orders"], id="a-list-mixing-non-classes"),
        ],
    )
    def test_any_other_value_is_refused(self, value):
        with pytest.raises(TypeError, match=r"'destinations' accepts a Relation"):
            il.Asset._declared_relations({"destinations": value})

    def test_the_decorator_points_a_relation_name_at_the_channel(self):
        with pytest.raises(TypeError, match=r'relations=\{"destinations": \.\.\.\}'):

            @il.asset(destinations=[il.MemoryDestination])
            def rows() -> list[dict[str, Any]]:
                return []


class TestBuiltClass:
    def test_a_decorated_subclass_is_a_new_class_and_the_original_is_untouched(self):
        class FakeOriginal(il.MemoryDestination):
            """A destination written by hand."""

        built = il.destination(tags=["Cloud"])(FakeOriginal)

        assert built is not FakeOriginal and issubclass(built, FakeOriginal)
        assert FakeOriginal.tags == il.MemoryDestination.tags
        assert built.tags == ["Cloud"]
        identity = (built.__name__, built.key, built.kind, built.classpath(), built.__doc__)
        assert identity == (
            "FakeOriginal",
            FakeOriginal.key,
            FakeOriginal.kind,
            FakeOriginal.classpath(),
            "A destination written by hand.",
        )

    def test_relations_are_collected_once_and_win_over_annotations(self):
        @il.asset(relations={"connection": il.Relation(DecoratorConnection, optional=True)})
        def rows(connection: DecoratorConnection) -> list[dict[str, Any]]:
            return []

        assert rows.relations["connection"].optional is True

    def test_relations_narrow_through_the_channel(self):
        @il.asset(relations={"destinations": [il.MemoryDestination]})
        def rows() -> list[dict[str, Any]]:
            return []

        assert rows.relations["destinations"].keys == [il.MemoryDestination.key]

    def test_an_upstream_relation_is_declared_through_the_channel(self):
        @il.asset(relations={"upstream": il.Relation("asset", "shop.orders")})
        def rows() -> list[dict[str, Any]]:
            return []

        upstream = rows.relations["upstream"]
        assert (upstream.kind, upstream.key, upstream.name) == ("asset", "shop.orders", "upstream")


class TestTyping:
    def test_a_subclass_keeps_its_type_and_a_plain_class_is_the_anchor(self):
        @il.connection(name="Typed")
        class FakeTyped(DecoratorConnection):
            pass

        @il.connection
        class FakeUntyped:
            host: str = "x"

        # ty checks this assignment: it fails if the decorator typed FakeTyped as a plain Connection.
        typed: FakeTyped = FakeTyped(token="t")
        assert typed.token == "t"
        assert_type(FakeUntyped, type[il.Connection])
        assert_type(il.asset(tags=["x"]), Decorator[il.Asset])
