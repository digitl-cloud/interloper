"""Tests for ``interloper.component.decorator``."""

# Note: no ``from __future__ import annotations``. The engine is exercised
# through ``@il.asset``, whose relation inference reads the ``data()``
# parameter annotations and needs them as real classes.

from typing import Any

import pytest

import interloper as il
from interloper.component.decorator import _relations, _route
from interloper.destination.database import DatabaseDestination


class DecoratorConnection(il.Connection):
    """Connection fixture used as a relation target."""

    token: str = il.SecretField(default="")


class TestOverrideChannel:
    """Plain keyword arguments are routed by introspecting the anchor."""

    def test_a_classvar_name_is_routed_to_the_classvars(self):
        assert _route(il.Asset, {"tags": ["Report"]}) == ({"tags": ["Report"]}, {})

    def test_a_field_name_is_routed_to_the_fields(self):
        assert _route(il.Asset, {"dataset": "raw"}) == ({}, {"dataset": "raw"})

    def test_a_classvar_override_is_stamped_on_the_built_class(self):
        @il.asset(tags=["Report"], icon="carbon:data-table", name="Rows")
        def rows() -> list[dict[str, Any]]:
            return []

        assert rows.tags == ["Report"]
        assert rows.icon == "carbon:data-table"
        assert rows.name == "Rows"

    def test_a_field_override_becomes_the_field_default(self):
        @il.asset(dataset="raw")
        def rows() -> list[dict[str, Any]]:
            return []

        assert rows.model_fields["dataset"].default == "raw"
        assert rows().dataset == "raw"

    def test_an_unknown_name_raises_listing_the_accepted_names(self):
        with pytest.raises(TypeError, match=r"Asset does not accept 'nope'") as error:
            _route(il.Asset, {"nope": 1})

        message = str(error.value)
        assert "'tags'" in message
        assert "'dataset'" in message

    def test_the_decorator_reports_an_unknown_name(self):
        with pytest.raises(TypeError, match=r"does not accept 'materializable'"):

            @il.source(materializable=False)
            def probe():
                return []

    @pytest.mark.parametrize("name", ["kind", "relations", "internal_fields", "asset_types", "model_config"])
    def test_a_reserved_name_is_never_routable(self, name):
        with pytest.raises(TypeError, match=r"does not accept"):
            _route(il.Source, {name: None})

    def test_a_private_name_is_never_routable(self):
        with pytest.raises(TypeError, match=r"does not accept '_source_type'"):
            _route(il.Asset, {"_source_type": None})

    def test_the_identity_field_is_never_routable(self):
        with pytest.raises(TypeError, match=r"does not accept 'id'"):
            _route(il.Asset, {"id": "fixed"})

    def test_a_name_that_is_both_a_classvar_and_a_field_is_refused(self):
        # `name` is a ClassVar on Component and a field here, so the value
        # would read one way on the class and another on every instance.
        with pytest.warns(UserWarning, match=r"shadows an attribute"):

            class Ambiguous(il.Config):
                """Config fixture whose own field shadows the ``name`` ClassVar."""

                name: str = "field-default"

        with pytest.raises(TypeError, match=r"Ambiguous declares 'name' both as a ClassVar and a field"):
            _route(Ambiguous, {"name": "Given"})

    def test_a_relation_name_points_at_the_relation_channel(self):
        with pytest.raises(TypeError, match=r"Asset declares 'destinations' as a relation"):
            _route(il.Asset, {"destinations": [il.MemoryDestination]})

    def test_the_decorator_points_the_retired_kwarg_at_the_relation_channel(self):
        with pytest.raises(TypeError, match=r'relations=\{"destinations": \.\.\.\}'):

            @il.asset(destinations=[il.MemoryDestination])
            def rows() -> list[dict[str, Any]]:
                return []

    def test_a_decorated_subclass_widens_what_is_accepted(self):
        # A decorated subclass may carry fields and ClassVars the anchor never
        # declares, so it is the class the routing introspects.
        classvars, fields = _route(DatabaseDestination, {"read_representation": "dataframe"})

        assert classvars == {"read_representation": "dataframe"}
        assert fields == {}


class TestRelationChannel:
    """``relations=`` is the only relation channel."""

    def test_a_relation_is_kept_as_declared(self):
        relation = il.Relation("asset", "orders", optional=True)

        assert _relations(il.Asset, {"upstream": relation}) == {"upstream": relation}

    def test_a_component_class_is_the_shorthand_for_a_relation_on_it(self):
        declared = _relations(il.Asset, {"connection": DecoratorConnection})

        assert declared == {"connection": il.Relation(DecoratorConnection)}
        assert declared["connection"].target is DecoratorConnection

    def test_a_list_of_classes_narrows_the_anchors_relation(self):
        relation = _relations(il.Asset, {"destinations": [il.MemoryDestination]})["destinations"]

        assert relation.keys == [il.MemoryDestination.key]
        assert (relation.kind, relation.many, relation.optional) == ("destination", True, True)

    def test_a_tuple_of_classes_narrows_the_same_way(self):
        relation = _relations(il.Source, {"destinations": (il.MemoryDestination, il.CSVDestination)})["destinations"]

        assert relation.keys == [il.MemoryDestination.key, il.CSVDestination.key]

    def test_a_narrowing_list_needs_a_relation_of_that_name(self):
        with pytest.raises(TypeError, match=r"Asset declares no relation named 'upstream'"):
            _relations(il.Asset, {"upstream": [il.MemoryDestination]})

    def test_a_class_of_the_wrong_kind_is_refused(self):
        with pytest.raises(TypeError, match=r"'destinations' accepts kinds \['destination'\]"):
            _relations(il.Asset, {"destinations": [DecoratorConnection]})

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
            _relations(il.Asset, {"destinations": value})

    def test_the_decorator_declares_a_relation_through_the_channel(self):
        @il.asset(relations={"upstream": il.Relation("asset", "shop.orders")})
        def rows() -> list[dict[str, Any]]:
            return []

        upstream = rows.relations["upstream"]
        assert (upstream.kind, upstream.key, upstream.name) == ("asset", "shop.orders", "upstream")

    def test_the_decorator_narrows_destinations_through_the_channel(self):
        @il.asset(relations={"destinations": [il.MemoryDestination]})
        def rows() -> list[dict[str, Any]]:
            return []

        assert rows.relations["destinations"].keys == [il.MemoryDestination.key]
