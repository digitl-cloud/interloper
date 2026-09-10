"""Tests for ``interloper.destination.decorator``."""


import interloper as il


class FakeConnection(il.Connection):
    """Connection fixture used as a decorated destination's relation target."""

    token: str = il.SecretField(default="")


class TestDecorator:
    def test_a_field_default_is_overridden_through_the_plain_channel(self):
        @il.destination(base_path="/data/out")
        class Decorated(il.MemoryDestination):
            base_path: str = "."

        assert Decorated.model_fields["base_path"].default == "/data/out"
        assert Decorated().base_path == "/data/out"

    def test_a_component_class_is_the_relation_shorthand(self):
        @il.destination(relations={"connection": FakeConnection})
        class Decorated(il.MemoryDestination):
            """A destination-shaped fixture."""
        connection = FakeConnection()

        assert Decorated.relations["connection"].target is FakeConnection
        assert Decorated(connection=connection).connection is connection  # ty: ignore[unknown-argument, unresolved-attribute]

    def test_relations_kwarg_declares_a_relation(self):
        @il.destination(relations={"connection": il.Relation(FakeConnection)})
        class Decorated(il.MemoryDestination):
            """A destination-shaped fixture."""
        assert Decorated.relations["connection"].target is FakeConnection

    def test_a_declared_relation_binds_and_resolves(self):
        @il.destination(relations={"connection": il.Relation(FakeConnection)})
        class Decorated(il.MemoryDestination):
            """A destination-shaped fixture."""
        connection = FakeConnection()
        assert Decorated(connection=connection).connection is connection  # ty: ignore[unknown-argument, unresolved-attribute]
