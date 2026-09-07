"""Tests for ``interloper.destination.decorator``."""

from typing import Any

import interloper as il


class FakeConnection(il.Connection):
    """Connection fixture used as a decorated destination's relation target."""

    token: str = il.SecretField(default="")


class TestDecorator:
    def test_relations_kwarg_declares_a_relation(self):
        @il.destination(relations={"connection": il.Relation(FakeConnection)})
        class Decorated(il.Destination):
            def read(self, context: Any) -> Any:  # pragma: no cover
                return None

            def write(self, context: Any, data: Any) -> None:  # pragma: no cover
                pass

        assert Decorated.relations["connection"].target is FakeConnection

    def test_a_declared_relation_binds_and_resolves(self):
        @il.destination(relations={"connection": il.Relation(FakeConnection)})
        class Decorated(il.Destination):
            def read(self, context: Any) -> Any:  # pragma: no cover
                return None

            def write(self, context: Any, data: Any) -> None:  # pragma: no cover
                pass

        connection = FakeConnection()
        assert Decorated(connection=connection).connection is connection  # ty: ignore[unknown-argument, unresolved-attribute]
