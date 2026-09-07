"""Tests for the relation primitive (``interloper.component.relation``)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

import interloper as il
from interloper.component.relation import ANY_SOURCE, ComponentIdentity, Relation


class Conn(il.Connection):
    """A connection class for shorthand tests."""


class TestComponentIdentity:
    def test_resolve_bare_key_uses_own_source(self) -> None:
        assert ComponentIdentity.resolve("orders", own_source_key="shop") == ComponentIdentity("shop", "orders")

    def test_resolve_qualified_key(self) -> None:
        assert ComponentIdentity.resolve("shop.orders", own_source_key="other") == ComponentIdentity("shop", "orders")

    def test_resolve_wildcard(self) -> None:
        resolved = ComponentIdentity.resolve("*.campaigns", own_source_key="x")
        assert resolved == ComponentIdentity(ANY_SOURCE, "campaigns")

    def test_satisfies_exact_qualified(self) -> None:
        assert ComponentIdentity("shop", "orders").satisfies("shop.orders", own_source_key=None)
        assert not ComponentIdentity("shop", "orders").satisfies("shop.items", own_source_key=None)

    def test_satisfies_wildcard_matches_any_source(self) -> None:
        assert ComponentIdentity("fb", "campaigns").satisfies("*.campaigns", own_source_key="matcher")
        assert not ComponentIdentity("fb", "ads").satisfies("*.campaigns", own_source_key="matcher")

    def test_satisfies_wildcard_requires_a_source(self) -> None:
        assert ComponentIdentity("tt", "campaigns").satisfies("*.campaigns", own_source_key="fb")
        assert not ComponentIdentity(None, "campaigns").satisfies("*.campaigns", own_source_key="fb")

    def test_satisfies_bare_key_for_non_asset(self) -> None:
        assert ComponentIdentity(None, "bigquery_destination").satisfies("bigquery_destination", own_source_key=None)

    def test_str(self) -> None:
        assert str(ComponentIdentity("shop", "orders")) == "shop.orders"
        assert str(ComponentIdentity(None, "bq")) == "bq"


class TestRelation:
    def test_class_shorthand_sets_kind_key_and_target(self) -> None:
        relation = Relation(Conn)
        assert relation.kind == "connection"
        assert relation.key == "conn"
        assert relation.target is Conn

    def test_string_form(self) -> None:
        relation = Relation("asset", "*.campaigns", many=True)
        assert relation.kinds() == ["asset"]
        assert relation.keys() == ["*.campaigns"]
        assert relation.many is True

    def test_list_kinds_and_keys(self) -> None:
        relation = Relation(["source", "asset"], ["a", "b"])
        assert relation.kinds() == ["source", "asset"]
        assert relation.keys() == ["a", "b"]

    def test_accepts_checks_kind(self) -> None:
        relation = Relation("destination")
        owner = ComponentIdentity(None, "shop")
        assert relation.accepts("destination", ComponentIdentity(None, "bq"), owner=owner)
        assert not relation.accepts("connection", ComponentIdentity(None, "bq"), owner=owner)

    def test_accepts_empty_key_takes_any_key_of_the_kind(self) -> None:
        relation = Relation("destination")
        assert relation.accepts("destination", ComponentIdentity(None, "anything"), owner=ComponentIdentity(None, "s"))

    def test_accepts_any_listed_key(self) -> None:
        relation = Relation("destination", ["bq", "gcs"])
        owner = ComponentIdentity(None, "shop")
        assert relation.accepts("destination", ComponentIdentity(None, "gcs"), owner=owner)
        assert not relation.accepts("destination", ComponentIdentity(None, "s3"), owner=owner)

    def test_accepts_bare_asset_key_means_owner_source(self) -> None:
        relation = Relation("asset", "campaigns")
        owner = ComponentIdentity("fb", "stats")
        assert relation.accepts("asset", ComponentIdentity("fb", "campaigns"), owner=owner)
        assert not relation.accepts("asset", ComponentIdentity("tt", "campaigns"), owner=owner)

    def test_self_filling_and_fallback(self) -> None:
        class Cfg(il.Config):
            threshold: int = il.InputField(default=1)

        assert Relation(Cfg).self_filling is True
        assert isinstance(Relation(Cfg).fallback(), Cfg)
        assert Relation("destination", many=True).self_filling is False

    def test_a_settings_target_always_fills_itself(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("token", raising=False)
        monkeypatch.delenv("TOKEN", raising=False)

        class Needy(il.Config):
            token: str = il.InputField()

        # A resource reads its required fields from the environment, so it is
        # the read that fails, not the build.
        assert Relation(Needy).self_filling is True
        with pytest.raises(ValidationError):
            Relation(Needy).fallback()

    def test_a_required_field_on_a_plain_component_keeps_it_unfillable(self) -> None:
        class Needy(il.Destination):
            bucket: str

            def read(self, context: il.IOContext) -> None:
                return None

            def write(self, context: il.IOContext, data: object) -> None:
                return None

        assert Relation(Needy).self_filling is False
        assert Relation(Needy).fallback() is None
        assert Relation(Needy, default=lambda: Needy(bucket="b")).self_filling is True

    def test_self_filling_false_for_target_without_model_fields(self) -> None:
        class Plain:
            def __init__(self, token: str) -> None:
                self.token = token

        relation = Relation(kind="config", target=Plain)
        assert relation.self_filling is False
        assert relation.fallback() is None

    def test_dump_excludes_target_and_default(self) -> None:
        dumped = Relation(Conn, default=Conn).model_dump(mode="json")
        assert dumped == {
            "kind": "connection",
            "key": "conn",
            "many": False,
            "optional": False,
            "on_delete": "block",
            "name": "",
        }


class TestDescriptor:
    def test_class_access_returns_the_relation_itself(self) -> None:
        """A relation is its own descriptor; ``collect()`` is what stamps its name, not the descriptor."""

        class Owner:
            conn: Conn = Relation(Conn, name="conn")

        assert isinstance(Owner.conn, Relation)
        assert Owner.conn.name == "conn"

    def test_instance_access_returns_what_is_bound(self) -> None:
        """An instance reads through to its own bindings."""
        connection = Conn()

        class Owner(il.Source):
            """Source declaring the relation as a plain attribute."""

            conn: Conn = Relation(Conn)

        assert Owner(conn=connection).conn is connection
        assert Owner().conn is None
