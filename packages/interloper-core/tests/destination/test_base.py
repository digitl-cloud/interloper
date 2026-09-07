"""Tests for ``interloper.destination.base``."""

# Note: no ``from __future__ import annotations``: an annotation naming a
# component class declares a relation, and the collector needs it as a real
# class, not a lazy string.

from typing import Any

import pytest

import interloper as il
from interloper.destination.base import DestinationDefinition


class FakeConnection(il.Connection):
    """Connection fixture used as a destination's relation target."""

    token: str = il.SecretField(default="")

    @il.fetch_field_provider
    def datasets(self) -> list[dict[str, str]]:
        """List the datasets the credentials can reach.

        Returns:
            One entry per dataset, as the fetch field's option payload.
        """
        return [{"id": "one", "name": "One"}]

    def not_a_provider(self) -> list[dict[str, str]]:
        """Look like a provider without being marked as one.

        Returns:
            An empty option list.
        """
        return []


class FakeDestination(il.Destination):
    """Destination fixture declaring its connection as an annotation."""

    connection: FakeConnection

    def read(self, context: Any) -> Any:  # pragma: no cover
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover
        pass


class TestDefinition:
    def test_kind_and_registration(self):
        assert il.Destination.kind == "destination"
        assert il.KINDS.get("destination") is il.Destination

    def test_definition_returns_destination_definition(self):
        assert isinstance(FakeDestination.definition(), DestinationDefinition)

    def test_anchor_declares_no_relation(self):
        assert il.Destination.relations == {}

    def test_annotation_declares_a_relation(self):
        relation = FakeDestination.relations["connection"]
        assert (relation.kind, relation.key, relation.target) == ("connection", "fake_connection", FakeConnection)

    def test_relations_reach_the_definition(self):
        assert FakeDestination.definition().relations["connection"].target is FakeConnection

    def test_a_relation_is_not_a_config_field(self):
        assert "connection" not in FakeDestination.definition().config_schema.get("properties", {})


class TestFetchProviderValidation:
    """``FetchField(provider=...)`` resolves through the declared relations."""

    def test_provider_on_a_declared_relation_is_accepted(self):
        class Valid(il.Destination):
            connection: FakeConnection
            dataset: str = il.FetchField(provider="connection.datasets", value_key="id")

            def read(self, context: Any) -> Any:  # pragma: no cover
                return None

            def write(self, context: Any, data: Any) -> None:  # pragma: no cover
                pass

        fetch = Valid.definition().config_schema["properties"]["dataset"]["x-fetch"]
        assert fetch["provider"] == "connection.datasets"

    def test_provider_on_an_undeclared_relation_is_rejected(self):
        class Undeclared(il.Destination):
            connection: FakeConnection
            dataset: str = il.FetchField(provider="other.datasets")

            def read(self, context: Any) -> Any:  # pragma: no cover
                return None

            def write(self, context: Any, data: Any) -> None:  # pragma: no cover
                pass

        with pytest.raises(TypeError, match="not declared"):
            Undeclared.definition()

    def test_provider_naming_an_unmarked_method_is_rejected(self):
        class Unmarked(il.Destination):
            connection: FakeConnection
            dataset: str = il.FetchField(provider="connection.not_a_provider")

            def read(self, context: Any) -> Any:  # pragma: no cover
                return None

            def write(self, context: Any, data: Any) -> None:  # pragma: no cover
                pass

        with pytest.raises(TypeError, match="not a @fetch_field_provider"):
            Unmarked.definition()
