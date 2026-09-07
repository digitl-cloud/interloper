"""Tests for ``interloper.resource.fields`` — FetchField + fetch_field_provider."""

from __future__ import annotations

import pytest

import interloper as il
from interloper.resource.fields import is_fetch_field_provider


class Conn(il.Connection):
    token: str = il.SecretField(default="")

    @il.fetch_field_provider
    def things(self) -> list[dict[str, str]]:
        return [{"id": "1", "name": "one"}]

    def not_a_provider(self) -> list[dict[str, str]]:
        return []


class TestFetchProvider:
    def test_marker_detected(self):
        assert is_fetch_field_provider(Conn.things)
        assert not is_fetch_field_provider(Conn.not_a_provider)


class TestPresentationKwargs:
    """``label`` / ``info`` presentation kwargs accepted by every factory."""

    def test_label_becomes_schema_title(self):
        class FakeLabeledSource(il.Source):
            account_id: str = il.InputField(default="", label="Account ID")

        prop = FakeLabeledSource.definition().config_schema["properties"]["account_id"]
        assert prop["title"] == "Account ID"

    def test_info_becomes_x_info(self):
        class FakeInfoSource(il.Source):
            account_id: str = il.InputField(default="", info="Where to find this in the vendor console.")

        prop = FakeInfoSource.definition().config_schema["properties"]["account_id"]
        assert prop["x-info"] == "Where to find this in the vendor console."

    def test_presentation_kwargs_compose_across_factories(self):
        class FakeComposedSource(il.Source):
            region: str = il.SelectField(
                default="eu",
                options=[{"label": "EU", "value": "eu"}],
                label="Region",
                description="API region.",
                info="Pick the region the account was created in.",
                discriminator=True,
            )

        prop = FakeComposedSource.definition().config_schema["properties"]["region"]
        assert prop["title"] == "Region"
        assert prop["description"] == "API region."
        assert prop["x-info"] == "Pick the region the account was created in."
        assert prop["x-discriminator"] is True
        assert prop["x-widget"] == "select"


class TestTimezoneField:
    def test_emits_timezone_widget(self):
        class FakeZonedSource(il.Source):
            timezone: str = il.TimezoneField(default="UTC")

        prop = FakeZonedSource.definition().config_schema["properties"]["timezone"]
        assert prop["x-widget"] == "timezone"
        assert prop["default"] == "UTC"


class TestDiscriminatorMarker:
    def test_marker_serialized_as_x_discriminator(self):
        class FakeMarkedSource(il.Source):
            account_id: str = il.InputField(default="", discriminator=True)

        prop = FakeMarkedSource.definition().config_schema["properties"]["account_id"]
        assert prop["x-discriminator"] is True

    def test_unmarked_field_carries_no_marker(self):
        class FakeUnmarkedSource(il.Source):
            account_id: str = il.InputField(default="")

        prop = FakeUnmarkedSource.definition().config_schema["properties"]["account_id"]
        assert "x-discriminator" not in prop


class TestFetchField:
    def test_emits_provider_only(self):
        class Src(il.Source):
            connection: Conn
            thing_id: str = il.FetchField(provider="connection.things", value_key="id")

        fetch = Src.definition().config_schema["properties"]["thing_id"]["x-fetch"]
        assert fetch["provider"] == "connection.things"
        # The dependency is implicit in the provider's relation: no depends_on/endpoint.
        assert "depends_on" not in fetch
        assert "endpoint" not in fetch

    def test_rejects_malformed_provider(self):
        with pytest.raises(ValueError, match="<name>.<method>"):
            il.FetchField(provider="things")

    def test_annotation_declared_relation_validates_and_is_exposed(self):
        """A relation declared via a typed annotation (not the decorator's ``relations=``) works.

        The relation lives on ``cls.relations``, not ``resource_types``, so
        validation and the definition's ``relations`` map must both resolve
        against it, otherwise the frontend gets an empty relation and the
        FetchField degrades to a plain input.
        """

        class Src(il.Source):
            connection: Conn
            thing_id: str = il.FetchField(provider="connection.things", value_key="id")

        defn = Src.definition()
        assert defn.config_schema["properties"]["thing_id"]["x-fetch"]["provider"] == "connection.things"
        # The annotation-declared relation must be exposed on the definition.
        assert defn.relations["connection"].target is Conn


class TestValidation:
    def test_unknown_relation_rejected(self):
        class Src(il.Source):
            connection: Conn
            thing_id: str = il.FetchField(provider="other.things")

        with pytest.raises(TypeError, match="not declared"):
            Src.definition()

    def test_relation_declared_without_a_class_rejected(self):
        class Src(il.Source):
            connection: Conn
            store = il.Relation("connection", "conn")
            thing_id: str = il.FetchField(provider="store.things")

        with pytest.raises(TypeError, match="not declared from a component class"):
            Src.definition()

    def test_non_provider_method_rejected(self):
        class Src(il.Source):
            connection: Conn
            thing_id: str = il.FetchField(provider="connection.not_a_provider")

        with pytest.raises(TypeError, match="not a @fetch_field_provider"):
            Src.definition()
