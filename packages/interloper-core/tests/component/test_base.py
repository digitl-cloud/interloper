"""Tests for ``interloper.component.base``."""

from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Any, ClassVar

import pytest
from pydantic import Field, ValidationError

import interloper as il
from interloper.component.base import (
    Component,
    ComponentDefinition,
    _adopt_kind,
)
from interloper.errors import ConfigError
from interloper.serializable import Spec

# -- Fixtures ------------------------------------------------------------------


class Mode(str, Enum):
    FAST = "fast"
    SLOW = "slow"


class FakeResource(il.Resource):
    """Resource fixture. Carries a scalar pair and an opaque dict field."""

    text: str = ""
    value: str = ""
    data: dict[str, Any] = Field(default_factory=dict)


class FakeComponent(Component):
    """Primary test component covering every serialization shape the base layer handles."""

    text: str = ""
    mode: Mode = Mode.FAST
    date: dt.date | None = None
    child: Component | None = None
    children: list[Component] | None = None
    resources: dict[str, Any] = Field(default_factory=dict)
    labels: list[str] = Field(default_factory=list)


class FakeOtherComponent(Component):
    """Second component class used to verify subclass identity through round-trips."""

    value: str = ""


class FakeKind(Component):
    """A satellite-style kind, subclassed to exercise anchor resolution."""

    sensitive: ClassVar[bool] = True


class FakeConcreteKind(FakeKind):
    """A concrete class of the fake kind (inherits kind ``fake_kind``)."""


# -- Identity and class metadata -----------------------------------------------


class TestIdentity:
    def test_key_auto_derived_from_class_name(self):
        assert FakeResource.key == "fake_resource"
        assert FakeComponent.key == "fake_component"
        assert FakeOtherComponent.key == "fake_other_component"

    def test_kind_set_on_direct_children_of_component(self):
        assert il.Resource.kind == "resource"
        assert FakeComponent.kind == "fake_component"
        assert FakeOtherComponent.kind == "fake_other_component"

    def test_kind_inherited_by_subclasses(self):
        # FakeResource extends Resource, not Component directly, so it
        # inherits its parent's kind rather than auto-deriving a new one.
        assert FakeResource.kind == il.Resource.kind

    def test_instance_id_auto_generated(self):
        resource = FakeResource()
        assert resource.id
        import uuid as _uuid

        assert str(_uuid.UUID(resource.id)) == resource.id  # one identity format: full UUID

    def test_instance_id_explicit_preserved(self):
        resource = FakeResource(id="explicit1")
        assert resource.id == "explicit1"

    def test_path_is_fully_qualified(self):
        resource = FakeResource()
        assert resource.path().endswith(".FakeResource")

    def test_str_format(self):
        resource = FakeResource(id="abcd1234")
        assert str(resource) == "FakeResource (key: fake_resource, id: abcd1234)"

    def test_has_own_field_true_for_non_none_default(self):
        assert FakeResource.has_own_field("text")

    def test_has_own_field_false_for_none_default(self):
        # `child` defaults to None on FakeComponent.
        assert not FakeComponent.has_own_field("child")

    def test_has_own_field_false_for_missing_field(self):
        assert not FakeResource.has_own_field("does_not_exist")


class TestDiscriminator:
    """The config field marked ``discriminator=True`` identifies instances."""

    def test_none_declared_by_default(self):
        assert FakeComponent.discriminator_field() is None
        assert FakeComponent().discriminator is None

    def test_marked_field_discovered_and_value_exposed(self):
        class FakeDiscriminated(Component):
            account_id: str = il.InputField(default="", discriminator=True)

        assert FakeDiscriminated.discriminator_field() == "account_id"
        assert FakeDiscriminated(account_id="42").discriminator == "42"
        # An empty value means "not discriminated" rather than an empty suffix.
        assert FakeDiscriminated().discriminator is None

    def test_multiple_marked_fields_rejected_at_class_definition(self):
        with pytest.raises(TypeError, match="multiple discriminator fields"):

            class FakeDoublyDiscriminated(Component):
                a: str = il.InputField(default="", discriminator=True)
                b: str = il.InputField(default="", discriminator=True)

    def test_instance_name_is_the_discriminator(self):
        class FakeShop(Component):
            shop_id: str = il.InputField(default="", discriminator=True)

        # Falls back to the class label until a discriminator value is set.
        assert FakeShop().instance_name() == "Fake Shop"
        assert FakeShop(shop_id="99").instance_name() == "99"


class TestAnchor:
    def test_subclass_resolves_to_the_kind_declarer(self):
        assert FakeResource.anchor() is il.Resource
        assert FakeConcreteKind.anchor() is FakeKind

    def test_direct_declarer_is_its_own_anchor(self):
        assert il.Resource.anchor() is il.Resource
        assert FakeComponent.anchor() is FakeComponent


class TestKinds:
    """The framework's kinds are registered on package import."""

    def test_builtin_kinds_present(self):
        for kind in ("source", "asset", "destination", "resource", "connection", "config", "job"):
            assert kind in il.KINDS

    def test_workload_kinds(self):
        for kind in ("job", "source", "asset", "connection"):
            assert issubclass(il.KINDS[kind], il.Workload)
        for kind in ("asset", "connection"):
            assert issubclass(il.KINDS[kind], il.Operation)
        for kind in ("job", "source"):
            assert not issubclass(il.KINDS[kind], il.Operation)
        assert not issubclass(il.KINDS["destination"], il.Workload)

    def test_sensitive_follows_the_resource_subtree(self):
        assert il.KINDS["connection"].sensitive is True
        assert il.KINDS["config"].sensitive is True
        assert il.KINDS["source"].sensitive is False
        assert il.KINDS["job"].sensitive is False

    def test_unknown_kind_fails_loudly(self):
        assert il.KINDS.get("nope") is None
        with pytest.raises(KeyError, match="'nope' is not registered"):
            il.KINDS["nope"]

    def test_entry_point_adoption_anchors_the_kind(self):
        assert _adopt_kind("fake_concrete_kind", FakeConcreteKind) == ("fake_kind", FakeKind)

    def test_non_component_entry_is_rejected(self):
        with pytest.raises(TypeError, match="not a Component class"):
            _adopt_kind("bogus", object)


# -- Definition metadata -------------------------------------------------------


class TestDefinition:
    def test_definition_returns_component_definition(self):
        assert isinstance(FakeComponent.definition(), ComponentDefinition)

    def test_definition_fields_populated(self):
        defn = FakeComponent.definition()
        assert defn.kind == "fake_component"
        assert defn.key == "fake_component"
        assert defn.path.endswith(".FakeComponent")
        assert defn.name  # derived from class name

    def test_definition_description_from_docstring(self):
        class FakeDocumentedComponent(Component):
            """A documented component."""

        assert FakeDocumentedComponent.definition().description == "A documented component."

    def test_definition_config_schema_strips_internal_fields(self):
        class FakeSerializable(Component):
            """Component with one user field and one class-declared internal field."""

            internal_fields = frozenset({"plumbing"})

            value: str = ""
            plumbing: str = ""

        schema = FakeSerializable.definition().config_schema
        assert set(schema["properties"]) == {"value"}

    def test_relations_exported(self):
        relations = Widget.definition().relations
        assert relations["connection"].model_dump(mode="json") == {
            "kind": "connection",
            "key": "conn",
            "many": False,
            "optional": False,
            "on_delete": "block",
            "name": "connection",
        }
        assert relations["destinations"].many is True


# -- Relation declaration and binding ------------------------------------------


class Conn(il.Connection):
    """Connection for binding tests; the required secret is read from the environment, not from a binding."""

    api_secret: str = il.SecretField()


class Cfg(il.Config):
    """Config for binding tests; every field is defaulted, so the relation fills itself."""

    threshold: int = il.InputField(default=1)


class Dest(il.Destination):
    """Destination declaring its connection by annotation."""

    connection: Conn

    def read(self, context: Any) -> Any:  # pragma: no cover
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover
        pass


class NeedyDest(il.Destination):
    """Destination with a required field, so a relation targeting it can never fill itself."""

    bucket: str

    def read(self, context: Any) -> Any:  # pragma: no cover
        return None

    def write(self, context: Any, data: Any) -> None:  # pragma: no cover
        pass


class Widget(il.Source):
    """Source with an annotated connection, an explicit optional config, and a self-filling config."""

    connection: Conn
    config: Cfg | None = il.Relation(Cfg, optional=True)
    fallback: Cfg = il.Relation(Cfg)


class Gadget(il.Source):
    """Source whose one non-optional relation nothing can fill: no default, no settings target."""

    store: NeedyDest = il.Relation(NeedyDest)


class TestCollect:
    def test_annotation_becomes_relation(self):
        assert Widget.relations["connection"].kind == "connection"
        assert Widget.relations["connection"].key == "conn"
        assert "connection" not in Widget.model_fields

    def test_relation_attribute_keeps_flags(self):
        assert Widget.relations["config"].optional is True
        assert Widget.relations["config"].name == "config"

    def test_anchor_relations_inherited(self):
        assert Widget.relations["destinations"].many is True

    def test_subclass_replaces_same_name(self):
        class Narrow(Widget):
            connection: Conn | None = il.Relation(Conn, optional=True)

        assert Narrow.relations["connection"].optional is True
        assert Widget.relations["connection"].optional is False

    def test_non_component_annotation_stays_a_field(self):
        assert "dataset" in Widget.model_fields
        assert "dataset" not in Widget.relations


class TestBind:
    def test_kwargs_bind(self):
        connection = Conn(api_secret="s")
        widget = Widget(connection=connection)
        assert widget.connection is connection
        assert widget.bound("connection") is connection
        assert widget.bound_ids()["connection"] == [connection.id]

    def test_unbound_single_is_none_and_many_is_empty(self):
        widget = Widget(connection=Conn(api_secret="s"))
        assert widget.config is None
        assert widget.destinations == []

    def test_self_filling_relation_is_not_bound_but_resolves(self):
        widget = Widget(connection=Conn(api_secret="s"))
        assert widget.fallback is None
        assert isinstance(widget.resolve("fallback"), Cfg)
        assert "fallback" not in widget.bound_ids()

    def test_missing_required_is_a_build_error(self):
        with pytest.raises(ConfigError, match="store"):
            Gadget()

    def test_a_settings_relation_is_left_to_the_read(self, monkeypatch):
        # A connection's required fields come from the environment, so an
        # unbound one builds and only the read can fail.
        monkeypatch.delenv("api_secret", raising=False)
        monkeypatch.delenv("API_SECRET", raising=False)
        widget = Widget()  # ty: ignore[missing-argument]

        assert widget.bound("connection") is None
        with pytest.raises(ValidationError):
            widget.resolve("connection")

    def test_wrong_kind_rejected(self):
        with pytest.raises(ConfigError, match="connection"):
            Widget(connection=Cfg())  # ty: ignore[invalid-argument-type]

    def test_single_relation_rejects_second_target(self):
        widget = Widget(connection=Conn(api_secret="s"))
        with pytest.raises(ConfigError, match="single"):
            widget.bind("connection", Conn(api_secret="other"))

    def test_many_accumulates(self):
        widget = Widget(connection=Conn(api_secret="s"))
        first, second = Dest(connection=Conn(api_secret="a")), Dest(connection=Conn(api_secret="b"))
        widget.bind("destinations", first)
        widget.bind("destinations", second)
        assert widget.destinations == [first, second]

    def test_many_ignores_a_target_it_already_holds(self):
        widget = Widget(connection=Conn(api_secret="s"))
        destination = Dest(connection=Conn(api_secret="a"))
        widget.bind("destinations", destination, destination)
        assert widget.destinations == [destination]

    def test_unbind_detaches_optional(self):
        config = Cfg()
        widget = Widget(connection=Conn(api_secret="s"), config=config)
        widget.unbind("config", config)
        assert widget.config is None

    def test_unbind_refuses_to_empty_required(self):
        connection = Conn(api_secret="s")
        widget = Widget(connection=connection)
        with pytest.raises(ConfigError, match="non-optional"):
            widget.unbind("connection", connection)

    def test_unknown_relation_name_raises(self):
        widget = Widget(connection=Conn(api_secret="s"))
        with pytest.raises(KeyError, match="declares no relation 'nope'"):
            widget.bound("nope")

    def test_unknown_kwarg_is_a_type_error(self):
        with pytest.raises(TypeError, match="unexpected"):
            Widget(connection=Conn(api_secret="s"), nope=1)  # ty: ignore[unknown-argument]


class TestSetAttrRelations:
    """Attribute assignment on a relation name rebinds it.

    The replacement is validated before the existing binding is cleared, so a
    rejected reassignment leaves the previous binding intact.
    """

    def test_assigning_a_list_binds_every_element(self):
        widget = Widget(connection=Conn(api_secret="s"))
        dest = Dest(connection=Conn(api_secret="a"))
        widget.destinations = [dest]
        assert widget.destinations == [dest]

    def test_assigning_none_clears_an_optional_relation(self):
        widget = Widget(connection=Conn(api_secret="s"), config=Cfg())
        widget.config = None
        assert widget.config is None

    def test_assigning_wrong_kind_raises(self):
        widget = Widget(connection=Conn(api_secret="s"))
        with pytest.raises(ConfigError, match="connection"):
            widget.connection = Cfg()  # ty: ignore[invalid-assignment]

    def test_failed_reassignment_leaves_original_binding_intact(self):
        connection = Conn(api_secret="s")
        widget = Widget(connection=connection)
        with pytest.raises(ConfigError, match="connection"):
            widget.connection = Cfg()  # ty: ignore[invalid-assignment]
        assert widget.connection is connection


# -- Relation trickle and validation --------------------------------------------


class Child(il.Asset):
    """Asset declaring a connection relation."""

    connection: Conn | None = il.Relation(Conn, optional=True)

    def data(self, context: il.ExecutionContext) -> list[dict]:
        return []


class TestTrickle:
    def test_fills_unbound_same_name(self) -> None:
        conn = Conn(api_secret="s")
        parent = Widget(connection=conn)
        child = Child()
        parent.trickle(child)
        assert child.connection is conn

    def test_never_overrides_explicit_binding(self) -> None:
        own = Conn(api_secret="s")
        parent = Widget(connection=Conn(api_secret="s"))
        child = Child(connection=own)
        parent.trickle(child)
        assert child.connection is own

    def test_skips_targets_the_child_rejects(self) -> None:
        class Other(il.Connection):
            """Another connection kind."""

        class Picky(il.Asset):
            connection: Other | None = il.Relation(Other, optional=True)

            def data(self, context: il.ExecutionContext) -> list[dict]:
                return []

        parent = Widget(connection=Conn(api_secret="s"))
        child = Picky()
        parent.trickle(child)
        assert child.connection is None


class TestValidateRelations:
    def test_identity_mismatch_reported(self) -> None:
        widget = Widget(connection=Conn(api_secret="s"))
        widget._bound["connection"] = [Cfg()]  # bypass bind to simulate a stale binding
        with pytest.raises(ConfigError, match="connection"):
            widget.validate_relations()

    def test_asset_target_must_be_a_node_when_nodes_given(self) -> None:
        class Up(il.Asset):
            def data(self, context: il.ExecutionContext) -> list[dict]:
                return []

        class Down(il.Asset):
            up: il.Asset = il.Relation("asset", "up")

            def data(self, context: il.ExecutionContext, up: il.Upstream) -> list[dict]:
                return []

        upstream = Up()
        down = Down(up=upstream)
        down.validate_relations({upstream.id: upstream})
        with pytest.raises(ConfigError, match="not in the DAG"):
            down.validate_relations({})


# -- Serialization: to_spec, from_spec, reconstruct, round-trip, discriminator ----


class TestSerialization:
    # -- to_spec: shape and content ----------------------------------------

    def test_to_spec_returns_component_spec(self):
        spec = FakeResource(text="abc").to_spec()
        assert isinstance(spec, Spec)

    def test_to_spec_captures_path_and_id(self):
        resource = FakeResource(id="fixed123", text="abc")
        spec = resource.to_spec()
        assert spec.path == resource.path()
        assert spec.id == "fixed123"

    def test_to_spec_omits_id_from_init(self):
        resource = FakeResource(id="fixed123", text="abc")
        init = resource.to_spec().init or {}
        assert "id" not in init

    def test_to_spec_omits_none_valued_fields(self):
        component = FakeComponent(child=None)
        init = component.to_spec().init or {}
        assert "child" not in init

    def test_to_spec_captures_scalar_fields(self):
        resource = FakeResource(text="abc", value="xyz")
        init = resource.to_spec().init or {}
        assert init.get("text") == "abc"
        assert init.get("value") == "xyz"

    def test_to_spec_serializes_enum_as_value(self):
        component = FakeComponent(text="abc", mode=Mode.SLOW)
        init = component.to_spec().init or {}
        assert init["mode"] == "slow"

    def test_to_spec_serializes_date_as_iso_string(self):
        component = FakeComponent(text="abc", date=dt.date(2026, 4, 9))
        init = component.to_spec().init or {}
        assert init["date"] == "2026-04-09"

    # -- Round-trip: every nesting shape ------------------------------------

    def test_roundtrip_plain_component(self):
        resource = FakeResource(text="abc", value="xyz")
        restored = Component.from_spec(resource.to_spec())
        assert isinstance(restored, FakeResource)
        assert restored.text == "abc"
        assert restored.value == "xyz"

    def test_roundtrip_preserves_instance_id(self):
        resource = FakeResource(id="fixedid1", text="abc")
        restored = Component.from_spec(resource.to_spec())
        assert restored.id == "fixedid1"

    def test_roundtrip_generates_new_id_when_absent(self):
        spec = Spec(path=FakeResource().path(), id="", init={"text": "abc"})
        restored = Component.from_spec(spec)
        assert restored.id  # generated by model_post_init
        import uuid as _uuid

        assert str(_uuid.UUID(restored.id)) == restored.id

    def test_roundtrip_single_nested_component(self):
        component = FakeComponent(child=FakeOtherComponent(value="v1"))
        restored = FakeComponent.from_spec(component.to_spec())
        assert isinstance(restored.child, FakeOtherComponent)
        assert restored.child.value == "v1"

    def test_roundtrip_list_of_components(self):
        component = FakeComponent(
            children=[
                FakeOtherComponent(value="v1"),
                FakeResource(text="r1"),
            ]
        )
        restored = FakeComponent.from_spec(component.to_spec())
        assert isinstance(restored.children, list)
        assert len(restored.children) == 2
        assert isinstance(restored.children[0], FakeOtherComponent)
        assert isinstance(restored.children[1], FakeResource)
        assert restored.children[0].value == "v1"
        assert restored.children[1].text == "r1"

    def test_roundtrip_dict_of_components(self):
        component = FakeComponent(
            resources={
                "primary": FakeResource(text="a"),
                "secondary": FakeResource(text="b"),
            }
        )
        restored = FakeComponent.from_spec(component.to_spec())
        assert set(restored.resources) == {"primary", "secondary"}
        assert isinstance(restored.resources["primary"], FakeResource)
        assert restored.resources["primary"].text == "a"
        assert restored.resources["secondary"].text == "b"

    def test_roundtrip_mixed_nested_shapes(self):
        component = FakeComponent(
            child=FakeOtherComponent(value="v0"),
            children=[FakeOtherComponent(value="v1"), FakeResource(text="r1")],
            resources={"r": FakeResource(text="abc", value="xyz")},
            labels=["a", "b"],
        )
        restored = FakeComponent.from_spec(component.to_spec())

        assert isinstance(restored.child, FakeOtherComponent)
        assert restored.child.value == "v0"

        assert isinstance(restored.children, list)
        assert [type(ch).__name__ for ch in restored.children] == [
            "FakeOtherComponent",
            "FakeResource",
        ]

        assert isinstance(restored.resources["r"], FakeResource)
        assert restored.resources["r"].text == "abc"
        assert restored.labels == ["a", "b"]

    def test_roundtrip_via_json_string(self):
        """Spec must survive a full JSON string round-trip."""
        component = FakeComponent(
            mode=Mode.SLOW,
            child=FakeOtherComponent(value="v1"),
            resources={"r": FakeResource(text="abc")},
        )
        spec_json = component.to_spec().model_dump_json()
        reloaded_spec = Spec.model_validate_json(spec_json)
        restored = reloaded_spec.reconstruct()

        assert isinstance(restored, FakeComponent)
        assert restored.mode == Mode.SLOW
        assert isinstance(restored.child, FakeOtherComponent)
        assert restored.child.value == "v1"
        r = restored.resources["r"]
        assert isinstance(r, FakeResource)
        assert r.text == "abc"

    # -- from_spec entry point ---------------------------------------------

    def test_from_spec_accepts_component_spec(self):
        spec = FakeResource(text="abc").to_spec()
        restored = Component.from_spec(spec)
        assert isinstance(restored, FakeResource)

    def test_from_spec_accepts_plain_dict(self):
        spec_dict = FakeResource(text="abc").to_spec().model_dump(mode="json")
        restored = Component.from_spec(spec_dict)
        assert isinstance(restored, FakeResource)
        assert restored.text == "abc"

    def test_from_spec_on_subclass_reconstructs_via_path(self):
        spec = FakeResource(text="abc").to_spec()
        restored = FakeResource.from_spec(spec)
        assert isinstance(restored, FakeResource)
        assert restored.text == "abc"

    def test_from_spec_on_subclass_walks_nested_specs(self):
        component = FakeComponent(
            children=[FakeOtherComponent(value="v1"), FakeResource(text="r1")],
        )
        restored = FakeComponent.from_spec(component.to_spec())
        assert isinstance(restored.children, list)
        assert isinstance(restored.children[0], FakeOtherComponent)
        assert isinstance(restored.children[1], FakeResource)

    # -- Spec discriminator edge cases -------------------------------------

    def test_user_dict_with_path_key_not_mistaken_for_spec(self):
        """A user dict containing 'path' but with extra keys stays a plain dict."""
        r = FakeResource(data={"path": "foo", "extra": "bar"})
        restored = Component.from_spec(r.to_spec())
        assert isinstance(restored, FakeResource)
        assert restored.data == {"path": "foo", "extra": "bar"}

    def test_component_shaped_dict_inside_user_dict_is_reconstructed(self):
        """A Spec-shaped dict nested inside a user dict is walked and reconstructed."""
        component = FakeComponent(resources={"r": FakeResource(text="abc")})
        restored = Component.from_spec(component.to_spec())
        assert isinstance(restored, FakeComponent)
        assert isinstance(restored.resources["r"], FakeResource)

    # -- Error cases -------------------------------------------------------

    def test_reconstruct_raises_on_bad_path(self):
        spec = Spec(path="does.not.exist.Thing", id="", init=None)
        with pytest.raises((ImportError, AttributeError)):
            spec.reconstruct()


class TestSpecRule:
    """Relations in the wire format: inline once, then by reference."""

    def test_a_relation_with_nothing_bound_is_omitted(self) -> None:
        widget = Widget(connection=Conn(api_secret="s"))
        init = widget.to_spec().init or {}
        assert "config" not in init
        assert "fallback" not in init
        assert "destinations" not in init

    def test_a_single_valued_relation_emits_one_value_and_a_many_one_a_list(self) -> None:
        destination = Dest(connection=Conn(api_secret="s"))
        widget = Widget(connection=Conn(api_secret="s"), destinations=[destination])
        init = widget.to_spec().init or {}
        assert init["connection"]["path"] == Conn.classpath()
        assert init["destinations"] == [destination.to_spec().model_dump(mode="json", exclude_defaults=True)]

    def test_a_parentless_target_is_inline_once_then_referenced(self) -> None:
        destination = Dest(connection=Conn(api_secret="s"))
        first = Widget(connection=Conn(api_secret="s"), destinations=[destination])
        second = Widget(connection=Conn(api_secret="s"), destinations=[destination])
        job = il.CronJob(cron="0 6 * * *", targets=[first, second])

        init = job.to_spec().init or {}
        assert init["targets"][0]["init"]["destinations"][0]["path"] == Dest.classpath()
        assert init["targets"][1]["init"]["destinations"][0] == {"ref": destination.id}

    def test_every_emitted_component_carries_its_id(self) -> None:
        widget = Widget(connection=Conn(api_secret="s"))
        spec = widget.to_spec()
        assert spec.id == widget.id
        assert (spec.init or {})["connection"]["id"] == widget.connection.id

    def test_round_trip_shares_a_referenced_instance(self) -> None:
        destination = Dest(connection=Conn(api_secret="s"))
        first = Widget(connection=Conn(api_secret="s"), destinations=[destination])
        second = Widget(connection=Conn(api_secret="s"), destinations=[destination])
        job = il.CronJob(cron="0 6 * * *", targets=[first, second])

        rebuilt = il.CronJob.from_spec(job.to_spec())
        rebuilt_first, rebuilt_second = rebuilt.targets
        assert rebuilt_first.destinations[0] is rebuilt_second.destinations[0]
        assert rebuilt_first.destinations[0].id == destination.id


class TestCatalogKeySpecs:
    """Specs reference components by import ``path`` or catalog ``key``."""

    def test_key_resolves_through_catalog(self):
        from interloper.catalog import Catalog

        catalog = Catalog(components={"fake_resource": FakeResource.definition()})
        spec = Spec(key="fake_resource", init={"text": "abc"})
        instance = spec.reconstruct(catalog)
        assert isinstance(instance, FakeResource)
        assert instance.text == "abc"

    def test_unknown_catalog_key_raises(self):
        from interloper.catalog import Catalog
        from interloper.errors import CatalogKeyError

        with pytest.raises(CatalogKeyError, match="Unknown catalog key 'nope'"):
            Spec(key="nope").reconstruct(Catalog(components={}))

    def test_from_spec_passes_the_catalog(self):
        from interloper.catalog import Catalog

        catalog = Catalog(components={"fake_resource": FakeResource.definition()})
        instance = FakeResource.from_spec(Spec(key="fake_resource"), catalog)
        assert isinstance(instance, FakeResource)


class TestKeyResolution:
    """``resolve_key`` raises rather than returning None; a stale key fails loudly."""

    def test_resolves_the_class(self):
        from interloper.catalog import Catalog

        catalog = Catalog(components={"fake_resource": FakeResource.definition()})
        assert Component.resolve_key("fake_resource", catalog) is FakeResource

    def test_absent_key_raises(self):
        from interloper.catalog import Catalog
        from interloper.errors import CatalogKeyError

        with pytest.raises(CatalogKeyError, match="Unknown catalog key 'gone'"):
            Component.resolve_key("gone", Catalog(components={}))

    def test_subclass_receiver_rejects_another_kind(self):
        from interloper.catalog import Catalog

        catalog = Catalog(components={"fake_resource": FakeResource.definition()})
        with pytest.raises(TypeError, match="does not resolve to a Source class"):
            il.Source.resolve_key("fake_resource", catalog)

    def test_unimportable_path_raises(self):
        from interloper.catalog import Catalog

        # A key that outlived its class: the catalog entry survives in a
        # persisted row, the module it names no longer has the attribute.
        definition = FakeResource.definition().model_copy(update={"path": "interloper.resource.base.Gone"})
        catalog = Catalog(components={"fake_resource": definition})
        with pytest.raises(AttributeError):
            Component.resolve_key("fake_resource", catalog)


class TestStrictInitKwargs:
    """Unknown init kwargs fail loudly instead of being silently dropped."""

    def test_unknown_kwarg_raises(self):
        with pytest.raises(TypeError, match="unexpected keyword argument.*nope"):
            FakeComponent(nope=1)  # type: ignore[call-arg]  # ty: ignore[unknown-argument]


class TestPublicApi:
    """The relation model's names are the ones the package exports."""

    def test_public_relation_names(self) -> None:
        from interloper.asset.upstream import Upstream
        from interloper.component.relation import ComponentIdentity, Relation

        assert il.Relation is Relation
        assert il.ComponentIdentity is ComponentIdentity
        assert il.Upstream is Upstream

    def test_retired_names_are_gone(self) -> None:
        for retired in ("Dependency", "RelationDefinition", "ResourceRef", "AssetIdentity"):
            assert not hasattr(il, retired)
