"""Tests for ``interloper.component.base``."""

from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Any, ClassVar

import pytest
from pydantic import Field

import interloper as il
from interloper.component.base import (
    Component,
    ComponentDefinition,
    RelationDefinition,
    _adopt_kind,
)
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


class FakeAltResource(il.Resource):
    """Second Resource type, used to exercise type-mismatch scenarios."""

    token: str = ""


class FakeComponent(Component):
    """Primary test component covering every serialization shape the base layer handles."""

    text: str = ""
    mode: Mode = Mode.FAST
    date: dt.date | None = None
    child: Component | None = None
    children: list[Component] | None = None
    labels: list[str] = Field(default_factory=list)


class FakeOtherComponent(Component):
    """Second component class used to verify subclass identity through round-trips."""

    value: str = ""


class FakeKind(Component):
    """A satellite-style kind with its own relation vocabulary."""

    sensitive: ClassVar[bool] = True
    relation_types: ClassVar[dict[str, RelationDefinition]] = {
        "link": RelationDefinition(kinds=["asset"], field="links", slotted=True),
    }

    links: dict[str, Any] = Field(default_factory=dict)


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
        c = FakeResource()
        assert c.id
        import uuid as _uuid

        assert str(_uuid.UUID(c.id)) == c.id  # one identity format: full UUID

    def test_instance_id_explicit_preserved(self):
        c = FakeResource(id="explicit1")
        assert c.id == "explicit1"

    def test_path_is_fully_qualified(self):
        c = FakeResource()
        assert c.path().endswith(".FakeResource")

    def test_str_format(self):
        c = FakeResource(id="abcd1234")
        assert str(c) == "FakeResource (key: fake_resource, id: abcd1234)"

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

    def test_misdeclared_relation_field_is_rejected(self):
        class Broken(Component):
            relation_types: ClassVar[dict[str, RelationDefinition]] = {
                "link": RelationDefinition(kinds=["asset"], field="linkss"),
            }

        with pytest.raises(ValueError, match="no such field"):
            Broken.anchor()


class TestVocabularyMerge:
    """Subclass ``relation_types`` declarations extend, never replace."""

    def test_subclass_extends_the_inherited_vocabulary(self):
        assert set(FakeConcreteKind.relation_types) == {"link"}

        class FakeExtendedKind(FakeKind):
            relation_types: ClassVar[dict[str, RelationDefinition]] = {
                "wire": RelationDefinition(kinds=["asset"], field="wires"),
            }

            wires: list[str] = Field(default_factory=list)

        assert set(FakeExtendedKind.relation_types) == {"link", "wire"}
        # Extend-only: the parent's vocabulary is untouched.
        assert set(FakeKind.relation_types) == {"link"}

    def test_redeclared_type_replaces_that_definition(self):
        class FakeNarrowedKind(FakeKind):
            relation_types: ClassVar[dict[str, RelationDefinition]] = {
                "link": RelationDefinition(kinds=["job"], field="links", slotted=True),
            }

        assert FakeNarrowedKind.relation_types["link"].kinds == ["job"]
        assert FakeKind.relation_types["link"].kinds == ["asset"]

    def test_definition_validates_the_extended_vocabulary(self):
        class FakeBrokenExtension(FakeKind):
            relation_types: ClassVar[dict[str, RelationDefinition]] = {
                "wire": RelationDefinition(kinds=["asset"], field="wiress"),
            }

        with pytest.raises(ValueError, match="no such field"):
            FakeBrokenExtension.definition()


class TestKinds:
    """The framework's kinds are registered on package import."""

    def test_builtin_kinds_present(self):
        for kind in ("source", "asset", "destination", "resource", "connection", "config", "job"):
            assert kind in il.KINDS

    def test_runnable_kinds(self):
        for kind, expected in (("job", True), ("source", True), ("asset", True), ("connection", False)):
            assert il.KINDS[kind].runnable is expected

    def test_sensitive_follows_the_resource_subtree(self):
        assert il.KINDS["connection"].sensitive is True
        assert il.KINDS["config"].sensitive is True
        assert il.KINDS["source"].sensitive is False
        assert il.KINDS["job"].sensitive is False

    def test_relation_vocabulary_from_anchors(self):
        assert set(il.KINDS["asset"].relation_types) == {"resource", "destination", "dependency"}
        assert il.KINDS["job"].relation_types["target"].kinds == ["source", "asset"]

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

    def test_definition_relations_from_class_vocabulary(self):

        from interloper.component.base import RelationDefinition

        class FakeRelated(Component):
            """Component declaring a relation vocabulary."""

            relation_types: ClassVar[dict[str, RelationDefinition]] = {
                "wires": RelationDefinition(kinds=["fake_component"], field="wires", slotted=True)
            }

            wires: dict[str, Any] = Field(default_factory=dict)

        relations = FakeRelated.definition().relations
        assert relations["wires"].kinds == ["fake_component"]
        assert relations["wires"].slotted is True


# -- Resource slot inference and trickle-down ----------------------------------


class FakeConsumer(Component):
    """Component with a Resource-typed annotation to exercise ``_infer_resource_refs``.

    The ``resource`` annotation is rewritten to a ``ResourceRef`` descriptor by
    ``Component.__init_subclass__``, so it is not a Pydantic field at runtime
    (hence the ``# type: ignore[call-arg]`` below when constructing instances).
    """

    resource: FakeResource


class TestResources:
    def test_resource_annotation_becomes_resource_ref(self):
        from interloper.resource.ref import ResourceRef

        assert isinstance(FakeConsumer.__dict__["resource"], ResourceRef)
        assert FakeConsumer.resource_types["resource"] is FakeResource

    def test_explicit_resource_types_entry_not_overwritten(self):
        """When ``resource_types`` already has the slot, the annotation loop skips it."""

        class FakeExplicitConsumer(Component):
            resource_types: ClassVar[dict[str, type]] = {"slot": FakeResource}
            slot: FakeResource  # annotation would normally add a ref

        # Slot is still present, and no ResourceRef descriptor was installed for it.
        assert FakeExplicitConsumer.resource_types["slot"] is FakeResource
        assert "slot" not in FakeExplicitConsumer.__dict__

    def test_trickle_fills_slot_by_name(self):
        shared = FakeResource(text="shared")
        parent = FakeConsumer(resources={"resource": shared})  # ty: ignore[missing-argument]
        child = FakeConsumer()  # ty: ignore[missing-argument]
        parent.trickle_resources(child)
        assert child.resources["resource"] is shared

    def test_trickle_falls_back_to_type_match(self):
        shared = FakeResource(text="shared")
        # Parent holds the resource under a different slot name so name-match fails.
        parent = FakeConsumer(resources={"other": shared})  # ty: ignore[missing-argument]
        child = FakeConsumer()  # ty: ignore[missing-argument]
        parent.trickle_resources(child)
        assert child.resources["resource"] is shared

    def test_trickle_does_not_overwrite_existing_slot(self):
        existing = FakeResource(text="existing")
        override = FakeResource(text="override")
        parent = FakeConsumer(resources={"resource": override})  # ty: ignore[missing-argument]
        child = FakeConsumer(resources={"resource": existing})  # ty: ignore[missing-argument]
        parent.trickle_resources(child)
        assert child.resources["resource"] is existing

    def test_trickle_by_name_skipped_on_type_mismatch(self):
        """A same-named resource of the wrong type must not fill the slot."""
        parent = FakeConsumer(resources={"resource": FakeAltResource()})  # ty: ignore[missing-argument]
        child = FakeConsumer()  # ty: ignore[missing-argument]
        parent.trickle_resources(child)
        assert "resource" not in child.resources

    def test_trickle_by_name_mismatch_falls_back_to_type_match(self):
        shared = FakeResource(text="shared")
        parent = FakeConsumer(resources={"resource": FakeAltResource(), "other": shared})  # ty: ignore[missing-argument]
        child = FakeConsumer()  # ty: ignore[missing-argument]
        parent.trickle_resources(child)
        assert child.resources["resource"] is shared

    def test_init_kwarg_routed_into_resources(self):
        """A Resource passed under a ResourceRef slot name lands in ``resources``."""
        res = FakeResource(text="direct")
        c = FakeConsumer(resource=res)
        assert c.resources["resource"] is res
        assert c.resource is res

    def test_init_kwarg_wrong_type_rejected(self):
        with pytest.raises(TypeError, match="resource 'resource' must be an instance of FakeResource"):
            FakeConsumer(resource=FakeAltResource())  # ty: ignore[invalid-argument-type]

    def test_init_kwarg_conflicting_with_resources_entry_rejected(self):
        with pytest.raises(ValueError, match="both as a keyword argument and in 'resources'"):
            FakeConsumer(resource=FakeResource(), resources={"resource": FakeResource()})

    def test_init_kwarg_merges_with_other_resources(self):
        other = FakeAltResource()
        res = FakeResource()
        c = FakeConsumer(resource=res, resources={"extra": other})
        assert c.resources == {"resource": res, "extra": other}

    def test_init_kwarg_for_explicit_model_field_untouched(self):
        """A slot that is also a real pydantic field goes through pydantic, not ``resources``."""

        class FakeExplicitFieldConsumer(Component):
            resource_types: ClassVar[dict[str, type]] = {"slot": FakeResource}
            slot: FakeResource | None = None

        res = FakeResource()
        c = FakeExplicitFieldConsumer(slot=res)
        assert c.slot is res
        assert "slot" not in c.resources

    def test_init_kwarg_roundtrips_via_spec(self):
        c = FakeConsumer(resource=FakeResource(text="abc"))
        restored = Component.from_spec(c.to_spec())
        assert isinstance(restored.resources["resource"], FakeResource)
        assert restored.resources["resource"].text == "abc"


# -- Serialization: to_spec, from_spec, reconstruct, round-trip, discriminator ----


class TestSerialization:
    # -- to_spec: shape and content ----------------------------------------

    def test_to_spec_returns_component_spec(self):
        spec = FakeResource(text="abc").to_spec()
        assert isinstance(spec, Spec)

    def test_to_spec_captures_path_and_id(self):
        c = FakeResource(id="fixed123", text="abc")
        spec = c.to_spec()
        assert spec.path == c.path()
        assert spec.id == "fixed123"

    def test_to_spec_omits_id_from_init(self):
        c = FakeResource(id="fixed123", text="abc")
        init = c.to_spec().init or {}
        assert "id" not in init

    def test_to_spec_omits_none_valued_fields(self):
        c = FakeComponent(child=None)
        init = c.to_spec().init or {}
        assert "child" not in init

    def test_to_spec_captures_scalar_fields(self):
        c = FakeResource(text="abc", value="xyz")
        init = c.to_spec().init or {}
        assert init.get("text") == "abc"
        assert init.get("value") == "xyz"

    def test_to_spec_serializes_enum_as_value(self):
        c = FakeComponent(text="abc", mode=Mode.SLOW)
        init = c.to_spec().init or {}
        assert init["mode"] == "slow"

    def test_to_spec_serializes_date_as_iso_string(self):
        c = FakeComponent(text="abc", date=dt.date(2026, 4, 9))
        init = c.to_spec().init or {}
        assert init["date"] == "2026-04-09"

    # -- Round-trip: every nesting shape ------------------------------------

    def test_roundtrip_plain_component(self):
        c = FakeResource(text="abc", value="xyz")
        restored = Component.from_spec(c.to_spec())
        assert isinstance(restored, FakeResource)
        assert restored.text == "abc"
        assert restored.value == "xyz"

    def test_roundtrip_preserves_instance_id(self):
        c = FakeResource(id="fixedid1", text="abc")
        restored = Component.from_spec(c.to_spec())
        assert restored.id == "fixedid1"

    def test_roundtrip_generates_new_id_when_absent(self):
        spec = Spec(path=FakeResource().path(), id="", init={"text": "abc"})
        restored = Component.from_spec(spec)
        assert restored.id  # generated by model_post_init
        import uuid as _uuid

        assert str(_uuid.UUID(restored.id)) == restored.id

    def test_roundtrip_single_nested_component(self):
        c = FakeComponent(child=FakeOtherComponent(value="v1"))
        restored = FakeComponent.from_spec(c.to_spec())
        assert isinstance(restored.child, FakeOtherComponent)
        assert restored.child.value == "v1"

    def test_roundtrip_list_of_components(self):
        c = FakeComponent(
            children=[
                FakeOtherComponent(value="v1"),
                FakeResource(text="r1"),
            ]
        )
        restored = FakeComponent.from_spec(c.to_spec())
        assert isinstance(restored.children, list)
        assert len(restored.children) == 2
        assert isinstance(restored.children[0], FakeOtherComponent)
        assert isinstance(restored.children[1], FakeResource)
        assert restored.children[0].value == "v1"
        assert restored.children[1].text == "r1"

    def test_roundtrip_dict_of_components(self):
        c = FakeComponent(
            resources={
                "primary": FakeResource(text="a"),
                "secondary": FakeResource(text="b"),
            }
        )
        restored = FakeComponent.from_spec(c.to_spec())
        assert set(restored.resources) == {"primary", "secondary"}
        assert isinstance(restored.resources["primary"], FakeResource)
        assert restored.resources["primary"].text == "a"
        assert restored.resources["secondary"].text == "b"

    def test_roundtrip_mixed_nested_shapes(self):
        c = FakeComponent(
            child=FakeOtherComponent(value="v0"),
            children=[FakeOtherComponent(value="v1"), FakeResource(text="r1")],
            resources={"r": FakeResource(text="abc", value="xyz")},
            labels=["a", "b"],
        )
        restored = FakeComponent.from_spec(c.to_spec())

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
        c = FakeComponent(
            mode=Mode.SLOW,
            child=FakeOtherComponent(value="v1"),
            resources={"r": FakeResource(text="abc")},
        )
        spec_json = c.to_spec().model_dump_json()
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
        c = FakeComponent(
            children=[FakeOtherComponent(value="v1"), FakeResource(text="r1")],
        )
        restored = FakeComponent.from_spec(c.to_spec())
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
        c = FakeComponent(resources={"r": FakeResource(text="abc")})
        restored = Component.from_spec(c.to_spec())
        assert isinstance(restored.resources["r"], FakeResource)

    # -- Error cases -------------------------------------------------------

    def test_reconstruct_raises_on_bad_path(self):
        spec = Spec(path="does.not.exist.Thing", id="", init=None)
        with pytest.raises((ImportError, AttributeError)):
            spec.reconstruct()


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


class TestStrictInitKwargs:
    """Unknown init kwargs fail loudly instead of being silently dropped."""

    def test_unknown_kwarg_raises(self):
        with pytest.raises(TypeError, match="unexpected keyword argument.*nope"):
            FakeComponent(nope=1)  # type: ignore[call-arg]  # ty: ignore[unknown-argument]

    def test_resource_slot_kwargs_still_route(self):
        consumer = FakeConsumer(resource=FakeResource(text="abc"))  # type: ignore[call-arg]
        assert isinstance(consumer.resources["resource"], FakeResource)
