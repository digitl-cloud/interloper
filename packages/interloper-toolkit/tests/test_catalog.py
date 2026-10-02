"""Tests for ``interloper_toolkit.catalog``."""

from __future__ import annotations

import json

from interloper_toolkit import ToolkitContext
from interloper_toolkit import catalog as catalog_tools
from interloper_toolkit.models import DefinitionList


class TestCatalog:
    def test_search_fields_matches_across_sources(self, ctx: ToolkitContext):
        result = catalog_tools.search_fields(ctx, "campaign")

        assert result.status == "success"
        assert result.match_count == 2
        assert {m.qualified_key for m in result.matches} == {"facebook_ads.ads", "google_ads.campaigns"}

    def test_search_fields_pages_with_a_total(self, ctx: ToolkitContext):
        result = catalog_tools.search_fields(ctx, "campaign", limit=1, offset=1)

        assert result.status == "success"
        assert (result.match_count, result.total) == (1, 2)
        assert result.matches[0].qualified_key == "google_ads.campaigns"

    def test_compare_schemas_reports_shared_and_unique(self, ctx: ToolkitContext):
        result = catalog_tools.compare_schemas(ctx, "facebook_ads", "ads", "google_ads", "campaigns")

        assert result.status == "success"
        assert result.shared_fields[0].field == "campaign_id"
        assert result.only_in_a == ["spend"]
        assert result.only_in_b == ["clicks"]

    def test_list_definitions_reports_each_maturity(self, ctx: ToolkitContext):
        result = catalog_tools.list_definitions(ctx, "source")
        assert isinstance(result, DefinitionList)
        assert result.definitions
        assert {entry.maturity for entry in result.definitions} <= {"deprecated", "alpha", "beta", "stable"}

    def test_unknown_definition_is_a_structured_error(self, ctx: ToolkitContext):
        result = catalog_tools.get_definition(ctx, "nope")

        assert result.status == "error"

    def test_get_definition_inlines_schema_refs(self, ctx: ToolkitContext):
        result = catalog_tools.get_definition(ctx, "facebook_ads")

        assert result.status == "success"
        strategy = result.definition["config_schema"]["properties"]["materialization_strategy"]
        assert strategy["enum"] == ["strict", "reconcile"]
        assert strategy["default"] == "reconcile"
        serialized = json.dumps(result.definition)
        assert "$ref" not in serialized
        assert "$defs" not in serialized

    def test_inline_refs_survives_cyclic_definitions(self):
        schema = {
            "$defs": {"Node": {"properties": {"next": {"$ref": "#/$defs/Node"}}, "type": "object"}},
            "properties": {"root": {"$ref": "#/$defs/Node"}},
        }

        result = catalog_tools._inline_refs(schema)

        assert result["properties"]["root"]["type"] == "object"
        assert "$ref" not in json.dumps(result)
