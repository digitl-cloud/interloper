"""Tests for ``interloper.telemetry.attributes``."""

from __future__ import annotations

from interloper.telemetry import attributes


class TestFromMetadata:
    def test_maps_event_metadata_keys(self):
        attrs = attributes.from_metadata(
            {
                "run_id": "r1",
                "backfill_id": "b1",
                "component_id": "a1",
                "component_key": "orders",
                "qualified_key": "shop.orders",
                "partition_or_window": "2026-07-01",
                "source_id": "s1",
            }
        )
        assert attrs == {
            attributes.RUN_ID: "r1",
            attributes.BACKFILL_ID: "b1",
            attributes.COMPONENT_ID: "a1",
            attributes.COMPONENT_KEY: "orders",
            attributes.COMPONENT_QUALIFIED_KEY: "shop.orders",
            attributes.PARTITION: "2026-07-01",
            attributes.SOURCE_ID: "s1",
        }

    def test_drops_none_and_unknown_keys(self):
        attrs = attributes.from_metadata(
            {"run_id": "r1", "backfill_id": None, "message": "ignored", "traceparent": "00-..."}
        )
        assert attrs == {attributes.RUN_ID: "r1"}

    def test_stringifies_values(self):
        import uuid

        run_id = uuid.uuid4()
        assert attributes.from_metadata({"run_id": run_id}) == {attributes.RUN_ID: str(run_id)}


class TestPlatformIdentityMapping:
    def test_identity_keys_map_to_namespaced_attributes(self):
        attrs = attributes.from_metadata(
            {
                "run_id": "r1",
                "org_id": "o1",
                "target_id": "t1",
                "target_kind": "job",
                "target_key": "nightly",
                "target_name": "Nightly sync",
            }
        )
        assert attrs[attributes.ORG_ID] == "o1"
        assert attrs[attributes.TARGET_ID] == "t1"
        assert attrs[attributes.TARGET_KIND] == "job"
        assert attrs[attributes.TARGET_KEY] == "nightly"
        assert attrs[attributes.TARGET_NAME] == "Nightly sync"
