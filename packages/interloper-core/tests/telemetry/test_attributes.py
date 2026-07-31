"""Tests for ``interloper.telemetry.attributes``."""

from __future__ import annotations

from interloper.telemetry import attributes


class TestFromMetadata:
    def test_maps_event_metadata_keys(self):
        attrs = attributes.from_metadata(
            {
                "run_id": "r1",
                "backfill_id": "b1",
                "asset_id": "a1",
                "asset_key": "orders",
                "asset_qualified_key": "shop.orders",
                "partition_or_window": "2026-07-01",
                "source_id": "s1",
            }
        )
        assert attrs == {
            attributes.RUN_ID: "r1",
            attributes.BACKFILL_ID: "b1",
            attributes.ASSET_ID: "a1",
            attributes.ASSET_KEY: "orders",
            attributes.ASSET_QUALIFIED_KEY: "shop.orders",
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
