"""Tests for the quota definitions and their registry (``interloper_db.store.quotas.definitions``)."""

from __future__ import annotations

from uuid import UUID

import pytest

from interloper_db.store import Store
from interloper_db.store.quotas import (
    METRIC_SUCCESSFUL_RUNS,
    QUOTA_MAX_ASSETS_PER_SOURCE,
    QUOTA_MAX_BACKFILL_PARTITIONS,
    QUOTAS,
    BoundQuota,
    CapacityQuota,
    ConsumptionQuota,
    QuotaDefinition,
)


class TestRegistry:
    def test_settings_fields_match_registered_quotas(self):
        """QuotaSettings carries exactly the per-org quota defaults."""
        from interloper.settings import QuotaSettings

        assert set(QuotaSettings.model_fields) == set(QUOTAS.keys())

    def test_capacity_quotas_carry_counters_and_consumption_metrics(self):
        sources = QUOTAS["max_sources"]
        assert isinstance(sources, CapacityQuota) and sources.count is not None
        runs = QUOTAS["max_successful_runs_per_month"]
        assert isinstance(runs, ConsumptionQuota) and runs.metric == METRIC_SUCCESSFUL_RUNS

    def test_unregistered_key_fails_loudly(self, store: Store, org_id: UUID):
        with pytest.raises(KeyError, match="not registered"):
            store.quotas.effective_limit(org_id, "max_bananas")

    def test_definition_is_abstract(self):
        """The base class cannot be instantiated — subclasses must implement check()."""
        with pytest.raises(TypeError, match="abstract"):
            QuotaDefinition(key="max_bananas", label="Max bananas", message=lambda used, limit, subject: "")

    def test_definitions_require_key_and_label(self):
        with pytest.raises(ValueError, match="key and a label"):
            BoundQuota(key="max_bananas", label="", message=lambda used, limit, subject: "")

    def test_consumption_quota_requires_a_metric(self):
        with pytest.raises(ValueError, match="ledger metric"):
            ConsumptionQuota(key="max_bananas", label="Max bananas", message=lambda used, limit, subject: "")

    def test_bound_quota_rejects_a_declarative_check_without_used(self, org_id: UUID):
        definition = QUOTAS[QUOTA_MAX_BACKFILL_PARTITIONS]
        with pytest.raises(ValueError, match="pass used="):
            definition.check(None, org_id, 5)  # ty: ignore[invalid-argument-type]

    def test_counterless_capacity_quota_rejects_a_measured_check(self, org_id: UUID):
        definition = QUOTAS[QUOTA_MAX_ASSETS_PER_SOURCE]
        with pytest.raises(ValueError, match="no usage counter"):
            definition.check(None, org_id, 5)  # ty: ignore[invalid-argument-type]
