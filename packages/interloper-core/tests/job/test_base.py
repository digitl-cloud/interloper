"""Tests for ``interloper.job.base``."""

from typing import Any, ClassVar

import pytest

import interloper as il
from interloper.errors import ConfigError, DAGError


class FakeAsset(il.Asset):
    """Plain asset fixture."""

    def data(self) -> Any:  # pragma: no cover
        return None


class FakeStandaloneAsset(il.Asset):
    """Standalone asset fixture used as a direct job target."""

    def data(self) -> Any:  # pragma: no cover
        return None


class FakeSource(il.Source):
    """Source fixture owning a single asset."""

    asset_types: ClassVar[list[type[il.Asset]]] = [FakeAsset]


class FakeJobDestination(il.MemoryDestination):
    """Destination fixture for cascade tests."""


class FakeOtherJobDestination(FakeJobDestination):
    """Second destination class, to tell a cascaded binding from an own one."""


class TestRetry:
    def test_a_job_carries_a_retry_policy(self):
        policy = il.RetryPolicy(max_attempts=2)
        assert il.Job(retry=policy).retry == policy

    def test_a_job_declares_no_policy_by_default(self):
        assert il.Job().retry is None


class TestDefinition:
    """Class-level identity and defaults."""

    def test_kind_and_key(self):
        assert il.Job.kind == "job"
        assert il.Job.key == "job"

    def test_definition_self_describes(self):
        defn = il.CronJob.definition()
        assert set(defn.config_schema["properties"]) == {
            "cron",
            "timezone",
            "enabled",
            "retry",
            "timeout",
            "tags",
            "lookback",
            "offset",
            "concurrency",
        }
        assert "cron" in defn.config_schema.get("required", [])
        assert defn.relations["targets"].kinds == ["source", "asset"]
        assert defn.relations["targets"].many is True

    def test_a_concrete_job_leads_with_its_own_fields(self):
        properties = il.CronJob.config_schema()["properties"]
        assert list(properties) == [
            "cron",
            "timezone",
            "lookback",
            "offset",
            "concurrency",
            "tags",
            "enabled",
            "retry",
            "timeout",
        ]
        assert properties["lookback"]["x-section"] == "Partitioning"
        assert {name for name, prop in properties.items() if prop.get("x-section") == "Operation"} == {
            "cron",
            "timezone",
            "concurrency",
            "enabled",
            "retry",
            "timeout",
        }

    def test_anchor_carries_the_workload_only(self):
        defn = il.Job.definition()
        assert set(defn.config_schema["properties"]) == {"enabled", "retry", "timeout", "tags"}
        assert il.CronJob.kind == "job"

    def test_defaults(self):
        job = il.CronJob(cron="0 6 * * *")
        assert job.targets == []
        assert job.destinations == []
        assert job.enabled is True
        assert job.tags == []
        assert job.lookback == 1
        assert job.offset == 1


class TestRelations:
    """The job anchor declares what it materializes and where it writes."""

    def test_anchor_declares_targets(self):
        relation = il.Job.relations["targets"]
        assert (relation.kinds, relation.many, relation.optional, relation.on_delete) == (
            ["source", "asset"],
            True,
            True,
            "detach",
        )

    def test_anchor_declares_destinations(self):
        relation = il.Job.relations["destinations"]
        assert (relation.kind, relation.many, relation.optional, relation.on_delete) == (
            "destination",
            True,
            True,
            "block",
        )

    def test_targets_bind_from_the_constructor(self):
        source, asset = FakeSource(), FakeStandaloneAsset()
        job = il.Job(targets=[source, asset])
        assert job.targets == [source, asset]

    def test_a_hook_is_not_an_acceptable_target(self):
        with pytest.raises(ConfigError, match="does not accept"):
            il.Job(targets=[il.WebhookHook(url="https://x.test")])  # ty: ignore[invalid-argument-type]

    def test_relation_fields_are_not_config_fields(self):
        properties = il.Job.config_schema()["properties"]
        assert "targets" not in properties
        assert "destinations" not in properties


class TestDag:
    """Compiling targets into an executable DAG."""

    def test_dag_over_source_and_asset_targets(self):
        job = il.Job(targets=[FakeSource(), FakeStandaloneAsset()])
        dag = il.DAG(*job.targets)
        assert sorted(type(a).key for a in dag.operations) == ["fake_asset", "fake_standalone_asset"]

    def test_dag_requires_targets(self):
        with pytest.raises(DAGError):
            il.DAG(*il.Job().targets)


class TestSpec:
    """Spec round-trip, including nested targets."""

    def test_round_trip_preserves_the_job_config(self):
        job = il.CronJob(cron="0 6 * * *", enabled=False, tags=["daily"], lookback=7, offset=3)
        clone = il.CronJob.from_spec(job.to_spec())

        assert clone.id == job.id
        assert clone.cron == "0 6 * * *"
        assert clone.enabled is False
        assert clone.tags == ["daily"]
        assert clone.lookback == 7
        assert clone.offset == 3

    def test_round_trip_preserves_targets(self):
        job = il.CronJob(cron="0 6 * * *", targets=[FakeSource(), FakeStandaloneAsset()])
        clone = il.CronJob.from_spec(job.to_spec())

        assert isinstance(clone.targets[0], FakeSource)
        assert isinstance(clone.targets[1], FakeStandaloneAsset)
        assert [type(a).key for a in clone.targets[0].assets] == ["fake_asset"]


class TestWorkloadDefaults:
    """A job's destinations are the default for its targets, where the job runs."""

    @staticmethod
    def _destinations(job: il.Job) -> dict[str, list[il.Destination]]:
        assets = [operation for operation in job.operations() if isinstance(operation, il.Asset)]
        return {asset.key: list(asset.destinations) for asset in assets}

    def test_a_target_without_destinations_runs_with_the_jobs(self):
        destination = FakeJobDestination()
        target = FakeSource()

        job = il.Job(targets=[target], destinations=[destination])

        assert all(found == [destination] for found in self._destinations(job).values())
        assert target.destinations == []

    def test_a_targets_own_destinations_win(self):
        own = FakeJobDestination()
        job = il.Job(targets=[FakeSource(destinations=[own])], destinations=[FakeOtherJobDestination()])

        assert all(found == [own] for found in self._destinations(job).values())

    def test_an_owned_asset_target_keeps_its_sources_destinations(self):
        own = FakeJobDestination()
        asset = FakeSource(destinations=[own]).assets[0]

        job = il.Job(targets=[asset], destinations=[FakeOtherJobDestination()])

        assert self._destinations(job) == {asset.key: [own]}
        assert asset.bound("destinations") == []

    def test_an_owned_asset_target_without_destinations_runs_with_the_jobs(self):
        destination = FakeJobDestination()
        source = FakeSource()
        asset = source.assets[0]

        [operation] = il.Job(targets=[asset], destinations=[destination]).operations()

        assert isinstance(operation, il.Asset)
        assert list(operation.destinations) == [destination]
        assert operation.parent is source
        assert operation.dataset == asset.dataset
        assert asset.bound("destinations") == []

    def test_a_standalone_asset_target_runs_with_the_jobs(self):
        destination = FakeJobDestination()

        job = il.Job(targets=[FakeStandaloneAsset()], destinations=[destination])

        assert all(found == [destination] for found in self._destinations(job).values())

    def test_destinations_given_after_construction_still_apply(self):
        destination = FakeJobDestination()
        job = il.Job(targets=[FakeSource()])

        job.destinations = [destination]

        assert all(found == [destination] for found in self._destinations(job).values())

    def test_two_jobs_sharing_a_target_each_run_it_with_their_own(self):
        target = FakeSource()
        first, second = FakeJobDestination(), FakeOtherJobDestination()

        one = il.Job(targets=[target], destinations=[first])
        two = il.Job(targets=[target], destinations=[second])

        assert all(found == [first] for found in self._destinations(one).values())
        assert all(found == [second] for found in self._destinations(two).values())
        assert target.destinations == []

    def test_compiling_twice_yields_the_same_ids_and_leaves_the_targets_alone(self):
        target = FakeSource()
        job = il.Job(targets=[target], destinations=[FakeJobDestination()])

        first, second = job.operations(), job.operations()
        first[0].enabled = False

        assert [operation.id for operation in first] == [operation.id for operation in second]
        assert second[0].enabled
        assert all(asset.enabled for asset in target.assets)
