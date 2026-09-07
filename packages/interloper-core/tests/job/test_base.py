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


class FakeJobDestination(il.Destination):
    """Destination fixture for cascade tests."""

    def write(self, context: il.IOContext, data: object) -> None:  # pragma: no cover
        pass

    def read(self, context: il.IOContext) -> object:  # pragma: no cover
        return None


class FakeOtherJobDestination(FakeJobDestination):
    """Second destination class, to tell a cascaded binding from an own one."""


class TestDefinition:
    """Class-level identity and defaults."""

    def test_kind_and_key(self):
        assert il.Job.kind == "job"
        assert il.Job.key == "job"

    def test_definition_self_describes(self):
        defn = il.CronJob.definition()
        assert set(defn.config_schema["properties"]) == {"cron", "timezone", "enabled", "tags", "lookback", "offset"}
        assert "cron" in defn.config_schema.get("required", [])
        assert defn.relations["targets"].kinds() == ["source", "asset"]
        assert defn.relations["targets"].many is True

    def test_anchor_carries_the_workload_only(self):
        defn = il.Job.definition()
        assert set(defn.config_schema["properties"]) == {"enabled", "tags"}
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
        assert (relation.kinds(), relation.many, relation.optional, relation.on_delete) == (
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
    """Job-level relations cascade to targets and destinations."""

    def test_destinations_cascade_to_targets_without_their_own(self):
        destination = FakeJobDestination()
        job = il.Job(targets=[FakeSource()], destinations=[destination])
        assert job.targets[0].destinations == [destination]

    def test_explicit_target_destinations_win(self):
        own = FakeJobDestination()
        job = il.Job(targets=[FakeSource(destinations=[own])], destinations=[FakeOtherJobDestination()])
        assert job.targets[0].destinations == [own]

    def test_a_destination_bound_after_construction_still_cascades(self):
        destination = FakeJobDestination()
        job = il.Job(targets=[FakeSource()])
        job.bind("destinations", destination)
        assert job.targets[0].destinations == [destination]

    def test_a_destination_assigned_after_construction_still_cascades(self):
        destination = FakeJobDestination()
        job = il.Job(targets=[FakeSource()])
        job.destinations = [destination]
        assert job.targets[0].destinations == [destination]

    def test_targets_assigned_after_construction_receive_the_cascade(self):
        destination = FakeJobDestination()
        target = FakeSource()
        job = il.Job(destinations=[destination])
        job.targets = [target]
        assert target.destinations == [destination]

    def test_destinations_cascade_to_asset_targets(self):
        destination = FakeJobDestination()
        job = il.Job(targets=[FakeStandaloneAsset()], destinations=[destination])
        assert job.targets[0].destinations == [destination]
