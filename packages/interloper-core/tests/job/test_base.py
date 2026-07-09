"""Tests for ``interloper.job.base``."""

from typing import ClassVar

import pytest

import interloper as il
from interloper.errors import DAGError


class FakeAsset(il.Asset):
    """Plain asset fixture."""


class FakeStandaloneAsset(il.Asset):
    """Standalone asset fixture used as a direct job target."""


class FakeSource(il.Source):
    """Source fixture owning a single asset."""

    asset_types: ClassVar[list[type[il.Asset]]] = [FakeAsset]


class TestDefinition:
    """Class-level identity and defaults."""

    def test_kind_and_key(self):
        assert il.Job.kind == "job"
        assert il.Job.key == "job"

    def test_definition_self_describes(self):
        defn = il.CronJob.definition()
        assert set(defn.config_schema["properties"]) == {"cron", "enabled", "tags", "partitioned", "backfill_days"}
        assert "cron" in defn.config_schema.get("required", [])
        assert defn.relations["target"].kinds == ["source", "asset"]
        assert defn.relations["target"].slotted is False

    def test_anchor_carries_the_workload_only(self):
        defn = il.Job.definition()
        assert set(defn.config_schema["properties"]) == {"enabled", "tags"}
        assert il.CronJob.kind == "job"

    def test_defaults(self):
        job = il.CronJob(cron="0 6 * * *")
        assert job.targets == []
        assert job.enabled is True
        assert job.tags == []
        assert job.partitioned is False
        assert job.backfill_days == 1


class TestDag:
    """Compiling targets into an executable DAG."""

    def test_dag_over_source_and_asset_targets(self):
        job = il.Job(targets=[FakeSource(), FakeStandaloneAsset()])
        dag = il.DAG(*job.targets)
        assert sorted(type(a).key for a in dag.assets) == ["fake_asset", "fake_standalone_asset"]

    def test_dag_requires_targets(self):
        with pytest.raises(DAGError):
            il.DAG(*il.Job().targets)


class TestSpec:
    """Spec round-trip, including nested targets."""

    def test_round_trip(self):
        job = il.CronJob(
            cron="0 6 * * *",
            enabled=False,
            tags=["daily"],
            partitioned=True,
            backfill_days=7,
            targets=[FakeSource(), FakeStandaloneAsset()],
        )
        clone = il.CronJob.from_spec(job.to_spec())

        assert clone.id == job.id
        assert clone.cron == "0 6 * * *"
        assert clone.enabled is False
        assert clone.tags == ["daily"]
        assert clone.partitioned is True
        assert clone.backfill_days == 7
        assert isinstance(clone.targets[0], FakeSource)
        assert isinstance(clone.targets[1], FakeStandaloneAsset)
        assert [type(a).key for a in clone.targets[0].assets] == ["fake_asset"]


class FakeJobDestination(il.Destination):
    """Destination fixture for cascade tests."""

    def write(self, context: il.IOContext, data: object) -> None:  # pragma: no cover
        pass

    def read(self, context: il.IOContext) -> object:  # pragma: no cover
        return None


class FakeJobResource(il.Resource):
    """Resource fixture for trickle tests."""

    token: str = ""


class FakeResourceAsset(il.Asset):
    """Asset with a resource slot for job trickle tests."""

    resource_types: ClassVar[dict[str, type[il.Resource]]] = {"conn": FakeJobResource}


class TestWorkloadDefaults:
    """Job-level destinations and resources cascade to targets."""

    def test_destinations_cascade_to_targets_without_their_own(self):
        dest = FakeJobDestination()
        job = il.Job(targets=[FakeSource(), FakeStandaloneAsset()], destinations=[dest])
        for target in job.targets:
            assert target.destinations == [dest]

    def test_explicit_target_destinations_win(self):
        own = FakeJobDestination()
        job = il.Job(targets=[FakeStandaloneAsset(destinations=[own])], destinations=[FakeJobDestination()])
        assert job.targets[0].destinations == [own]

    def test_resources_trickle_into_target_slots(self):
        res = FakeJobResource(token="abc")
        job = il.Job(targets=[FakeResourceAsset()], resources={"conn": res})
        assert job.targets[0].resources["conn"] is res

    def test_resources_trickle_into_destination_slots_by_type(self):
        class FakeConnectedDestination(FakeJobDestination):
            resource_types: ClassVar[dict[str, type[il.Resource]]] = {"creds": FakeJobResource}

        res = FakeJobResource(token="abc")
        job = il.Job(targets=[FakeStandaloneAsset()], destinations=[FakeConnectedDestination()], resources={"any": res})
        assert job.destinations[0].resources["creds"] is res

    def test_single_destination_coerced_to_list(self):
        dest = FakeJobDestination()
        job = il.Job.model_validate({"targets": [FakeStandaloneAsset()], "destinations": dest})
        assert job.destinations == [dest]

    def test_vocabulary_declares_workload_defaults(self):
        relations = il.Job.relation_types
        assert relations["destination"].field == "destinations"
        assert relations["resource"].slotted is True
