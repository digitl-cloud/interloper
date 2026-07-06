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

    def test_defaults(self):
        job = il.Job()
        assert job.targets == []
        assert job.cron is None
        assert job.enabled is True
        assert job.tags == []
        assert job.partitioned is False
        assert job.backfill_days is None


class TestDag:
    """Compiling targets into an executable DAG."""

    def test_dag_over_source_and_asset_targets(self):
        job = il.Job(targets=[FakeSource(), FakeStandaloneAsset()])
        dag = job.dag()
        assert sorted(type(a).key for a in dag.assets) == ["fake_asset", "fake_standalone_asset"]

    def test_dag_requires_targets(self):
        with pytest.raises(DAGError):
            il.Job().dag()


class TestSpec:
    """ComponentSpec round-trip, including nested targets."""

    def test_round_trip(self):
        job = il.Job(
            cron="0 6 * * *",
            enabled=False,
            tags=["daily"],
            partitioned=True,
            backfill_days=7,
            targets=[FakeSource(), FakeStandaloneAsset()],
        )
        clone = il.Job.from_spec(job.to_spec())

        assert clone.id == job.id
        assert clone.cron == "0 6 * * *"
        assert clone.enabled is False
        assert clone.tags == ["daily"]
        assert clone.partitioned is True
        assert clone.backfill_days == 7
        assert isinstance(clone.targets[0], FakeSource)
        assert isinstance(clone.targets[1], FakeStandaloneAsset)
        assert [type(a).key for a in clone.targets[0].assets] == ["fake_asset"]
