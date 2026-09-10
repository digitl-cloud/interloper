"""Tests for ``interloper.normalizer.strategy``."""

from interloper.normalizer import MaterializationStrategy


class TestMaterializationStrategy:
    """Two strategies named for what they do."""

    def test_two_members(self):
        assert list(MaterializationStrategy) == [MaterializationStrategy.STRICT, MaterializationStrategy.RECONCILE]

    def test_the_retired_auto_value_reads_as_reconcile(self):
        # Stored configs and manifests written before 0.80 spell the default "auto".
        assert MaterializationStrategy("auto") is MaterializationStrategy.RECONCILE
