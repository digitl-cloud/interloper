"""Tests for ``interloper.utils.stats``."""

from interloper.utils import percentile


class TestPercentile:
    def test_nearest_rank(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]

        assert percentile(values, 50) == 50.0
        assert percentile(values, 90) == 90.0
        assert percentile(values, 100) == 100.0
        assert percentile([7.0], 90) == 7.0
        assert percentile([], 50) is None
