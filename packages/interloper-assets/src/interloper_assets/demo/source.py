import datetime as dt
import logging

import interloper as il
import pandas as pd

logger = logging.getLogger(__name__)


class DemoSchema(il.Schema):
    date: dt.date
    hello: str


partitioning = il.TimePartitionConfig(column="date", allow_window=False)


@il.source(
    tags=["Testing"],
    icon="carbon:data-structured",
)
class DemoSource(il.Source):
    """Demo source. Defines a small DAG (a -> b,c,d -> e) with time partitioning."""

    hello: str = il.InputField(default="world", description="Greeting text")
    random_failure_probability: float = il.InputField(default=0.0, description="Probability of random failure (0-1)")

    def _do(self, context: il.ExecutionContext, name: str) -> pd.DataFrame:
        import random
        import time

        context.logger.info(f"Hello {self.hello} from {name}")

        time.sleep(random.uniform(0.5, 1.5))
        if random.random() < self.random_failure_probability:
            raise RuntimeError("Random failure in demo source")

        return pd.DataFrame([{"date": context.partition_date, "hello": self.hello}])

    @il.asset(schema=DemoSchema, partitioning=partitioning, tags=["Report"])
    def a(self, context: il.ExecutionContext) -> pd.DataFrame:
        """Root asset. Returns a single row with the configured greeting."""
        return self._do(context, "A")

    @il.asset(schema=DemoSchema, partitioning=partitioning, tags=["Report"])
    def b(self, context: il.ExecutionContext, a: str, x: str | None = None) -> pd.DataFrame:
        """Depends on A. Part of the example DAG (a -> b -> e)."""
        return self._do(context, "B")

    @il.asset(schema=DemoSchema, partitioning=partitioning, tags=["Report"])
    def c(self, context: il.ExecutionContext, a: str) -> pd.DataFrame:
        """Depends on A. Part of the example DAG (a -> c -> e)."""
        return self._do(context, "C")

    @il.asset(schema=DemoSchema, partitioning=partitioning, tags=["Report"])
    def d(self, context: il.ExecutionContext, a: str) -> pd.DataFrame:
        """Depends on A. Part of the example DAG (a -> d -> e)."""
        return self._do(context, "D")

    @il.asset(schema=DemoSchema, partitioning=partitioning, tags=["Report"])
    def e(self, context: il.ExecutionContext, b: str, c: str, d: str) -> pd.DataFrame:
        """Depends on B, C, and D. Sink asset of the example DAG."""
        return self._do(context, "E")


@il.asset(
    schema=DemoSchema,
    partitioning=partitioning,
    tags=["Report"],
)
def demo_asset(context: il.ExecutionContext) -> pd.DataFrame:
    """Demo asset. Returns a single row with the configured greeting."""
    return pd.DataFrame([{"date": context.partition_date, "hello": "world"}])
