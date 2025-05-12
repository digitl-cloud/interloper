import datetime as dt
import logging

import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer
from interloper_sql import SQLiteIO
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.threading import ThreadingInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

itlp.basic_logging(logging.INFO)


ThreadingInstrumentor().instrument()
batch_span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
resource = Resource(attributes={"service.name": "interloper"})
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(batch_span_processor)
trace.set_tracer_provider(trace_provider)


@itlp.source(normalizer=DataframeNormalizer())
def source() -> tuple[itlp.Asset, ...]:
    @itlp.asset()
    def a() -> pd.DataFrame:
        return pd.DataFrame({"val": [1, 2, 3]})

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=False))
    def b(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        a: pd.DataFrame = itlp.UpstreamAsset("a"),
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "val": [123],
                "date": [date],
            }
        )

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
    def c(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        b: pd.DataFrame = itlp.UpstreamAsset("b"),
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "val": [123],
                "date": [date],
            }
        )

    @itlp.asset()
    def d(
        c: pd.DataFrame = itlp.UpstreamAsset("c"),
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "val": [123],
                # "date": [date],
            }
        )

    return (a, b, c)


source.io = {
    # "file": itlp.FileIO(base_dir="./data"),
    "sqlite": SQLiteIO(db_path="data/sqlite.db"),
}


pipeline = itlp.Pipeline(source)

# pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))
pipeline.backfill(
    partitions=itlp.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 3),
    )
)

trace_provider.shutdown()
