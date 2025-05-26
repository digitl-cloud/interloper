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
    def root() -> pd.DataFrame:
        return pd.DataFrame({"val": [1, 2, 3]})

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
    def left_1(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        root: pd.DataFrame = itlp.UpstreamAsset("root"),
    ) -> pd.DataFrame:
        raise ValueError("left_1 failed")
        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
    def left_2(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        left_1: pd.DataFrame = itlp.UpstreamAsset("left_1"),
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
    def right_1(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        root: pd.DataFrame = itlp.UpstreamAsset("root"),
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date", allow_window=True))
    def right_2(
        date: tuple[dt.date, dt.date] = itlp.DateWindow(),
        right_1: pd.DataFrame = itlp.UpstreamAsset("right_1"),
    ) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "val": [123],
                "date": pd.Series([date[0]], dtype="datetime64[ns]"),
            }
        )

    return (root, left_1, left_2, right_1, right_2)


source.io = {
    # "file": itlp.FileIO(base_dir="./data"),
    "sqlite": SQLiteIO(db_path="data/sqlite.db"),
    # "bigquery": BigQueryIO(project="dc-int-connectors-prd", location="eu"),
}


pipeline = itlp.Pipeline(source, fail_fast=True)

pipeline.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))
# pipeline.backfill(
#     partitions=itlp.TimePartitionWindow(
#         start=dt.date(2025, 1, 1),
#         end=dt.date(2025, 1, 3),
#     ).iterate()
# )

trace_provider.shutdown()
