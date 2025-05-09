import datetime as dt
import logging

import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer
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
def my_source() -> tuple[itlp.Asset, ...]:
    @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
    def my_asset_A(date: dt.date = itlp.Date()) -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2, 3]})

    @itlp.asset(partitioning=itlp.TimePartitionConfig("date"))
    def my_asset_B(date: dt.date = itlp.Date()) -> pd.DataFrame:
        return pd.DataFrame({"b": [4, 5, 6]})

    return (my_asset_A, my_asset_B)


my_source.io = {
    "file": itlp.FileIO(base_dir="./data"),
    "file2": itlp.FileIO(base_dir="./data2"),
}


itlp.Pipeline(my_source).backfill(
    partitions=itlp.TimePartitionWindow(
        start=dt.date(2025, 1, 1),
        end=dt.date(2025, 1, 3),
    )
)

trace_provider.shutdown()
